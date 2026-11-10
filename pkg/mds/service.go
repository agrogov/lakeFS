package mds

// service.go is the process model of design §4.1: `lakefs-sso mds run` ticks every period until
// SIGTERM, and serves /_health, /metrics and /_pprof/ on listen_address (§9).
//
// A tick is a single TickFunc. The indexer itself is not part of this skeleton: at the time this
// file was written the per-tick body is a no-op that reports every configured repository as
// healthy, and the indexing work is installed with WithTickFunc. Everything around it - the
// server, the period, graceful stop, the tick duration and the health a failed tick leaves
// behind - is what the indexer runs on.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds/indexer"
	"github.com/treeverse/lakefs/pkg/mds/reader"
)

const (
	// serviceName is the "service" log field of every line the indexer writes.
	serviceName = "mds"

	// readHeaderTimeout bounds how long a probe may take to send its request headers.
	readHeaderTimeout = 10 * time.Second

	// shutdownTimeout bounds how long an in-flight probe may delay the stop of the process.
	shutdownTimeout = 10 * time.Second
)

// The Metadata Search settings that the guide's key list does not have (X6) are fixed behaviour now:
// the values below are the defaults the removed keys had, so an installation that never wrote them
// keeps indexing exactly as it did.
const (
	// fixedDeleteBranches mirrors the deletion of a matching branch to R-metadata, with its tags
	// kept (§4.8). The removed key was metadata_search.delete_branches, default true.
	fixedDeleteBranches = true

	// fixedCompactionMaxDeleteFiles is the number of position-delete files that triggers a
	// compaction of a branch's table (§4.9). The removed key was
	// metadata_search.compaction.max_delete_files, default 200.
	fixedCompactionMaxDeleteFiles = 200

	// fixedCompactionEveryCommits is the number of indexed commits between two compactions
	// (§4.9). The removed key was metadata_search.compaction.every_commits, default 500.
	fixedCompactionEveryCommits = 500
)

// ErrNotConfigured is returned by Run when the service has no configuration at all. A missing
// section is reported by LoadConfig as ErrInvalidConfig; this error is the programming-error
// guard of a Service built by hand.
var ErrNotConfigured = errors.New("metadata search indexer has no configuration")

// TickFunc is the per-tick body of the indexer (§4.1). It processes every configured repository
// and reports per-repository and per-branch progress through the Service's Health. A non-nil
// error means the tick failed as a whole, before it could attribute the failure to one
// repository: the service then marks every repository failed.
type TickFunc func(ctx context.Context) error

// Service is the Metadata Search indexer process (§4.1): one HTTP server and one period.
type Service struct {
	cfg     *Config
	log     logging.Logger
	metrics Recorder
	health  *Health
	tick    TickFunc

	// lakeFS is the in-process reader of the server's repositories (RA4b), built by `mds run`
	// from the same KV store and block adapter the lakeFS server uses. storage and secretKey are
	// what the Iceberg REST catalog of a write needs from the same server configuration.
	lakeFS    reader.LakeFS
	storage   iceberg.Properties
	secretKey string

	mu   sync.RWMutex
	addr string
}

// ServiceOption customises a Service.
type ServiceOption func(*Service)

// WithTickFunc replaces the per-tick body. Without it the tick is a no-op that reports every
// configured repository as healthy, which is the skeleton of §4.1 before the indexer is wired in.
func WithTickFunc(tick TickFunc) ServiceOption {
	return func(s *Service) {
		if tick != nil {
			s.tick = tick
		}
	}
}

// WithLakeFS installs the in-process reader the indexer works through (RA4b, X13). It is required
// for Run to install the real indexer: `mds run` builds it over the KV store and block adapter of
// the server configuration, so there is no REST client to build from an endpoint.
func WithLakeFS(lakeFS reader.LakeFS) ServiceOption {
	return func(s *Service) {
		if lakeFS != nil {
			s.lakeFS = lakeFS
		}
	}
}

// WithStorage sets the iceberg-go IO properties of a table write: the block adapter's own
// credentials, derived from the standard blockstore.* configuration of the process (X6, RA4b).
func WithStorage(storage iceberg.Properties) ServiceOption {
	return func(s *Service) {
		s.storage = storage
	}
}

// WithEncryptSecretKey sets auth.encrypt.secret_key of the server configuration. RA5 signs the
// Metadata Search service user's login token with it, and it must be the server's own key (G12).
func WithEncryptSecretKey(secretKey string) ServiceOption {
	return func(s *Service) {
		s.secretKey = secretKey
	}
}

// NewService returns a Service for the given configuration. A nil logger falls back to the
// process logger, so a caller in a test or an operator running the command needs no logging
// setup of its own.
//
// The per-tick body starts out as the skeleton no-op of §4.1 and is replaced by Run with the real
// indexer (see Run) unless WithTickFunc supplied one. Keeping NewService side-effect free is what
// lets a Service that is only probed - a health check, a unit test - answer without a lakeFS.
func NewService(cfg *Config, log logging.Logger, opts ...ServiceOption) *Service {
	if log == nil {
		log = logging.ContextUnavailable()
	}
	s := &Service{
		cfg:    cfg,
		log:    log.WithField(logging.ServiceNameFieldKey, serviceName),
		health: NewHealth(repositoryNames(cfg)),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Health returns the health the service reports on /_health (§9), so that a caller that installs
// its own TickFunc can report per-agent state.
func (s *Service) Health() *Health {
	return s.health
}

// Addr returns the address the HTTP server is bound to once Run has started listening, and the
// empty string before that. It is what a test or an operator that asked for port 0 reads.
func (s *Service) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// Run serves /_health, /metrics and /_pprof/ on the configured listen_address and ticks every
// period until ctx is cancelled, which is how SIGTERM stops the process (§4.1). It returns nil
// after a graceful stop and an error when the address cannot be bound or the server fails.
//
// It is also where the indexer is installed (§4.1): unless a TickFunc was supplied, Run builds the
// Metadata Search indexer of pkg/mds/indexer from the configuration and ticks it. The build is
// part of startup, so a configuration the indexer cannot address fails here instead of on the
// first tick.
func (s *Service) Run(ctx context.Context) error {
	if s.cfg == nil {
		return ErrNotConfigured
	}
	if s.cfg.Period <= 0 {
		return fmt.Errorf("%w: %s.period must be positive, got %s", ErrInvalidConfig, ConfigKey, s.cfg.Period)
	}
	if err := s.installIndexer(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", s.cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("mds: listen on %q: %w", s.cfg.ListenAddress, err)
	}
	s.setAddr(listener.Addr().String())

	server := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	s.log.WithFields(logging.Fields{
		"address":     listener.Addr().String(),
		"period":      s.cfg.Period,
		"concurrency": s.cfg.Concurrency,
		"backfill":    indexer.BackfillHistory,
	}).Info("mds: indexer started")

	loopErr := s.tickUntilDone(ctx, serveErr)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)

	s.log.Info("mds: indexer stopped")
	switch {
	case loopErr != nil:
		return loopErr
	case shutdownErr != nil:
		return fmt.Errorf("mds: shut down the HTTP server: %w", shutdownErr)
	}
	return nil
}

// tickUntilDone runs the period until ctx is cancelled or the HTTP server stops serving.
//
// A tick runs in its own goroutine and the loop keeps waiting on the stop signals: a graceful stop
// must not wait for a tick that is blocked on a slow or unreachable lakeFS. The tick observes the
// cancellation of ctx, and the position it had reached is a lakeFS commit, so the next process
// resumes exactly where this one stopped (§4.4).
func (s *Service) tickUntilDone(ctx context.Context, serveErr <-chan error) error {
	ticker := time.NewTicker(s.cfg.Period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-serveErr:
			if err == nil || errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return fmt.Errorf("mds: serve HTTP: %w", err)
		case <-ticker.C:
			tickDone := make(chan struct{})
			go func() {
				defer close(tickDone)
				s.tickOnce(ctx)
			}()
			select {
			case <-tickDone:
			case <-ctx.Done():
				return nil
			case err := <-serveErr:
				if err == nil || errors.Is(err, http.ErrServerClosed) {
					return nil
				}
				return fmt.Errorf("mds: serve HTTP: %w", err)
			}
		}
	}
}

// tickOnce runs one tick and records its duration, its failure and its health. A cancelled tick
// is not a failure: it is the in-flight work of a graceful stop, so it is neither counted nor
// reported.
func (s *Service) tickOnce(ctx context.Context) {
	start := time.Now()
	tick := s.tick
	if tick == nil {
		tick = s.noopTick
	}
	err := tick(ctx)
	s.metrics.TickDuration(time.Since(start))
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		s.metrics.Error(AllRepositories, StageTick)
		s.health.FailAll(err)
		s.log.WithError(err).Warn("mds: tick failed")
		return
	}
	s.log.Debug("mds: tick done")
}

// noopTick is the skeleton's per-tick body: it reports every configured repository healthy and
// does nothing else. It runs when no TickFunc was supplied and the Service was never Run, which is
// the state a unit test or a bare health probe is in.
func (s *Service) noopTick(context.Context) error {
	for _, repository := range repositoryNames(s.cfg) {
		s.health.FinishTick(repository, nil)
	}
	return nil
}

// installIndexer replaces the skeleton tick with the Metadata Search indexer of §4.1, unless a
// TickFunc was supplied. It is called by Run, so the process the command starts always indexes.
func (s *Service) installIndexer() error {
	if s.tick != nil {
		return nil
	}
	idx, err := indexer.New(indexerOptions(s.cfg, s.lakeFS, s.storage, s.secretKey, s.health, s.metrics, s.log))
	if err != nil {
		return fmt.Errorf("%w: cannot start the indexer: %w", ErrInvalidConfig, err)
	}
	s.tick = idx.Tick
	return nil
}

// indexerOptions maps the metadata_search section of §7 onto the indexer's own configuration. The
// indexer does not import this package, so this is the one place where the two meet.
//
// The section has the guide's key list and nothing else (G10, G11, X6). Everything the removed keys
// used to configure is fixed behaviour here: the request bound, the seed lookback, the rebuild
// threshold and the compaction triggers keep their documented defaults, tag retention keeps every
// mds-* tag, branch deletion is mirrored, and the object-store credentials of a write come from the
// standard blockstore configuration of the process rather than from a metadata_search key.
func indexerOptions(cfg *Config, lakeFS reader.LakeFS, storage iceberg.Properties, secretKey string, health *Health, metrics Recorder, log logging.Logger) indexer.Options {
	opts := indexer.Options{
		LakeFS:           lakeFS,
		Endpoint:         cfg.LakeFSEndpoint,
		AccessKeyID:      cfg.AccessKeyID,
		SecretKey:        cfg.SecretKey,
		EncryptSecretKey: secretKey,
		Storage:          storage,
		// X11: the guide has no backfill key. The first sync of a branch captures metadata "from
		// the time the branch was created", or from since when it is set, so the branch is always
		// walked back to its oldest first-parent commit.
		Backfill:         indexer.BackfillHistory,
		RequestTimeout:   indexer.DefaultRequestTimeout,
		SeedLookback:     indexer.DefaultSeedLookback,
		RebuildThreshold: indexer.DefaultRebuildThreshold,
		DeleteBranches:   fixedDeleteBranches,
		TagRetention:     0,
		Compaction: indexer.Compaction{
			MaxDeleteFiles: fixedCompactionMaxDeleteFiles,
			EveryCommits:   fixedCompactionEveryCommits,
		},
		Concurrency: cfg.Concurrency,
		MaxCommits:  cfg.MaxCommits,
		Health:      health,
		Metrics:     metrics,
		Log:         log,
	}
	if since, ok := cfg.SinceTime(); ok {
		opts.Since = &since
	}
	opts.Repositories = make([]indexer.Repository, 0, len(cfg.Repositories))
	for _, name := range cfg.RepositoryNames() {
		repository := cfg.Repositories[name]
		opts.Repositories = append(opts.Repositories, indexer.Repository{
			Name:             name,
			Branches:         append([]string(nil), repository.Branches...),
			StorageNamespace: repository.StorageNamespace,
		})
	}
	return opts
}

// handler builds the HTTP surface of §9: the health report, the Prometheus metrics of the
// process and the Go runtime profiles.
func (s *Service) handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(HealthPath, s.health)
	mux.Handle(MetricsPath, promhttp.Handler())
	mux.Handle(PPROFPath, httputil.ServePPROF(PPROFPath))
	return mux
}

func (s *Service) setAddr(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addr = addr
}

// repositoryNames is Config.RepositoryNames that also accepts a nil configuration, so that
// NewService can be built before Run reports ErrNotConfigured.
func repositoryNames(cfg *Config) []string {
	if cfg == nil {
		return nil
	}
	return cfg.RepositoryNames()
}
