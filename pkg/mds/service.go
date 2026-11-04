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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// serviceName is the "service" log field of every line the indexer writes.
	serviceName = "mds"

	// readHeaderTimeout bounds how long a probe may take to send its request headers.
	readHeaderTimeout = 10 * time.Second

	// shutdownTimeout bounds how long an in-flight probe may delay the stop of the process.
	shutdownTimeout = 10 * time.Second
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

// NewService returns a Service for the given configuration. A nil logger falls back to the
// process logger, so a caller in a test or an operator running the command needs no logging
// setup of its own.
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
	if s.tick == nil {
		s.tick = s.noopTick
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
func (s *Service) Run(ctx context.Context) error {
	if s.cfg == nil {
		return ErrNotConfigured
	}
	if s.cfg.Period <= 0 {
		return fmt.Errorf("%w: %s.period must be positive, got %s", ErrInvalidConfig, ConfigKey, s.cfg.Period)
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
		"backfill":    s.cfg.Backfill,
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
			s.tickOnce(ctx)
		}
	}
}

// tickOnce runs one tick and records its duration, its failure and its health. A cancelled tick
// is not a failure: it is the in-flight work of a graceful stop, so it is neither counted nor
// reported.
func (s *Service) tickOnce(ctx context.Context) {
	start := time.Now()
	err := s.tick(ctx)
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
// does nothing else. It is replaced with WithTickFunc by the wire-up of the indexer.
func (s *Service) noopTick(context.Context) error {
	for _, repository := range repositoryNames(s.cfg) {
		s.health.FinishTick(repository, nil)
	}
	return nil
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
