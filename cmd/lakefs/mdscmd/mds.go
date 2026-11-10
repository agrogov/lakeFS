// Package mdscmd registers the Metadata Search indexer command, `mds run`, on the root command of
// the lakeFS binaries.
//
// Both binaries need it: the upstream chart runs the command on the `lakefs` binary
// (`lakefs mds run`), and this repository's deployments run the `lakefs-sso` binary built by the
// Makefile (`LAKEFS_SSO_BINARY_NAME`), where the same command is `lakefs-sso mds run`.
//
// It is its own package, and not a file in cmd/lakefs/cmd, because of an import cycle: the command
// builds the indexer service (pkg/mds -> pkg/mds/indexer), and pkg/mds/indexer imports
// pkg/icebergcatalog, which imports cmd/lakefs/cmd for the server dependency struct. The command
// therefore has to be a package that imports cmd/lakefs/cmd and is never imported back. Registering
// on the root command from this package's init is what keeps the two binaries from drifting:
// cmd/lakefs and cmd/lakefs-sso expose the command by importing this package, and neither defines
// a copy of it.
//
// The indexer of METADATA_SEARCH_REALIGN.md RA4b (X13) runs on the server's own configuration: it
// opens the same KV store from database.*, builds pkg/catalog over the blockstore.* block adapter,
// and reads the same metadata_search section. Run one replica.
package mdscmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/spf13/cobra"
	cmdcore "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/mds"
	"github.com/treeverse/lakefs/pkg/mds/reader"
	"github.com/treeverse/lakefs/pkg/mds/startupcheck"
	"github.com/treeverse/lakefs/pkg/upload"
)

// The static errors of the command. They are wrapped with the details an operator needs, so a
// caller can match them and the message still names the fix.
var (
	// errConfigRequired reports a run without the --config flag.
	errConfigRequired = errors.New("mds run needs --config")

	// errMissingBlockstoreSection reports a blockstore.type without the section of that type.
	errMissingBlockstoreSection = errors.New("the blockstore configuration of that type is missing")
)

// configFlagName is the root's persistent --config flag. `mds run` deliberately does not define a
// second one: the official lakeFS chart invokes the command as `lakefs mds run --config
// /app/config.yaml`, and that has to be the same file `lakefs run` reads.
const configFlagName = "config"

//nolint:gochecknoinits
func init() {
	cmdcore.GetRoot().AddCommand(NewMDSCommand())
}

// NewMDSCommand returns the "mds" command group: the Metadata Search indexer of
// METADATA_SEARCH_REALIGN.md RA4b (X13). It is a subcommand of the server binary and runs on the
// server's own configuration.
func NewMDSCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mds",
		Short: "Metadata Search indexer",
		Long: `Commands of the Metadata Search indexer.

The indexer keeps the system.object_metadata table of every configured data repository R in its
companion repository R-metadata in sync with the commits of R, so that Iceberg engines can query
system.metadata.<R>.<ref>.objects. It runs on the lakeFS server configuration: the same database,
the same blockstore and the same auth key, and it reads the repositories in process through
pkg/catalog.`,
	}
	cmd.AddCommand(newMDSRunCommand())
	return cmd
}

// newMDSRunCommand returns "mds run". It loads the full lakeFS configuration the way `run` does
// (the --config file plus LAKEFS_* environment variables), opens the KV store and the catalog it
// describes, serves /_health, /metrics and /_pprof/ on metadata_search.listen_address, and runs
// until SIGTERM or SIGINT (§4.1, §9).
func newMDSRunCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the Metadata Search indexer",
		Long: `Runs the Metadata Search sync.

The command reads the whole lakeFS configuration, exactly as "lakefs run" does: the file given with
--config plus the LAKEFS_* environment variables. It opens the KV store from database.*, builds the
catalog over the blockstore.* adapter and reads auth.encrypt.secret_key, so a missing or
unreachable database fails startup the way it does for the server. Data repositories are read in
process; the metadata tables are written through the lakeFS Iceberg REST catalog at
<metadata_search.lakefs_endpoint>/iceberg/api.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfgFile, err := cmd.Flags().GetString(configFlagName)
			if err != nil {
				return fmt.Errorf("mds run: read --%s: %w", configFlagName, err)
			}
			if cfgFile == "" {
				return fmt.Errorf("%w: --%s is the lakeFS configuration file to run on", errConfigRequired, configFlagName)
			}

			// The full server configuration first, so that viper holds the file and the LAKEFS_*
			// environment when the metadata_search section is read below. LoadConfig is the same
			// loader `lakefs run` uses, including its startup failures.
			fullCfg := cmdcore.LoadConfig()
			cfg, err := mds.LoadConfig()
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()

			return runMDS(ctx, fullCfg, cfg)
		},
	}
}

// runMDS is the body of `mds run`: it opens the KV store and the catalog of the server
// configuration, builds the in-process reader the indexer works through, and serves the indexer
// until ctx is cancelled.
//
// It returns an error for every startup failure - an unreachable database, a catalog that cannot
// be built, a bad listen address - so the process does not start half-configured, the way `run`
// stops on the same failures.
func runMDS(ctx context.Context, fullCfg config.Config, cfg *mds.Config) error {
	log := logging.ContextUnavailable().WithField(logging.ServiceNameFieldKey, "mds")

	secretKey := fullCfg.AuthConfig().GetBaseAuthConfig().Encrypt.SecretKey.SecureValue()

	// The guide's startup check (METADATA_SEARCH_REALIGN.md RA5, G12): lakefs_endpoint must answer
	// as the lakeFS Iceberg REST catalog, or the process stops (after two minutes of retries). A
	// catalog that rejects the identity is only logged: the service starts anyway and every sync
	// cycle retries the same authentication.
	if err := startupcheck.CheckCatalog(ctx, startupcheck.CatalogCheckConfig{
		Endpoint:         cfg.LakeFSEndpoint,
		AccessKeyID:      cfg.AccessKeyID,
		SecretKey:        cfg.SecretKey,
		EncryptSecretKey: secretKey,
		Log:              log,
	}); err != nil {
		return err
	}

	kvStore, cat, err := openMDSCatalog(ctx, fullCfg)
	if err != nil {
		return err
	}
	defer kvStore.Close()
	defer func() { _ = cat.Close() }()

	baseCfg := fullCfg.GetBaseConfig()
	storage, err := blockstoreIOProperties(baseCfg)
	if err != nil {
		return err
	}

	log.WithFields(logging.Fields{
		"database":     baseCfg.Database.Type,
		"blockstore":   baseCfg.Blockstore.Type,
		"repositories": len(cfg.Repositories),
	}).Info("mds: running on the lakeFS server configuration")

	return mds.NewService(cfg, log,
		mds.WithLakeFS(reader.New(cat, 0)),
		mds.WithStorage(storage),
		mds.WithEncryptSecretKey(secretKey),
	).Run(ctx)
}

// openMDSCatalog opens the KV store from database.* and builds pkg/catalog over the blockstore.*
// adapter, which is what `run` does with the same configuration. The catalog is returned with the
// store because its close releases resources the store owns; the caller closes both.
//
// The KV drivers it may open (cosmosdb, dynamodb, postgres) are registered by the blank imports of
// cmd/lakefs/cmd's run.go, which both binaries link: the indexer opens the same database the server
// does, so the same drivers have to be in the process.
func openMDSCatalog(ctx context.Context, fullCfg config.Config) (kv.Store, *catalog.Catalog, error) {
	baseCfg := fullCfg.GetBaseConfig()
	kvParams, err := kvparams.NewConfig(&baseCfg.Database)
	if err != nil {
		return nil, nil, fmt.Errorf("mds: build the KV configuration from database.*: %w", err)
	}
	kvStore, err := kv.Open(ctx, kvParams)
	if err != nil {
		return nil, nil, fmt.Errorf("mds: open the KV store from database.*: %w", err)
	}
	// `run` refuses a schema it cannot serve, and so does the indexer: it writes commits to the
	// same metadata as the server.
	if _, err := kv.ValidateSchemaVersion(ctx, kvStore); err != nil && !errors.Is(err, kv.ErrNotFound) {
		kvStore.Close()
		return nil, nil, fmt.Errorf("mds: validate the KV schema version: %w", err)
	}

	cat, err := catalog.New(ctx, catalog.Config{
		Config:       fullCfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	if err != nil {
		kvStore.Close()
		return nil, nil, fmt.Errorf("mds: build the catalog from blockstore.*: %w", err)
	}
	return kvStore, cat, nil
}

// blockstoreIOProperties translates the standard blockstore.* configuration into the iceberg-go IO
// properties a table write uses when the lakeFS Iceberg catalog does not vend credentials (X6,
// RA4b): the sync writes its Parquet, Avro and metadata files with the block adapter's own
// credentials, never with a metadata_search.storage or metadata_search.azure key.
//
// A blockstore without static credentials - GS, and S3 on an instance role - returns no
// properties, which leaves the client's own credential chain (or the catalog's vended session) in
// charge. A local or in-memory store is not a supported Metadata Search deployment
// (pkg/mds/table/catalog.go): its writes fail at the first table file rather than borrowing an IO
// implementation from a test.
func blockstoreIOProperties(baseCfg *config.BaseConfig) (iceberg.Properties, error) {
	props := iceberg.Properties{}
	set := func(key, value string) {
		if value != "" {
			props[key] = value
		}
	}

	switch baseCfg.Blockstore.Type {
	case block.BlockstoreTypeS3:
		s3 := baseCfg.Blockstore.S3
		if s3 == nil {
			return nil, fmt.Errorf("%w: %s needs blockstore.s3", errMissingBlockstoreSection, block.BlockstoreTypeS3)
		}
		set(icebergio.S3ClientRegion, s3.Region)
		set(icebergio.S3Region, s3.Region)
		set(icebergio.S3EndpointURL, s3.Endpoint)
		if s3.Credentials != nil {
			set(icebergio.S3AccessKeyID, s3.Credentials.AccessKeyID.SecureValue())
			set(icebergio.S3SecretAccessKey, s3.Credentials.SecretAccessKey.SecureValue())
			set(icebergio.S3SessionToken, s3.Credentials.SessionToken.SecureValue())
		}
		if s3.ForcePathStyle {
			// iceberg-go names the flag by its inverse: virtual-hosted addressing is what it does
			// by default, and a path-style deployment has to turn it off.
			props[icebergio.S3ForceVirtualAddressing] = "false"
		}
	case block.BlockstoreTypeAzure:
		azure := baseCfg.Blockstore.Azure
		if azure == nil {
			return nil, fmt.Errorf("%w: %s needs blockstore.azure", errMissingBlockstoreSection, block.BlockstoreTypeAzure)
		}
		set(icebergio.ADLSSharedKeyAccountName, azure.StorageAccount)
		set(icebergio.ADLSSharedKeyAccountKey, azure.StorageAccessKey.SecureValue())
	default:
		return nil, nil
	}
	if len(props) == 0 {
		return nil, nil
	}
	return props, nil
}
