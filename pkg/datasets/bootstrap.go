// Package datasets implements the lakeFS OSS Datasets control plane (see DATASETS_DESIGN.md).
//
// bootstrap.go holds the built-in datasets policies and groups of design §9.5, plus the
// machine-managed "datasets-all-read" policy described in the note under that section. The
// per-dataset policies (consumer read, immutability guard) live in authz.go; this file only
// creates installation-wide objects, once per startup.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation
// only. No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87, was used.
package datasets

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// Built-in datasets policies, DATASETS_DESIGN.md §9.5. The display names are the policy IDs the
// service writes and attaches.
const (
	// DatasetsFullAccessPolicyName is "datasets:*" on "*"; the data-plane read of every dataset
	// is granted by the managed DatasetsAllReadPolicyName policy below, not by this one.
	DatasetsFullAccessPolicyName = "DatasetsFullAccess"
	// DatasetsReadWriteAllPolicyName covers listing, creating, reading, publishing and
	// proposing on every dataset. It intentionally does not include DeleteDataset,
	// ManageAccess or ManageMetadataKeys.
	DatasetsReadWriteAllPolicyName = "DatasetsReadWriteAll"
	// DatasetsReadAllPolicyName covers listing and reading datasets.
	DatasetsReadAllPolicyName = "DatasetsReadAll"
	// DatasetsManageMetadataKeysPolicyName covers metadata key CRUD (§5.5).
	DatasetsManageMetadataKeysPolicyName = "DatasetsManageMetadataKeys"

	// DatasetsAllReadPolicyName is the machine-managed policy described in the note under §9.5.
	// A dataset is a repository with an arbitrary name, so "read the data of every dataset"
	// cannot be expressed as one ARN: the service rewrites this policy's statements from the
	// current dataset list (EnsureDatasetsAllReadPolicy) whenever a dataset is created or
	// deleted, and the full-access and read-all groups get it attached.
	DatasetsAllReadPolicyName = "datasets-all-read"
)

// Built-in datasets groups, DATASETS_DESIGN.md §9.5. They exist so that Entra app roles can be
// mapped onto them through the existing group sync (sso.groups_claim).
const (
	DatasetAdminsGroup     = "DatasetAdmins"
	DatasetPublishersGroup = "DatasetPublishers"
	DatasetConsumersGroup  = "DatasetConsumers"
)

// datasetReadActions, datasetObjectReadActions and datasetGlobalListActions - the action sets of
// the consumer read policy (§9.2) - live in authz.go; DatasetsAllReadPolicy below reuses them so
// that the per-dataset and the installation-wide read grants cannot drift apart.

// datasetsFullAccessPolicy is the §9.5 DatasetsFullAccess policy.
func datasetsFullAccessPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsFullAccessPolicyName,
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectAllow,
				Action:   []string{"datasets:*"},
				Resource: permissions.All,
			},
		},
	}
}

// datasetsReadWriteAllPolicy is the §9.5 DatasetsReadWriteAll policy:
// datasets:{ListDatasets, CreateDataset, ReadDataset, PublishVersion, CreateProposal} on "*".
// The design writes the action set in brace notation with a trailing "*"; it is expanded here to
// the five exact action names, because those are the only actions with those prefixes and an
// exact list cannot silently grow when a later phase adds an action (for example
// datasets:DeleteDataset).
func datasetsReadWriteAllPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsReadWriteAllPolicyName,
		Statement: model.Statements{
			{
				Effect: model.StatementEffectAllow,
				Action: []string{
					ActionListDatasets,
					ActionCreateDataset,
					ActionReadDataset,
					ActionPublishVersion,
					ActionCreateProposal,
				},
				Resource: permissions.All,
			},
		},
	}
}

// datasetsReadAllPolicy is the §9.5 DatasetsReadAll policy.
func datasetsReadAllPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsReadAllPolicyName,
		Statement: model.Statements{
			{
				Effect: model.StatementEffectAllow,
				Action: []string{
					ActionListDatasets,
					ActionReadDataset,
				},
				Resource: permissions.All,
			},
		},
	}
}

// datasetsManageMetadataKeysPolicy is the §9.5 DatasetsManageMetadataKeys policy.
func datasetsManageMetadataKeysPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsManageMetadataKeysPolicyName,
		Statement: model.Statements{
			{
				Effect: model.StatementEffectAllow,
				Action: []string{
					ActionManageMetadataKeys,
				},
				Resource: permissions.All,
			},
		},
	}
}

// DatasetsAllReadPolicy builds the managed "datasets-all-read" policy (§9.5 note) for
// datasetNames: the data-plane read statements of the consumer read policy (§9.2), repeated per
// dataset, plus one installation-wide statement that lets the holder list repositories and
// datasets.
//
// Names are sorted and deduplicated, so the same dataset set always produces the same policy,
// which is what makes EnsureDatasetsAllReadPolicy able to tell "no change" from "changed".
// Datasets that do not exist yet simply produce no statement; when datasetNames is empty the
// policy is the list statement alone and grants no data access at all.
func DatasetsAllReadPolicy(datasetNames []string, ts time.Time) *model.Policy {
	names := uniqueSortedStrings(datasetNames)
	statements := make(model.Statements, 0, 3*len(names)+1)
	for _, name := range names {
		statements = append(statements,
			model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   append([]string(nil), datasetReadActions...),
				Resource: permissions.RepoArn(name),
			},
			model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   append([]string(nil), datasetObjectReadActions...),
				Resource: permissions.ObjectArn(name, "*"),
			},
			model.Statement{
				Effect:   model.StatementEffectAllow,
				Action:   []string{ActionReadDataset},
				Resource: DatasetArn(name),
			},
		)
	}
	statements = append(statements, model.Statement{
		Effect:   model.StatementEffectAllow,
		Action:   append([]string(nil), datasetGlobalListActions...),
		Resource: permissions.All,
	})
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsAllReadPolicyName,
		Statement:   statements,
	}
}

// EnsureDatasetsAllReadPolicy creates the managed "datasets-all-read" policy for datasetNames, or
// rewrites it when the set of datasets changed (the service calls this after creating or deleting
// a dataset, always with the complete current dataset list).
//
// It reports whether anything was written. This policy is machine-managed: hand edits are lost
// when the dataset set changes, which is what "managed" means. Unlike the built-in policies of
// §9.5 it is not a template an administrator is expected to edit.
func EnsureDatasetsAllReadPolicy(ctx context.Context, authService auth.Service, datasetNames []string, ts time.Time) (bool, error) {
	if authService == nil {
		return false, ErrNoAuthorizer
	}
	desired := DatasetsAllReadPolicy(datasetNames, ts)

	existing, err := authService.GetPolicy(ctx, DatasetsAllReadPolicyName)
	switch {
	case err == nil:
		if statementsEqual(existing.Statement, desired.Statement) {
			return false, nil
		}
		if err := authService.WritePolicy(ctx, desired, true /* update */); err != nil {
			return false, err
		}
		return true, nil
	case errors.Is(err, auth.ErrNotFound):
		if err := authService.WritePolicy(ctx, desired, false /* create */); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, err
	}
}

// DetectDatasetsAllReadDrift reports whether the managed "datasets-all-read" policy is missing or
// does not match datasetNames. The reconciler (§12.3 item 3) uses it to report drift without
// writing; EnsureDatasetsAllReadPolicy is the repair.
func DetectDatasetsAllReadDrift(ctx context.Context, authService auth.Service, datasetNames []string, ts time.Time) (bool, error) {
	if authService == nil {
		return false, ErrNoAuthorizer
	}
	desired := DatasetsAllReadPolicy(datasetNames, ts)
	existing, err := authService.GetPolicy(ctx, DatasetsAllReadPolicyName)
	if errors.Is(err, auth.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return !statementsEqual(existing.Statement, desired.Statement), nil
}

// statementsEqual reports whether a and b are the same statement list.
//
// It ignores the difference between a nil Condition map and an empty one. Storing a policy goes
// through the KV protobuf encoding, which materialises an empty map, so a statement written with
// no condition reads back as `Condition: map[...]{}`. Without this normalisation every
// EnsureDatasetsAllReadPolicy call would believe the managed policy changed and rewrite it on
// every startup.
func statementsEqual(a, b model.Statements) bool {
	return reflect.DeepEqual(normalizeStatements(a), normalizeStatements(b))
}

// normalizeStatements returns a copy of statements with every empty Condition map set to nil.
func normalizeStatements(statements model.Statements) model.Statements {
	if statements == nil {
		return nil
	}
	out := make(model.Statements, len(statements))
	for i, stmt := range statements {
		if len(stmt.Condition) == 0 {
			stmt.Condition = nil
		}
		out[i] = stmt
	}
	return out
}

// BootstrapConfig holds the optional inputs of Bootstrap.
type BootstrapConfig struct {
	// Datasets are the datasets that exist right now. They seed the managed datasets-all-read
	// policy; when they are unknown at startup the policy is created with the list statement
	// alone and the service adds data-plane statements on the next create/delete.
	Datasets []string
}

// BootstrapOption configures Bootstrap.
type BootstrapOption func(*BootstrapConfig)

// WithBootstrapDatasets passes the current dataset names to Bootstrap, so that a restart does
// not temporarily drop data-plane access for the read-all groups.
func WithBootstrapDatasets(datasetNames []string) BootstrapOption {
	return func(c *BootstrapConfig) {
		c.Datasets = datasetNames
	}
}

// Bootstrap creates the built-in datasets policies and groups of DATASETS_DESIGN.md §9.5:
//
//	policies: DatasetsFullAccess, DatasetsReadWriteAll, DatasetsReadAll,
//	          DatasetsManageMetadataKeys, and the managed datasets-all-read
//	groups:   DatasetAdmins, DatasetPublishers, DatasetConsumers
//
// and attaches the policies to the groups:
//
//	DatasetAdmins     DatasetsFullAccess, datasets-all-read
//	DatasetPublishers DatasetsReadWriteAll
//	DatasetConsumers  DatasetsReadAll, datasets-all-read
//
// The data-plane read of every dataset is granted by the managed datasets-all-read policy (§9.5
// note), which only the admins and the consumers get: a publisher may publish a definition, and
// reading the data it publishes is a separate grant - per dataset through the consumer read
// policy of §9.2, or installation-wide by putting the publisher in DatasetAdmins.
//
// Bootstrap is idempotent: it is safe to run on every startup.
//
//   - Policies and groups are created with "create if absent" semantics and auth.ErrAlreadyExists
//     is treated as success, so a second run creates nothing. Existing built-in policies are
//     never overwritten: an installation that edited DatasetsReadAll keeps its edit, and the
//     reconciler reports drift instead of fighting the administrator (the RBAC bootstrap in
//     pkg/auth/setup makes the same promise).
//   - Attachments likewise tolerate auth.ErrAlreadyExists, so re-running re-creates attachments
//     that were detached by hand - including the datasets-all-read attachment - without
//     duplicating anything.
//   - The one exception is datasets-all-read, which is machine-managed: it is rewritten only when
//     its statement set differs from the current dataset list (see EnsureDatasetsAllReadPolicy).
//
// Because nothing here is a no-op that depends on ordering, a partially failed run can simply be
// retried.
//
// ts is the creation timestamp recorded on newly written policies and groups (the server passes
// the setup timestamp, as the core bootstrap does).
func Bootstrap(ctx context.Context, authService auth.Service, ts time.Time, opts ...BootstrapOption) error {
	if authService == nil {
		return ErrNoAuthorizer
	}
	cfg := &BootstrapConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	if err := createDatasetPolicies(ctx, authService, []*model.Policy{
		datasetsFullAccessPolicy(ts),
		datasetsReadWriteAllPolicy(ts),
		datasetsReadAllPolicy(ts),
		datasetsManageMetadataKeysPolicy(ts),
	}); err != nil {
		return err
	}

	if err := createDatasetGroups(ctx, authService, ts, []string{
		DatasetAdminsGroup,
		DatasetPublishersGroup,
		DatasetConsumersGroup,
	}); err != nil {
		return err
	}

	if _, err := EnsureDatasetsAllReadPolicy(ctx, authService, cfg.Datasets, ts); err != nil {
		return err
	}

	attachments := []struct {
		group    string
		policies []string
	}{
		{DatasetAdminsGroup, []string{DatasetsFullAccessPolicyName, DatasetsAllReadPolicyName}},
		{DatasetPublishersGroup, []string{DatasetsReadWriteAllPolicyName}},
		{DatasetConsumersGroup, []string{DatasetsReadAllPolicyName, DatasetsAllReadPolicyName}},
	}
	for _, attachment := range attachments {
		if err := attachDatasetPolicies(ctx, authService, attachment.group, attachment.policies); err != nil {
			return err
		}
	}
	return nil
}

// createDatasetPolicies writes every policy that does not exist yet, ignoring auth.ErrAlreadyExists.
func createDatasetPolicies(ctx context.Context, authService auth.Service, policies []*model.Policy) error {
	for _, policy := range policies {
		err := authService.WritePolicy(ctx, policy, false /* create only */)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

// createDatasetGroups creates every group that does not exist yet, ignoring auth.ErrAlreadyExists.
func createDatasetGroups(ctx context.Context, authService auth.Service, ts time.Time, groups []string) error {
	for _, group := range groups {
		_, err := authService.CreateGroup(ctx, &model.Group{CreatedAt: ts, DisplayName: group})
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

// attachDatasetPolicies attaches policies to group, ignoring auth.ErrAlreadyExists. It is a
// re-attach, not a replace: detaching a built-in policy from a built-in group is undone on the
// next bootstrap, which is what the reconciler promises for the immutability guard too (§12.3).
func attachDatasetPolicies(ctx context.Context, authService auth.Service, group string, policies []string) error {
	for _, policy := range policies {
		err := authService.AttachPolicyToGroup(ctx, policy, group)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

// uniqueSortedStrings returns a sorted copy of values without duplicates or empty strings.
func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
