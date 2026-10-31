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
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// Built-in datasets policies, DATASETS_DESIGN.md §9.5. The display names are the policy IDs the
// service writes and attaches.
const (
	// DatasetsFullAccessPolicyName is "dataset:*" on "*"; the data-plane read of every dataset
	// is granted by the managed DatasetsAllReadPolicyName policy below, not by this one.
	DatasetsFullAccessPolicyName = "DatasetsFullAccess"
	// DatasetsReadWriteAllPolicyName covers listing, creating, reading and updating every
	// dataset. It intentionally does not include DeleteDataset or ManageAccess.
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
				Action:   []string{"dataset:*"},
				Resource: DatasetArn("*"),
			},
		},
	}
}

// datasetsReadWriteAllPolicy is the §9.5 DatasetsReadWriteAll policy: list, read, create and
// update every dataset, plus reading the shared metadata keys.
func datasetsReadWriteAllPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: DatasetsReadWriteAllPolicyName,
		Statement: model.Statements{
			{
				Effect: model.StatementEffectAllow,
				Action: []string{
					ActionListDatasets,
					ActionReadDataset,
					ActionCreateDataset,
					ActionUpdateDataset,
					ActionListMetadataKeys,
					ActionReadMetadataKey,
				},
				Resource: DatasetArn("*"),
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
					ActionListMetadataKeys,
					ActionReadMetadataKey,
				},
				Resource: DatasetArn("*"),
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
					ActionListMetadataKeys,
					ActionReadMetadataKey,
					ActionCreateMetadataKey,
					ActionUpdateMetadataKey,
					ActionDeleteMetadataKey,
				},
				Resource: DatasetArn("*"),
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

// legacyDatasetArnPrefix and legacyActionPrefix are the plural names used before the service
// was renamed to match the public RBAC documentation. They are read only by the migration below.
const (
	legacyDatasetArnPrefix = "arn:lakefs:datasets:::"
	legacyActionPrefix     = "datasets:"
)

// IsLegacyManagedPolicy reports whether name is one of the policies this package manages and
// whose statements may still carry the pre-rename dataset actions or ARNs.
func IsLegacyManagedPolicy(name string) bool {
	switch name {
	case DatasetsFullAccessPolicyName, DatasetsReadWriteAllPolicyName, DatasetsReadAllPolicyName,
		DatasetsManageMetadataKeysPolicyName, DatasetsAllReadPolicyName, DatasetsPinsProtectPolicyName:
		return true
	}
	return strings.HasPrefix(name, "dataset-") &&
		(strings.HasSuffix(name, "-read") || strings.HasSuffix(name, "-immutable"))
}

// MigrateLegacyManagedPolicies rewrites the legacy plural datasets actions and ARN prefix in the
// managed policies (the built-ins and the per-dataset read / immutability policies) to their
// singular dataset names. It reports whether any policy was written.
//
// The migration is idempotent: it rewrites only statements that still contain a legacy action or
// resource, so a policy that has already been migrated is not touched.
func MigrateLegacyManagedPolicies(ctx context.Context, authService auth.Service) (bool, error) {
	if authService == nil {
		return false, ErrNoAuthorizer
	}
	policies, _, err := authService.ListPolicies(ctx, &model.PaginationParams{Amount: -1})
	if err != nil {
		return false, err
	}
	changed := false
	for _, policy := range policies {
		if !IsLegacyManagedPolicy(policy.DisplayName) {
			continue
		}
		statements, migrated := migrateLegacyStatements(policy.Statement)
		if !migrated {
			continue
		}
		policy.Statement = statements
		if err := authService.WritePolicy(ctx, policy, true /* update */); err != nil {
			return changed, err
		}
		changed = true
	}
	return changed, nil
}

// migrateLegacyStatements returns a migrated copy of statements and reports whether anything
// changed.
func migrateLegacyStatements(statements model.Statements) (model.Statements, bool) {
	if statements == nil {
		return nil, false
	}
	out := make(model.Statements, len(statements))
	changed := false
	for i, statement := range statements {
		statement.Action, changed = mergeMigration(statement.Action, migrateLegacyActions(statement.Action), changed)
		if resource := migrateLegacyResource(statement.Resource); resource != statement.Resource {
			statement.Resource = resource
			changed = true
		}
		out[i] = statement
	}
	return out, changed
}

// migrateLegacyActions maps one statement's legacy action list to the singular namespace. A
// legacy PublishVersion or CreateProposal becomes UpdateDataset; the coarse ManageMetadataKeys
// action expands to the five documented metadata-key actions.
func migrateLegacyActions(actions []string) []string {
	out := make([]string, 0, len(actions))
	seen := make(map[string]struct{}, len(actions))
	add := func(items ...string) {
		for _, item := range items {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	for _, action := range actions {
		switch action {
		case legacyActionPrefix + "*":
			add("dataset:*")
		case legacyActionPrefix + "PublishVersion", legacyActionPrefix + "CreateProposal":
			add(ActionUpdateDataset)
		case legacyActionPrefix + "ManageMetadataKeys":
			add(ActionListMetadataKeys, ActionReadMetadataKey,
				ActionCreateMetadataKey, ActionUpdateMetadataKey, ActionDeleteMetadataKey)
		default:
			if strings.HasPrefix(action, legacyActionPrefix) {
				add("dataset:" + strings.TrimPrefix(action, legacyActionPrefix))
				continue
			}
			add(action)
		}
	}
	return out
}

// migrateLegacyResource maps a legacy datasets ARN to the singular prefix. A legacy metadata-key
// ARN becomes the documented dataset/* resource.
func migrateLegacyResource(resource string) string {
	resource = strings.ReplaceAll(resource, legacyDatasetArnPrefix+"metadata-key/", datasetArnPrefix+"dataset/*")
	return strings.ReplaceAll(resource, legacyDatasetArnPrefix, datasetArnPrefix)
}

// mergeMigration returns migrated and reports whether it differs from original.
func mergeMigration(original, migrated []string, changed bool) ([]string, bool) {
	if len(original) != len(migrated) {
		return migrated, true
	}
	for i := range original {
		if original[i] != migrated[i] {
			return migrated, true
		}
	}
	return original, changed
}

// BootstrapConfig holds the optional inputs of Bootstrap.
type BootstrapConfig struct {
	// Datasets are the datasets that exist right now. They seed the managed datasets-all-read
	// policy; when they are unknown at startup the policy is created with the list statement
	// alone and the service adds data-plane statements on the next create/delete.
	Datasets []string

	// PinsProtectGroups lists extra groups that get the datasets-pins-protect deny policy (§8.2)
	// in addition to the base groups of both group templates. The server passes
	// datasets.immutable_guard_groups, the same list that extends the immutability guard.
	PinsProtectGroups []string
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

// WithPinsProtectGroups passes extra groups that must also receive the datasets-pins-protect deny
// policy of §8.2 (datasets.immutable_guard_groups).
func WithPinsProtectGroups(groups []string) BootstrapOption {
	return func(c *BootstrapConfig) {
		c.PinsProtectGroups = groups
	}
}

// Bootstrap creates the built-in datasets policies and groups of DATASETS_DESIGN.md §9.5, plus the
// installation-wide datasets-pins-protect deny policy of §8.2:
//
//	policies: DatasetsFullAccess, DatasetsReadWriteAll, DatasetsReadAll,
//	          DatasetsManageMetadataKeys, datasets-pins-protect, and the managed datasets-all-read
//	groups:   DatasetAdmins, DatasetPublishers, DatasetConsumers
//
// and attaches the policies to the groups:
//
//	DatasetAdmins     DatasetsFullAccess, datasets-all-read
//	DatasetPublishers DatasetsReadWriteAll
//	DatasetConsumers  DatasetsReadAll, datasets-all-read
//
// datasets-pins-protect is not attached to a dataset group: it protects *source* repositories, so
// it goes to the base groups of both group templates (§9.4's non-service principals) plus the
// extra groups of datasets.immutable_guard_groups. Attaching it to a group that does not exist in
// this installation is skipped rather than fatal (see attachPinsProtectPolicy).
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

	if _, err := MigrateLegacyManagedPolicies(ctx, authService); err != nil {
		return err
	}

	if err := createDatasetPolicies(ctx, authService, []*model.Policy{
		datasetsFullAccessPolicy(ts),
		datasetsReadWriteAllPolicy(ts),
		datasetsReadAllPolicy(ts),
		datasetsManageMetadataKeysPolicy(ts),
		PinsProtectPolicy(ts),
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
	return attachPinsProtectPolicy(ctx, authService, cfg.PinsProtectGroups)
}

// attachPinsProtectPolicy attaches datasets-pins-protect (§8.2) to the non-service groups of the
// installation, tolerating groups that do not exist.
//
// Unlike the §9.5 attachments, a missing group is not an error here: the base group list covers both
// group templates (simplified ACL and RBAC, see immutabilityGuardBaseGroups), and no installation
// runs both, so most of the names are expected to be absent. Tolerating auth.ErrNotFound keeps the
// deny policy out of the way of a template the installation does not use, while still covering every
// principal that is a member of a group the installation does have.
func attachPinsProtectPolicy(ctx context.Context, authService auth.Service, extraGroups []string) error {
	for _, group := range PinsProtectGroups(extraGroups) {
		err := authService.AttachPolicyToGroup(ctx, DatasetsPinsProtectPolicyName, group)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) && !errors.Is(err, auth.ErrNotFound) {
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
//
// A group that does not exist in this installation's template (the ACL groups vs the RBAC
// groups, §9.4) is skipped with a debug log rather than failing the whole bootstrap: the
// datasets-pins-protect attachment already does the same for the base groups.
func attachDatasetPolicies(ctx context.Context, authService auth.Service, group string, policies []string) error {
	for _, policy := range policies {
		err := authService.AttachPolicyToGroup(ctx, policy, group)
		switch {
		case err == nil, errors.Is(err, auth.ErrAlreadyExists):
		case errors.Is(err, auth.ErrNotFound):
			logging.FromContext(ctx).WithError(err).WithField("group", group).
				Debug("datasets: group is not part of this installation's template; skipping policy attachment")
		default:
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
