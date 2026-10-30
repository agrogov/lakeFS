package acl

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	// ACLTemplateVersion is the version of the statement templates produced by
	// ACLToStatement. Bump it whenever those templates change, so that
	// ReconcileACLPolicies rewrites existing ACL-derived policies exactly once.
	ACLTemplateVersion = "1"

	// ACLTemplateVersionKeyName is the record, inside the "auth" KV partition, that holds the
	// ACL template version last applied by ReconcileACLPolicies. Record it through
	// TemplateVersionKey.
	ACLTemplateVersionKeyName = "acl-template-version"

	// legacyACLTemplateVersionKeyName is the pre-R1 name of ACLTemplateVersionKeyName. It used
	// the same "metadata/" path but snake_case. It is only read, so an installation that
	// reconciled under it is migrated rather than reconciled a second time, and a rollback to an
	// older binary still finds its own record.
	legacyACLTemplateVersionKeyName = "acl_template_version"
)

var errUnsupportedReconcileService = errors.New("reconcile ACL policies: auth service does not expose a KV store")

// TemplateVersionKey returns the KV key of a template-version record in the "auth" partition
// (model.PartitionKey). Every such record is built by this helper, so they share one convention:
// the "metadata/" path (model.MetadataKeyPath) and a kebab-case name, instead of bare keys mixed
// into the partition root next to the groups and policies.
//
// The RBAC base template (pkg/sso.EnsureRBACBase) and the ACL statement templates (this package)
// still keep two distinct records, deliberately: they version two independent template sets that
// are applied by independent code paths, are bumped independently, and do not share a value space
// (a string here, an int there). RBAC_DESIGN.md §4.5 #2 asked for a single shared key; a single
// mutable record would have to be read-modify-written by both guards, and two lakeFS instances
// booting concurrently would then drop each other's field. Sharing the convention, the partition
// and this helper while keeping one record per template is the deliberate deviation.
//
// This package must not import pkg/sso (§6), so the helper cannot live there; pkg/sso already
// depends on contrib/auth/acl, which is what lets both guards share it from here.
func TemplateVersionKey(name string) []byte {
	return []byte(model.MetadataKeyPath(name))
}

// ReadTemplateVersion returns the version recorded under TemplateVersionKey(name) and whether it
// was found, falling back to the given legacy keys. A version found under a legacy key is copied
// to the current key, so the fallback is used once per installation, and installations that
// recorded their version under an older key are not treated as never-bootstrapped.
func ReadTemplateVersion(ctx context.Context, store kv.Store, name string, legacyKeys ...[]byte) (string, bool, error) {
	partition := []byte(model.PartitionKey)
	key := TemplateVersionKey(name)
	value, err := store.Get(ctx, partition, key)
	switch {
	case err == nil:
		return string(value.Value), true, nil
	case !errors.Is(err, kv.ErrNotFound):
		return "", false, fmt.Errorf("read %s: %w", name, err)
	}

	for _, legacyKey := range legacyKeys {
		legacyValue, err := store.Get(ctx, partition, legacyKey)
		switch {
		case err == nil:
			// Copy the legacy record forward before using it: if the copy fails the next start
			// retries the migration instead of silently degrading to a re-bootstrap.
			if err := store.Set(ctx, partition, key, legacyValue.Value); err != nil {
				return "", false, fmt.Errorf("migrate %s: %w", name, err)
			}
			return string(legacyValue.Value), true, nil
		case !errors.Is(err, kv.ErrNotFound):
			return "", false, fmt.Errorf("read legacy %s: %w", name, err)
		}
	}
	return "", false, nil
}

type aclStoreProvider interface {
	aclKVStore() kv.Store
}

// aclKVStore exposes the service's KV store to reconciliation in this package.
// It works both for *AuthService and for wrappers that embed it.
func (s *AuthService) aclKVStore() kv.Store {
	return s.store
}

// ReconcileACLPolicies rewrites policies that carry a non-empty ACL when their stored
// statements no longer match the current ACL templates. Policies without an ACL are
// custom policies and are never modified. The reconciliation is guarded by a template
// version key, so it runs once per ACLTemplateVersion.
func ReconcileACLPolicies(ctx context.Context, svc auth.Service, logger logging.Logger) error {
	storeProvider, ok := svc.(aclStoreProvider)
	if !ok {
		return fmt.Errorf("%w: %T", errUnsupportedReconcileService, svc)
	}
	store := storeProvider.aclKVStore()

	key := TemplateVersionKey(ACLTemplateVersionKeyName)
	applied, found, err := ReadTemplateVersion(ctx, store, ACLTemplateVersionKeyName,
		TemplateVersionKey(legacyACLTemplateVersionKeyName))
	if err != nil {
		return fmt.Errorf("read ACL template version: %w", err)
	}
	if found && applied == ACLTemplateVersion {
		logger.WithField("template_version", ACLTemplateVersion).
			Debug("ACL policies already reconciled for template version")
		return nil
	}

	changed, err := reconcileACLPolicies(ctx, svc, logger)
	if err != nil {
		return err
	}
	if err := store.Set(ctx, []byte(model.PartitionKey), key, []byte(ACLTemplateVersion)); err != nil {
		return fmt.Errorf("write ACL template version: %w", err)
	}
	logger.WithFields(logging.Fields{
		"template_version": ACLTemplateVersion,
		"policies_changed": changed,
	}).Info("Reconciled ACL policies")
	return nil
}

func reconcileACLPolicies(ctx context.Context, svc auth.Service, logger logging.Logger) (int, error) {
	changed := 0
	params := &model.PaginationParams{Amount: -1}
	for {
		policies, paginator, err := svc.ListPolicies(ctx, params)
		if err != nil {
			return changed, fmt.Errorf("list policies: %w", err)
		}
		for _, policy := range policies {
			if policy.ACL.Permission == "" {
				continue
			}
			statements, err := ACLToStatement(policy.ACL)
			if err != nil {
				return changed, fmt.Errorf("policy %s: %w", policy.DisplayName, err)
			}
			if statementsEqual(policy.Statement, statements) {
				continue
			}

			updated := *policy
			updated.Statement = statements
			if err := svc.WritePolicy(ctx, &updated, true); err != nil {
				return changed, fmt.Errorf("update policy %s: %w", policy.DisplayName, err)
			}
			changed++
			logger.WithFields(logging.Fields{
				"policy":           policy.DisplayName,
				"acl":              policy.ACL.Permission,
				"template_version": ACLTemplateVersion,
			}).Info("Updated ACL policy from current permission templates")
		}
		if paginator == nil || paginator.NextPageToken == "" {
			return changed, nil
		}
		params.After = paginator.NextPageToken
	}
}

// statementsEqual compares statements semantically. KV protobuf encoding turns a nil
// Condition map into an empty one, and nil and empty action lists are equivalent.
func statementsEqual(a, b model.Statements) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !statementEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func statementEqual(a, b model.Statement) bool {
	return a.Effect == b.Effect &&
		resourcesEqual(a.Resource, b.Resource) &&
		stringSlicesEqual(a.Action, b.Action) &&
		conditionsEqual(a.Condition, b.Condition)
}

// resourcesEqual compares the Resource field semantically. Resource is either a single ARN or a
// JSON-encoded array of ARNs (auth.ParsePolicyResourceAsList), and the array grants any of its
// elements, so it is a set: `["*","arn:..."]` and a re-encoded or re-ordered equivalent are the
// same grant and must not trigger a rewrite. Byte equality is checked first, so the common case
// (the templates emit a single bare ARN) stays a string compare.
func resourcesEqual(a, b string) bool {
	if a == b {
		return true
	}
	aResources, err := auth.ParsePolicyResourceAsList(a)
	if err != nil {
		return false
	}
	bResources, err := auth.ParsePolicyResourceAsList(b)
	if err != nil {
		return false
	}
	slices.Sort(aResources)
	slices.Sort(bResources)
	return slices.Equal(aResources, bResources)
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func conditionsEqual(a, b map[string]map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, aOps := range a {
		bOps, ok := b[key]
		if !ok || len(aOps) != len(bOps) {
			return false
		}
		for op, aValues := range aOps {
			bValues, ok := bOps[op]
			if !ok || !stringSlicesEqual(aValues, bValues) {
				return false
			}
		}
	}
	return true
}
