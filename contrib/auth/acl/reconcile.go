package acl

import (
	"context"
	"errors"
	"fmt"

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

	// aclTemplateVersionKey is stored in the auth partition. It records the
	// template version that was last reconciled.
	aclTemplateVersionKey = "acl_template_version"
)

var errUnsupportedReconcileService = errors.New("reconcile ACL policies: auth service does not expose a KV store")

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

	partition := []byte(model.PartitionKey)
	versionKey := []byte(model.MetadataKeyPath(aclTemplateVersionKey))
	currentVersion, err := store.Get(ctx, partition, versionKey)
	switch {
	case err == nil && string(currentVersion.Value) == ACLTemplateVersion:
		logger.WithField("template_version", ACLTemplateVersion).
			Debug("ACL policies already reconciled for template version")
		return nil
	case err != nil && !errors.Is(err, kv.ErrNotFound):
		return fmt.Errorf("read ACL template version: %w", err)
	}

	changed, err := reconcileACLPolicies(ctx, svc, logger)
	if err != nil {
		return err
	}
	if err := store.Set(ctx, partition, versionKey, []byte(ACLTemplateVersion)); err != nil {
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
		a.Resource == b.Resource &&
		stringSlicesEqual(a.Action, b.Action) &&
		conditionsEqual(a.Condition, b.Condition)
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
