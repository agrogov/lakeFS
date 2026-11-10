// Package bootstrap provisions the Metadata Search sync identity of the current official guide
// (METADATA_SEARCH_REALIGN.md RA5, G12, X1–X4) on a lakeFS installation: the mds-service-user
// principal and the MDSCatalogAccess policy the sync writes the metadata tables through the lakeFS
// Iceberg REST catalog with. It runs as an extra routes builder of the lakefs-sso binary and mounts
// no route: the identity has to exist before the first indexer request is authorized, whether or
// not a request ever arrives.
//
// The guide's sync identity is deliberately smaller than the one the earlier design had:
//
//   - mds-service-user is a user, not a member of a group, and it carries one policy,
//     MDSCatalogAccess. The sync signs its own login tokens for that user with
//     auth.encrypt.secret_key (pkg/mds/table.SignServiceLoginToken), so there is no credential to
//     create: the operator configures no access key pair at all (G11, G12).
//   - MDSCatalogAccess grants catalog:* on arn:lakefs:catalog:::* - the catalog permissions the
//     sync needs to create the system namespace and the object_metadata table in a companion
//     repository and to commit to it, and nothing outside the Iceberg catalog. The sync reads the
//     data repositories in process through pkg/catalog (RA4b), so it needs no fs: grant on them.
//     The policy is installation-wide because the companion repositories it covers are created
//     while the server runs, and the guide names a single policy rather than per-repository ones.
//   - Another user may run the sync instead (access_key_id/secret_key in metadata_search): that
//     user needs the same MDSCatalogAccess policy attached, which is why the policy is a named,
//     attachable policy rather than an inline grant on the service user.
//
// The bootstrap also removes what the earlier design installed (X1–X3): the mds-indexer user and
// its credentials, the MetadataSearchIndexers group, the mds-indexer allow policies (including
// their numbered chunks), every mds-metadata-repo-protect[-<n>] deny policy, and the read-only
// MDSFSReadAccess policy when it is still attached to mds-service-user (the guide detaches it on
// start). Only names this package owns are removed: an operator policy that merely shares a prefix
// (for example mds-indexer-extra-storage-namespace) survives every run.
//
// The policy is attached to the user, as the guide says. An external auth backend that cannot attach
// a policy to a user (contrib/auth/acl's HTTP controller answers the attachment with 501) therefore
// fails startup with that answer instead of starting a sync that is not authorized; the in-process
// ACL service of the default lakefs-sso deployment and the Enterprise auth service both implement
// the attachment.
//
// There was no Metadata Search part in iceberg-reserved-prefix-protect to remove: that policy
// reserves _lakefs_iceberg/ for the catalog (pkg/icebergcatalog/gc.go) and has never named a
// Metadata Search resource, so RA5 leaves it untouched.
//
// Everything here is idempotent and runs on every start:
//
//   - mds-service-user is created when absent, and auth.ErrAlreadyExists is success;
//   - MDSCatalogAccess is created when absent and rewritten when its statements differ from the
//     built-in ones (a hand-edited or pre-created policy is repaired instead of trusted - the name
//     is predictable, so a principal allowed to write policies could otherwise seed a policy with
//     no statements or with allows), and a rewrite is logged as a warning;
//   - the attachment tolerates auth.ErrAlreadyExists;
//   - every legacy removal treats auth.ErrNotFound as success, so a second start, or a fresh
//     installation that never had the old principals, changes nothing.
//
// The policy backend is probed before anything is written. auth.BasicAuthService - the default auth
// mode of both lakefs binaries - answers every policy call with auth.ErrNotImplemented and stores
// its single administrator user under a fixed key: creating mds-service-user there would rename the
// installation's administrator. Such a service is logged as a warning naming what is not in force
// and the bootstrap continues; every other error - a real failure on a backend that does store
// policies - fails startup, exactly like icebergcatalog.EnsureReservedPrefixProtectPolicy.
//
// Clean-room note (design §2): written from the public lakeFS guide and this repository's code only;
// no lakeFS Enterprise/Cloud code was used.
package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	cmdpkg "github.com/treeverse/lakefs/cmd/lakefs/cmd"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

// The guide's principals and policies. They are exported because an administrator names them when
// running the sync as another user (attach MDSCatalogAccess) or when inspecting the installation.
const (
	// UserName is the principal the sync writes as. The guide provisions it on every lakeFS
	// start; the indexer signs its own login tokens for it with auth.encrypt.secret_key.
	UserName = "mds-service-user"

	// PolicyName is the one policy the sync identity carries: the catalog permissions the sync
	// needs. Another user that runs the sync instead needs this policy attached to it too.
	PolicyName = "MDSCatalogAccess"

	// FSReadPolicyName is the read-only policy the guide detaches from mds-service-user on start.
	// The policy itself is not deleted: it may still be attached to other principals.
	FSReadPolicyName = "MDSFSReadAccess"
)

// The principals and policies of the earlier design that every start removes (X1–X3).
const (
	// LegacyUserName is the old sync user. Its credentials are deleted with it.
	LegacyUserName = "mds-indexer"

	// LegacyGroupName is the old group mds-indexer was a member of. The group is removed; its
	// policy attachments go with it.
	LegacyGroupName = "MetadataSearchIndexers"

	// LegacyPolicyName is the old allow policy. Its numbered chunks (mds-indexer-<n>) are removed
	// with it.
	LegacyPolicyName = "mds-indexer"

	// LegacyMetadataRepoProtectPolicyName is the old deny policy. Its numbered chunks
	// (mds-metadata-repo-protect-<n>) are removed with it.
	LegacyMetadataRepoProtectPolicyName = "mds-metadata-repo-protect"
)

// CatalogAccessResource is the resource MDSCatalogAccess grants catalog:* on: every catalog
// resource of every repository. The sync creates and commits the object_metadata table of each
// configured repository's companion repository, and those companion repositories are created while
// the server runs, so an exact per-repository ARN would need a policy refresher; the guide names a
// single installation-wide policy instead.
const CatalogAccessResource = "arn:lakefs:catalog:::*"

// CatalogAccessActions are the actions MDSCatalogAccess grants. catalog:* is the guide's "catalog
// permissions the sync needs": the namespace and table lifecycle of a companion repository and the
// table-data actions of credential vending, with no fs: action on a data repository.
var CatalogAccessActions = []string{"catalog:*"}

// ErrNoAuthorizer is returned by the bootstrap when it is handed no auth service: the whole point
// of the call is to create the user and attach the policy, so there is nothing to fall back to.
// It is a programming error, like icebergcatalog.ErrNoAuthorizer.
var ErrNoAuthorizer = errors.New("mds bootstrap: no auth service configured")

// ErrMissingDeps is returned by RoutesBuilder while the server passed no auth service or logger:
// the sync identity cannot be created without a service, and the startup summary needs a logger.
var ErrMissingDeps = errors.New("mds bootstrap: the server passed no auth service or logger")

// CatalogAccessPolicy builds MDSCatalogAccess: one allow of the catalog permissions the sync needs
// on every catalog resource. It is an allow on the catalog: API only, so it grants nothing on a
// data repository's objects or branches (the sync reads those in process), and nothing outside the
// catalog.
func CatalogAccessPolicy(ts time.Time) *model.Policy {
	return &model.Policy{
		CreatedAt:   ts,
		DisplayName: PolicyName,
		Statement: model.Statements{
			{
				Effect:   model.StatementEffectAllow,
				Action:   append([]string(nil), CatalogAccessActions...),
				Resource: CatalogAccessResource,
			},
		},
	}
}

// EnsureSyncIdentity makes the guide's sync identity exist, repairs its policy, attaches it, and
// removes the principals and policies of the earlier design. It is idempotent and is called on
// every server start; the bool reports whether the identity is in force, i.e. false exactly when
// the auth service cannot store policies at all (the one degraded startup it accepts).
//
// ts is the creation timestamp recorded on the user and the policy when they have to be created
// (the caller passes time.Now(), as the datasets and catalog bootstraps do).
func EnsureSyncIdentity(ctx context.Context, authService auth.Service, ts time.Time) (bool, error) {
	if authService == nil {
		return false, ErrNoAuthorizer
	}

	// Probe the policy backend before writing anything: BasicAuthService implements CreateUser, but
	// it stores the installation's single administrator under a fixed key, so creating
	// mds-service-user on it would rename that administrator and then fail at the first policy
	// call. Probing here also means a degraded installation is left exactly as it was.
	available, err := policyBackendAvailable(ctx, authService)
	if err != nil || !available {
		return false, err
	}

	if _, err := authService.CreateUser(ctx, &model.User{Username: UserName, CreatedAt: ts}); err != nil &&
		!errors.Is(err, auth.ErrAlreadyExists) {
		return false, fmt.Errorf("mds bootstrap: create user %s: %w", UserName, err)
	}

	inForce, err := ensurePolicy(ctx, authService, CatalogAccessPolicy(ts))
	if err != nil || !inForce {
		return false, err
	}

	if err := attachPolicyToUser(ctx, authService, PolicyName, UserName); err != nil {
		return false, err
	}

	if err := detachPolicyFromUser(ctx, authService, FSReadPolicyName, UserName); err != nil {
		return false, err
	}

	if err := removeLegacyPrincipals(ctx, authService); err != nil {
		return false, err
	}
	return true, nil
}

// RoutesBuilder is the startup hook cmd/lakefs-sso registers with cmd.AddExtraRoutesBuilder. It
// mounts no route; it exists so the sync identity is installed from the same hook loop, and before
// the first request is served, as the catalog's own controls.
//
// It runs on every start, as the guide says lakeFS provisions the user on every start. A deployment
// that does not use Metadata Search gets the otherwise-unused service principal; that is the
// guide's contract, and the alternative - keying it off a configuration flag this card may not
// read - would leave an indexer unable to authenticate against a server that is otherwise ready
// for it.
func RoutesBuilder(ctx context.Context, deps cmdpkg.ServerDeps) error {
	if deps.AuthService == nil || deps.Logger == nil {
		return ErrMissingDeps
	}
	inForce, err := EnsureSyncIdentity(ctx, deps.AuthService, time.Now())
	if err != nil {
		return err
	}
	fields := logging.Fields{
		"user":   UserName,
		"policy": PolicyName,
	}
	if !inForce {
		// The warning EnsureSyncIdentity wrote says why; this line keeps the startup summary from
		// claiming the identity exists when it does not.
		deps.Logger.WithFields(fields).
			Info("metadata search: the mds-service-user sync identity is NOT in place; see the warning")
		return nil
	}
	deps.Logger.WithFields(fields).
		Info("metadata search: the mds-service-user sync identity is in place")
	return nil
}

// attachPolicyToUser attaches policy to user, treating an existing attachment as success. A service
// that cannot attach at this point degrades to the same warning as the probe.
func attachPolicyToUser(ctx context.Context, authService auth.Service, policy, user string) error {
	err := authService.AttachPolicyToUser(ctx, policy, user)
	switch {
	case err == nil, errors.Is(err, auth.ErrAlreadyExists):
		purgePolicyCache(authService)
		return nil
	case policyBackendUnavailable(err):
		logSyncIdentityNotCreated(ctx, err)
		return nil
	default:
		return fmt.Errorf("mds bootstrap: attach policy %s to user %s: %w", policy, user, err)
	}
}

// detachPolicyFromUser detaches policy from user when it is attached, and accepts both "not
// attached" (auth.ErrNotFound) and "policy no longer exists" as success: a fresh installation has
// nothing to detach, and the guide only asks that MDSFSReadAccess not stay attached.
func detachPolicyFromUser(ctx context.Context, authService auth.Service, policy, user string) error {
	attached, err := userHasPolicy(ctx, authService, user, policy)
	if err != nil {
		if policyBackendUnavailable(err) {
			logSyncIdentityNotCreated(ctx, err)
			return nil
		}
		return fmt.Errorf("mds bootstrap: list policies of user %s: %w", user, err)
	}
	if !attached {
		return nil
	}
	if err := authService.DetachPolicyFromUser(ctx, policy, user); err != nil && !errors.Is(err, auth.ErrNotFound) {
		if policyBackendUnavailable(err) {
			logSyncIdentityNotCreated(ctx, err)
			return nil
		}
		return fmt.Errorf("mds bootstrap: detach policy %s from user %s: %w", policy, user, err)
	}
	purgePolicyCache(authService)
	logging.FromContext(ctx).WithFields(logging.Fields{
		"policy": policy,
		"user":   user,
	}).Warn("mds bootstrap: detached the read-only policy the guide removes from the sync identity")
	return nil
}

// userHasPolicy reports whether policy is attached directly to user.
func userHasPolicy(ctx context.Context, authService auth.Service, user, policy string) (bool, error) {
	policies, err := listUserPolicies(ctx, authService, user)
	if err != nil {
		return false, err
	}
	for _, attached := range policies {
		if attached.DisplayName == policy {
			return true, nil
		}
	}
	return false, nil
}

// removeLegacyPrincipals removes what the earlier design installed: mds-indexer (and its
// credentials), MetadataSearchIndexers, the mds-indexer allow policies including their chunks, and
// every mds-metadata-repo-protect policy including its chunks. Each removal treats "not found" as
// success, so a fresh installation, and every start after the first, is a no-op.
func removeLegacyPrincipals(ctx context.Context, authService auth.Service) error {
	if err := removeUserCredentials(ctx, authService, LegacyUserName); err != nil {
		return err
	}
	if err := authService.DeleteUser(ctx, LegacyUserName); err != nil && !errors.Is(err, auth.ErrNotFound) {
		if policyBackendUnavailable(err) {
			logSyncIdentityNotCreated(ctx, err)
			return nil
		}
		return fmt.Errorf("mds bootstrap: remove the legacy user %s: %w", LegacyUserName, err)
	}
	if err := authService.DeleteGroup(ctx, LegacyGroupName); err != nil && !errors.Is(err, auth.ErrNotFound) {
		if policyBackendUnavailable(err) {
			logSyncIdentityNotCreated(ctx, err)
			return nil
		}
		return fmt.Errorf("mds bootstrap: remove the legacy group %s: %w", LegacyGroupName, err)
	}
	for _, base := range []string{LegacyPolicyName, LegacyMetadataRepoProtectPolicyName} {
		if err := removeOwnedPolicies(ctx, authService, base); err != nil {
			return err
		}
	}
	purgePolicyCache(authService)
	logging.FromContext(ctx).WithFields(logging.Fields{
		"user":  LegacyUserName,
		"group": LegacyGroupName,
	}).Info("mds bootstrap: removed the legacy metadata search principals and policies, if they existed")
	return nil
}

// removeUserCredentials deletes every access key of user before the user itself is removed: the
// auth service deletes a user's attachments, not its credentials, and an orphaned key of the old
// service principal must not authenticate anything.
func removeUserCredentials(ctx context.Context, authService auth.Service, user string) error {
	params := &model.PaginationParams{Amount: -1}
	for {
		credentials, paginator, err := authService.ListUserCredentials(ctx, user, params)
		if err != nil {
			if policyBackendUnavailable(err) {
				logSyncIdentityNotCreated(ctx, err)
				return nil
			}
			return fmt.Errorf("mds bootstrap: list credentials of the legacy user %s: %w", user, err)
		}
		for _, credential := range credentials {
			if err := authService.DeleteCredentials(ctx, user, credential.AccessKeyID); err != nil &&
				!errors.Is(err, auth.ErrNotFound) {
				if policyBackendUnavailable(err) {
					logSyncIdentityNotCreated(ctx, err)
					return nil
				}
				return fmt.Errorf("mds bootstrap: remove a credential of the legacy user %s: %w", user, err)
			}
		}
		if paginator == nil || paginator.NextPageToken == "" {
			return nil
		}
		params.After = paginator.NextPageToken
	}
}

// removeOwnedPolicies deletes the base policy base and every numbered chunk of it (base-1,
// base-2, ...). Only names this package owns are considered: an operator policy such as
// mds-indexer-extra-storage-namespace is never deleted.
func removeOwnedPolicies(ctx context.Context, authService auth.Service, base string) error {
	policies, err := listOwnedPolicies(ctx, authService, base)
	if err != nil {
		if policyBackendUnavailable(err) {
			logSyncIdentityNotCreated(ctx, err)
			return nil
		}
		return fmt.Errorf("mds bootstrap: list policies with prefix %s: %w", base, err)
	}
	for _, policy := range policies {
		if err := authService.DeletePolicy(ctx, policy.DisplayName); err != nil && !errors.Is(err, auth.ErrNotFound) {
			if policyBackendUnavailable(err) {
				logSyncIdentityNotCreated(ctx, err)
				return nil
			}
			return fmt.Errorf("mds bootstrap: remove the legacy policy %s: %w", policy.DisplayName, err)
		}
		purgePolicyCache(authService)
		logging.FromContext(ctx).WithField("policy", policy.DisplayName).
			Info("mds bootstrap: removed a legacy metadata search policy")
	}
	return nil
}

// listOwnedPolicies returns every policy of base's set this package owns: the base policy and its
// numeric chunks, and never an operator policy that merely shares the prefix (isOwnedPolicyName).
// The list is paginated, because the old allow policy wrote one chunk per 64 companion repositories.
func listOwnedPolicies(ctx context.Context, authService auth.Service, base string) ([]*model.Policy, error) {
	owned := make([]*model.Policy, 0)
	params := &model.PaginationParams{Prefix: base, Amount: -1}
	for {
		policies, paginator, err := authService.ListPolicies(ctx, params)
		if err != nil {
			return nil, err
		}
		for _, policy := range policies {
			if !isOwnedPolicyName(base, policy.DisplayName) {
				continue
			}
			owned = append(owned, policy)
		}
		if paginator == nil || paginator.NextPageToken == "" {
			return owned, nil
		}
		params.After = paginator.NextPageToken
	}
}

// listUserPolicies returns every policy attached directly to user, paginated.
func listUserPolicies(ctx context.Context, authService auth.Service, user string) ([]*model.Policy, error) {
	attached := make([]*model.Policy, 0)
	params := &model.PaginationParams{Amount: -1}
	for {
		policies, paginator, err := authService.ListUserPolicies(ctx, user, params)
		if err != nil {
			return nil, err
		}
		attached = append(attached, policies...)
		if paginator == nil || paginator.NextPageToken == "" {
			return attached, nil
		}
		params.After = paginator.NextPageToken
	}
}

// isOwnedPolicyName reports whether name is a policy this package writes: the base name or one of
// its numeric chunks. It is deliberately narrower than a prefix match, so an operator policy such
// as mds-indexer-extra-storage-namespace is never deleted as a stale chunk.
func isOwnedPolicyName(base, name string) bool {
	if name == base {
		return true
	}
	suffix, ok := strings.CutPrefix(name, base+"-")
	if !ok || suffix == "" {
		return false
	}
	chunk, err := strconv.Atoi(suffix)
	return err == nil && chunk >= 1 && strconv.Itoa(chunk) == suffix
}

// policyBackendAvailable reports whether authService can manage policies at all, i.e. whether
// reading MDSCatalogAccess answers with a policy or with auth.ErrNotFound rather than with
// auth.ErrNotImplemented. A service that cannot is logged as the one degraded startup
// EnsureSyncIdentity accepts, and the bool is false with a nil error.
func policyBackendAvailable(ctx context.Context, authService auth.Service) (bool, error) {
	_, err := authService.GetPolicy(ctx, PolicyName)
	switch {
	case err == nil, errors.Is(err, auth.ErrNotFound):
		return true, nil
	case policyBackendUnavailable(err):
		logSyncIdentityNotCreated(ctx, err)
		return false, nil
	default:
		return false, fmt.Errorf("mds bootstrap: read policy %s: %w", PolicyName, err)
	}
}

// ensurePolicy makes policy exist with exactly its built-in statements: it creates it when it is
// missing and rewrites it when the statements differ, and reports whether it is in force.
//
// A rewrite is required rather than optional: the name is predictable, so a principal that may
// write policies but holds no Metadata Search permission could pre-create MDSCatalogAccess with no
// statements or with allows on "*", and every later sync would inherit it. A policy whose
// statements differ is logged as a warning and replaced; the reconciler runs on every startup, so a
// hand-edited policy is repaired rather than trusted.
func ensurePolicy(ctx context.Context, authService auth.Service, policy *model.Policy) (bool, error) {
	existing, err := authService.GetPolicy(ctx, policy.DisplayName)
	switch {
	case err == nil:
		if statementsEqual(existing.Statement, policy.Statement) {
			return true, nil
		}
		if err := authService.WritePolicy(ctx, policy, true /* update */); err != nil {
			if policyBackendUnavailable(err) {
				logSyncIdentityNotCreated(ctx, err)
				return false, nil
			}
			return false, fmt.Errorf("mds bootstrap: rewrite policy %s: %w", policy.DisplayName, err)
		}
		purgePolicyCache(authService)
		logging.FromContext(ctx).WithField("policy", policy.DisplayName).
			Warn("mds bootstrap: rewrote a metadata search policy whose statements did not match")
		return true, nil
	case errors.Is(err, auth.ErrNotFound):
		if err := authService.WritePolicy(ctx, policy, false /* create */); err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			if policyBackendUnavailable(err) {
				logSyncIdentityNotCreated(ctx, err)
				return false, nil
			}
			return false, fmt.Errorf("mds bootstrap: create policy %s: %w", policy.DisplayName, err)
		}
		purgePolicyCache(authService)
		return true, nil
	case policyBackendUnavailable(err):
		logSyncIdentityNotCreated(ctx, err)
		return false, nil
	default:
		return false, fmt.Errorf("mds bootstrap: read policy %s: %w", policy.DisplayName, err)
	}
}

// purgePolicyCache clears the auth service's per-user effective-policy cache after a policy
// changes, when the service has one. A service without a cache keeps working because a nil Cache
// is a no-op.
func purgePolicyCache(authService auth.Service) {
	policyCache := authService.Cache()
	if policyCache == nil {
		return
	}
	policyCache.PurgePolicies()
}

// policyBackendUnavailable reports whether err is the auth service saying it has no policy backend
// at all, rather than that an operation on one that exists failed. auth.BasicAuthService - the
// default auth mode of lakefs and lakefs-sso - is the case this exists for: it answers every policy
// call with auth.ErrNotImplemented (pkg/auth/basic_service.go). errors.Is, not ==, because a
// service is free to wrap the sentinel.
//
// The distinction is what keeps startup honest: a backend that cannot store policies degrades the
// sync identity (a warning), while a backend that can and failed - a KV error, an external auth
// service answering 5xx - must still stop the server.
func policyBackendUnavailable(err error) bool {
	return errors.Is(err, auth.ErrNotImplemented)
}

// logSyncIdentityNotCreated reports the one degraded startup EnsureSyncIdentity accepts: the auth
// service cannot manage policies, so mds-service-user, MDSCatalogAccess and every legacy removal
// were skipped. The message names the consequence rather than the error alone, because an operator
// who reads only this line has to learn that the sync cannot be authorized in this deployment.
func logSyncIdentityNotCreated(ctx context.Context, err error) {
	logging.FromContext(ctx).WithError(err).WithFields(logging.Fields{
		"user":   UserName,
		"policy": PolicyName,
	}).Warn("mds bootstrap: the auth service cannot manage policies: the " + UserName + " user and the " +
		PolicyName + " policy are NOT created, so metadata search indexing is not authorized in this deployment")
}

// statementsEqual compares the statements a stored policy carries with the built-in ones. It
// ignores an empty Conditions map versus nil, so a policy that merely round-tripped through the KV
// encoding is not rewritten on every startup.
func statementsEqual(a, b model.Statements) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Effect != b[i].Effect ||
			!slices.Equal(a[i].Action, b[i].Action) ||
			a[i].Resource != b[i].Resource ||
			!conditionsEqual(a[i].Condition, b[i].Condition) {
			return false
		}
	}
	return true
}

// conditionsEqual compares two statement conditions, treating an empty map and a nil map as equal.
func conditionsEqual(a, b map[string]map[string][]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}
