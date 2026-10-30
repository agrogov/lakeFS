package sso

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/setup"
)

// rbacMigrateAdminPolicies is the full RBAC policy set for the Admins group. The first
// four are also attached by EnsureRBACBaseGroups; listing them here makes the command
// idempotent and makes the intent explicit.
var rbacMigrateAdminPolicies = []string{
	"FSFullAccess",
	"CatalogReadWriteAll",
	"AuthFullAccess",
	"RepoManagementFullAccess",
	"PRReadWriteAll",
}

// rbacLegacyGroupTargets maps the simplified ACL groups to their RBAC equivalents.
// Admins is handled separately because its name does not change and its ACL policy has
// to be detached after the RBAC policies are attached.
var rbacLegacyGroupTargets = []struct {
	legacy string
	target string
}{
	{legacy: acl.SupersGroup, target: setup.SuperUsersGroup},
	{legacy: acl.WritersGroup, target: setup.DevelopersGroup},
	{legacy: acl.ReadersGroup, target: setup.ViewersGroup},
}

// RBACMigrationOptions controls whether a migration is applied and whether the legacy
// groups and their ACL policies are removed afterward.
type RBACMigrationOptions struct {
	Apply   bool
	Cleanup bool
}

// RBACMemberCopy describes users to copy from a legacy ACL group to an RBAC group.
type RBACMemberCopy struct {
	From  string
	To    string
	Users []string
}

// RBACGroupMappingRewrite describes one sso.group_mapping value rewrite.
type RBACGroupMappingRewrite struct {
	From string
	To   string
}

// RBACMigrationPlan is the set of changes needed to move a simplified ACL installation
// to policy-based RBAC. An empty plan means the migration has already been applied.
type RBACMigrationPlan struct {
	AttachAdminPolicies  []string
	DetachAdminPolicies  []string
	MemberCopies         []RBACMemberCopy
	DeleteGroups         []string
	DeletePolicies       []string
	KeptPolicies         []RBACKeptPolicy
	GroupMappingRewrites []RBACGroupMappingRewrite
}

// RBACKeptPolicy is a legacy policy that cleanup leaves in place because it is still attached.
type RBACKeptPolicy struct {
	Policy     string
	AttachedTo []string
}

// Empty reports whether applying the plan would change any authorization state.
// GroupMappingRewrites and KeptPolicies are operator guidance, not changes.
func (p *RBACMigrationPlan) Empty() bool {
	return len(p.AttachAdminPolicies) == 0 &&
		len(p.DetachAdminPolicies) == 0 &&
		len(p.MemberCopies) == 0 &&
		len(p.DeleteGroups) == 0 &&
		len(p.DeletePolicies) == 0
}

// PlanRBACMigration inspects the current ACL state and returns the changes required for
// the migration. It does not modify anything.
func PlanRBACMigration(ctx context.Context, svc auth.Service, cleanup bool) (*RBACMigrationPlan, error) {
	plan := &RBACMigrationPlan{}
	// These rewrites are required even after cleanup: sso.group_mapping may still
	// target the removed legacy groups, and the next login would then re-add users to
	// groups that no longer exist.
	for _, mapping := range rbacLegacyGroupTargets {
		plan.GroupMappingRewrites = append(plan.GroupMappingRewrites, RBACGroupMappingRewrite{
			From: mapping.legacy,
			To:   mapping.target,
		})
	}

	adminPolicies, err := listAllGroupPolicies(ctx, svc, acl.AdminsGroup)
	if err != nil && !errors.Is(err, auth.ErrNotFound) {
		return nil, fmt.Errorf("list policies of group %s: %w", acl.AdminsGroup, err)
	}
	adminAttached := make(map[string]bool, len(adminPolicies))
	for _, policy := range adminPolicies {
		adminAttached[policy.DisplayName] = true
	}
	for _, policy := range rbacMigrateAdminPolicies {
		if !adminAttached[policy] {
			plan.AttachAdminPolicies = append(plan.AttachAdminPolicies, policy)
		}
	}
	adminACLPolicy := acl.PolicyName(acl.AdminsGroup)
	if adminAttached[adminACLPolicy] {
		plan.DetachAdminPolicies = append(plan.DetachAdminPolicies, adminACLPolicy)
	}

	for _, mapping := range rbacLegacyGroupTargets {
		users, groupExists, err := legacyGroupUsers(ctx, svc, mapping.legacy)
		if err != nil {
			return nil, err
		}
		if groupExists {
			targetUsers, err := listAllGroupUsers(ctx, svc, mapping.target)
			if err != nil && !errors.Is(err, auth.ErrNotFound) {
				return nil, fmt.Errorf("list members of group %s: %w", mapping.target, err)
			}
			targetMembers := make(map[string]bool, len(targetUsers))
			for _, user := range targetUsers {
				targetMembers[user.Username] = true
			}
			toCopy := make([]string, 0, len(users))
			for _, user := range users {
				if !targetMembers[user.Username] {
					toCopy = append(toCopy, user.Username)
				}
			}
			slices.Sort(toCopy)
			if len(toCopy) > 0 {
				plan.MemberCopies = append(plan.MemberCopies, RBACMemberCopy{
					From:  mapping.legacy,
					To:    mapping.target,
					Users: toCopy,
				})
			}
		}

		if !cleanup {
			continue
		}
		// The members were copied to the RBAC group above. DeleteGroup also removes
		// the remaining legacy memberships, so the group is empty before it goes away.
		if groupExists {
			plan.DeleteGroups = append(plan.DeleteGroups, mapping.legacy)
		}
		// Delete the legacy ACL policy when the group is gone or about to be deleted.
		policyName := acl.PolicyName(mapping.legacy)
		exists, err := policyExists(ctx, svc, policyName)
		if err != nil {
			return nil, err
		}
		if exists {
			plan.DeletePolicies = append(plan.DeletePolicies, policyName)
		}
	}

	// ACL(_-_)Admins is detached from Admins rather than deleted with a group, so cleanup
	// deletes it only once nothing else holds it: an admin may have attached it elsewhere.
	if cleanup {
		if err := planAdminACLPolicyCleanup(ctx, svc, plan, adminACLPolicy); err != nil {
			return nil, err
		}
	}

	return plan, nil
}

// ApplyRBACMigration applies a migration plan. It first ensures the RBAC base groups,
// policies and baseline attachments exist, without overwriting any policy an admin has
// edited. Every operation is tolerant of already having been done, so re-running the
// command is a no-op.
func ApplyRBACMigration(ctx context.Context, svc auth.Service, plan *RBACMigrationPlan) error {
	if err := setup.EnsureRBACBaseGroups(ctx, svc, time.Now()); err != nil {
		return fmt.Errorf("ensure RBAC base groups: %w", err)
	}

	for _, policy := range plan.AttachAdminPolicies {
		err := svc.AttachPolicyToGroup(ctx, policy, setup.AdminsGroup)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return fmt.Errorf("attach policy %s to group %s: %w", policy, setup.AdminsGroup, err)
		}
	}
	for _, policy := range plan.DetachAdminPolicies {
		err := svc.DetachPolicyFromGroup(ctx, policy, setup.AdminsGroup)
		if err != nil && !errors.Is(err, auth.ErrNotFound) {
			return fmt.Errorf("detach policy %s from group %s: %w", policy, setup.AdminsGroup, err)
		}
	}

	for _, copy := range plan.MemberCopies {
		for _, username := range copy.Users {
			err := svc.AddUserToGroup(ctx, username, copy.To)
			if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
				return fmt.Errorf("copy user %s from group %s to %s: %w", username, copy.From, copy.To, err)
			}
		}
	}

	for _, group := range plan.DeleteGroups {
		err := svc.DeleteGroup(ctx, group)
		if err != nil && !errors.Is(err, auth.ErrNotFound) {
			return fmt.Errorf("delete legacy group %s: %w", group, err)
		}
	}
	for _, policy := range plan.DeletePolicies {
		err := svc.DeletePolicy(ctx, policy)
		if err != nil && !errors.Is(err, auth.ErrNotFound) {
			return fmt.Errorf("delete legacy policy %s: %w", policy, err)
		}
	}

	return nil
}

// MigrateACLToRBAC plans and, when opts.Apply is set, applies the simplified -> internal
// RBAC migration. It writes the plan to out. The default is a dry run.
func MigrateACLToRBAC(ctx context.Context, svc auth.Service, opts RBACMigrationOptions, out io.Writer) error {
	plan, err := PlanRBACMigration(ctx, svc, opts.Cleanup)
	if err != nil {
		return err
	}
	if !opts.Apply {
		writeRBACMigrationPlan(out, plan, false)
		return nil
	}
	if err := ApplyRBACMigration(ctx, svc, plan); err != nil {
		return err
	}
	writeRBACMigrationPlan(out, plan, true)
	return nil
}

func legacyGroupUsers(ctx context.Context, svc auth.Service, group string) ([]*model.User, bool, error) {
	users, err := listAllGroupUsers(ctx, svc, group)
	if err == nil {
		return users, true, nil
	}
	if errors.Is(err, auth.ErrNotFound) {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("list members of group %s: %w", group, err)
}

func listAllGroupUsers(ctx context.Context, svc auth.Service, group string) ([]*model.User, error) {
	return listAll(func(params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
		return svc.ListGroupUsers(ctx, group, params)
	})
}

func listAllGroupPolicies(ctx context.Context, svc auth.Service, group string) ([]*model.Policy, error) {
	return listAll(func(params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
		return svc.ListGroupPolicies(ctx, group, params)
	})
}

// planAdminACLPolicyCleanup adds the Admins ACL policy to plan.DeletePolicies when no group
// or user holds it once the planned detach from Admins is applied. Otherwise it records the
// holders in plan.KeptPolicies.
func planAdminACLPolicyCleanup(ctx context.Context, svc auth.Service, plan *RBACMigrationPlan, policy string) error {
	exists, err := policyExists(ctx, svc, policy)
	if err != nil || !exists {
		return err
	}
	holders, err := policyHolders(ctx, svc, policy)
	if err != nil {
		return err
	}
	detaching := slices.Contains(plan.DetachAdminPolicies, policy)
	holders = slices.DeleteFunc(holders, func(holder string) bool {
		return detaching && holder == "group "+acl.AdminsGroup
	})
	if len(holders) > 0 {
		plan.KeptPolicies = append(plan.KeptPolicies, RBACKeptPolicy{Policy: policy, AttachedTo: holders})
		return nil
	}
	plan.DeletePolicies = append(plan.DeletePolicies, policy)
	return nil
}

// policyHolders returns every group and user the policy is attached to, as "group <name>"
// and "user <name>".
func policyHolders(ctx context.Context, svc auth.Service, policy string) ([]string, error) {
	var holders []string
	groups, err := listAll(func(params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
		return svc.ListGroups(ctx, params)
	})
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	for _, group := range groups {
		policies, err := listAllGroupPolicies(ctx, svc, group.DisplayName)
		if err != nil {
			return nil, fmt.Errorf("list policies of group %s: %w", group.DisplayName, err)
		}
		if slices.ContainsFunc(policies, func(p *model.Policy) bool { return p.DisplayName == policy }) {
			holders = append(holders, "group "+group.DisplayName)
		}
	}
	users, err := listAll(func(params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
		return svc.ListUsers(ctx, params)
	})
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	for _, user := range users {
		policies, err := listAll(func(params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
			return svc.ListUserPolicies(ctx, user.Username, params)
		})
		if err != nil {
			return nil, fmt.Errorf("list policies of user %s: %w", user.Username, err)
		}
		if slices.ContainsFunc(policies, func(p *model.Policy) bool { return p.DisplayName == policy }) {
			holders = append(holders, "user "+user.Username)
		}
	}
	return holders, nil
}

func listAll[T any](list func(*model.PaginationParams) ([]T, *model.Paginator, error)) ([]T, error) {
	var all []T
	after := ""
	for {
		items, paginator, err := list(&model.PaginationParams{Amount: auth.MaxPage, After: after})
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
		if paginator == nil || paginator.NextPageToken == "" {
			return all, nil
		}
		after = paginator.NextPageToken
	}
}

func policyExists(ctx context.Context, svc auth.Service, policy string) (bool, error) {
	_, err := svc.GetPolicy(ctx, policy)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, auth.ErrNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("get policy %s: %w", policy, err)
}

func writeRBACMigrationPlan(out io.Writer, plan *RBACMigrationPlan, applied bool) {
	if applied {
		fmt.Fprintln(out, "RBAC migration applied:")
	} else {
		fmt.Fprintln(out, "RBAC migration plan (dry run - no changes made; pass --yes to apply):")
	}
	if plan.Empty() {
		fmt.Fprintln(out, "  no authorization changes")
	}
	for _, policy := range plan.AttachAdminPolicies {
		fmt.Fprintf(out, "  attach policy %s to group %s\n", policy, setup.AdminsGroup)
	}
	for _, policy := range plan.DetachAdminPolicies {
		fmt.Fprintf(out, "  detach legacy policy %s from group %s\n", policy, setup.AdminsGroup)
	}
	for _, copy := range plan.MemberCopies {
		fmt.Fprintf(out, "  copy %d member(s) from %s to %s: %v\n", len(copy.Users), copy.From, copy.To, copy.Users)
	}
	for _, group := range plan.DeleteGroups {
		fmt.Fprintf(out, "  (--cleanup) delete legacy group %s (after copying its members)\n", group)
	}
	for _, policy := range plan.DeletePolicies {
		fmt.Fprintf(out, "  (--cleanup) delete legacy policy %s\n", policy)
	}
	for _, kept := range plan.KeptPolicies {
		fmt.Fprintf(out, "  (--cleanup) keep legacy policy %s: still attached to %s\n", kept.Policy, strings.Join(kept.AttachedTo, ", "))
	}
	fmt.Fprintln(out, "  custom groups are left untouched; their ACL policies are kept")

	if len(plan.GroupMappingRewrites) > 0 {
		fmt.Fprintln(out, "Required sso.group_mapping rewrites (otherwise the next login re-adds users to the legacy groups):")
		for _, rewrite := range plan.GroupMappingRewrites {
			fmt.Fprintf(out, "  %s -> %s\n", rewrite.From, rewrite.To)
		}
	}
	fmt.Fprintln(out, "WARNING: rolling back to `auth.ui_config.rbac: simplified` is not lossless: a group that holds more than one policy")
	fmt.Fprintln(out, "         shows no ACL in the simplified UI (RBAC_DESIGN.md section 3.3 #5).")
	if !applied {
		fmt.Fprintln(out, "         Without --cleanup the legacy groups and ACL policies are kept, so rollback stays lossless.")
	}
}
