package sso

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	logging "github.com/treeverse/lakefs/pkg/logging"
)

const maxGroupsPerUser = 1000

// groupManager is the subset of auth.Service used by SyncGroups.
// Using a narrow interface keeps SyncGroups testable without a full auth.Service mock.
type groupManager interface {
	ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)
	AddUserToGroup(ctx context.Context, username, groupID string) error
	RemoveUserFromGroup(ctx context.Context, username, groupID string) error
}

// SyncGroups reconciles the user's ACL group memberships against tokenGroups (raw values
// of the cfg.GroupsClaim from the ID token). Token values are translated through
// cfg.GroupMapping when set, and only its target groups are touched. Otherwise only groups
// whose name starts with cfg.ManagedGroupPrefix are touched (all when it is empty).
// Groups outside the managed scope are preserved as manually assigned.
//
// A nil tokenGroups slice means the token contained the claim but it was empty — all
// managed memberships are removed. When the claim is absent from the token entirely,
// the caller (OauthCallback) skips calling SyncGroups to avoid silently stripping access.
//
// Errors adding/removing individual groups are logged and skipped rather than aborting login.
func SyncGroups(ctx context.Context, svc groupManager, logger logging.Logger, username string, tokenGroups []string, cfg *SSOConfig) error {
	if !cfg.SyncGroupsOnLogin || cfg.GroupsClaim == "" {
		return nil
	}

	desired, managed := desiredGroups(tokenGroups, cfg)

	// Fetch the user's current groups (one page; lakeFS ACL has 4 built-in groups so
	// this is sufficient in practice, but log if the result is truncated).
	current, paginator, err := svc.ListUserGroups(ctx, username, &model.PaginationParams{Amount: maxGroupsPerUser})
	if err != nil {
		return fmt.Errorf("group sync: list groups for %q: %w", username, err)
	}
	if paginator != nil && paginator.NextPageToken != "" {
		logger.WithFields(logging.Fields{"user": username, "page_size": maxGroupsPerUser}).
			Error("group sync: user belongs to more groups than the page limit — sync may be incomplete")
	}

	// Separate current groups into managed (subject to sync) and unmanaged (preserved).
	currentManaged := make(map[string]bool, len(current))
	for _, g := range current {
		if managed(g.DisplayName) {
			currentManaged[g.DisplayName] = true
		}
	}

	// Add to groups in desired that the user does not yet belong to.
	for gname := range desired {
		if currentManaged[gname] {
			continue
		}
		if addErr := svc.AddUserToGroup(ctx, username, gname); addErr != nil {
			switch {
			case errors.Is(addErr, auth.ErrAlreadyExists):
				// Concurrent login or admin action added the user between list and add — idempotent.
			case errors.Is(addErr, auth.ErrNotFound):
				logger.WithFields(logging.Fields{"user": username, "group": gname}).
					Warn("group sync: group not found, skipping add")
			default:
				logger.WithFields(logging.Fields{"user": username, "group": gname}).
					WithError(addErr).Warn("group sync: add failed, skipping")
			}
		}
	}

	// Remove from managed groups that the token no longer lists.
	for gname := range currentManaged {
		if desired[gname] {
			continue
		}
		if rmErr := svc.RemoveUserFromGroup(ctx, username, gname); rmErr != nil {
			if errors.Is(rmErr, auth.ErrNotFound) {
				continue // already removed
			}
			logger.WithFields(logging.Fields{"user": username, "group": gname}).
				WithError(rmErr).Warn("group sync: remove failed, skipping")
		}
	}

	return nil
}

// desiredGroups returns the lakeFS groups the token grants and a predicate for the groups
// sync may change.
//
// With cfg.GroupMapping set, token values (e.g. Entra group object IDs or app role values)
// are translated to lakeFS groups, compared case-insensitively since config keys are
// lowercased; only groups that appear as mapping targets are managed. Otherwise token
// values are lakeFS group names, scoped by cfg.ManagedGroupPrefix.
func desiredGroups(tokenGroups []string, cfg *SSOConfig) (map[string]bool, func(string) bool) {
	desired := make(map[string]bool, len(tokenGroups))
	if len(cfg.GroupMapping) > 0 {
		mapping := make(map[string][]string, len(cfg.GroupMapping))
		targets := make(map[string]bool)
		for k, groups := range cfg.GroupMapping {
			mapping[strings.ToLower(k)] = groups
			for _, g := range groups {
				targets[g] = true
			}
		}
		for _, tg := range tokenGroups {
			for _, g := range mapping[strings.ToLower(tg)] {
				desired[g] = true
			}
		}
		return desired, func(g string) bool { return targets[g] }
	}

	inScope := func(g string) bool {
		return cfg.ManagedGroupPrefix == "" || strings.HasPrefix(g, cfg.ManagedGroupPrefix)
	}
	// Empty strings are filtered here rather than relying solely on ExtractStringSlice
	// so that SyncGroups is safe when called directly with caller-supplied slices.
	for _, tg := range tokenGroups {
		if tg != "" && inScope(tg) {
			desired[tg] = true
		}
	}
	return desired, inScope
}

// ExtractStringSlice coerces a claims value to []string.
// Handles: nil, string, []string, []interface{} (JSON array).
func ExtractStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case string:
		if val == "" {
			return nil
		}
		return []string{val}
	case []string:
		return val
	case []interface{}:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
