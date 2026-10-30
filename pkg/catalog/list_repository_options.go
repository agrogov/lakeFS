package catalog

import (
	"context"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// ListRepositoriesOptions controls list repositories request options
type ListRepositoriesOptions struct {
	// FilterFunc is an optional predicate that filters repositories.
	// Returns true if the repository should be included in the results.
	FilterFunc func(repoID string) bool
}

// ListRepositoriesOptionsFunc is a function that modifies ListRepositoriesOptions
type ListRepositoriesOptionsFunc func(opts *ListRepositoriesOptions)

// NewListRepositoriesOptions creates a new ListRepositoriesOptions from the given functions
func NewListRepositoriesOptions(opts []ListRepositoriesOptionsFunc) *ListRepositoriesOptions {
	options := &ListRepositoriesOptions{}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// WithListReposPermissionFilter creates a filter predicate based on user policies.
// It filters repositories where the user has fs:ListRepositories permission, evaluating policy
// conditions with the condition context that conditionContext builds for each repository: the
// client IP and, lazily, that repository's lakefs:RepositoryMetadata/<key> fields. Evaluation is
// deny-first - CheckPermissions reports CheckDeny for any matching deny statement, conditional or
// not, and only CheckAllow makes the repository visible - so a conditional deny hides the
// repository and a conditional allow grants it.
//
// conditionContext must return a context carrying an auth.ConditionContext: catalog has no
// *http.Request, so the caller (the API handler) supplies it.
func WithListReposPermissionFilter(conditionContext func(repoID string) context.Context, username string, policies []*model.Policy) ListRepositoriesOptionsFunc {
	return func(opts *ListRepositoriesOptions) {
		if len(policies) == 0 {
			// No policies means no access to any specific repository
			opts.FilterFunc = func(repoID string) bool {
				return false
			}
			return
		}
		opts.FilterFunc = func(repoID string) bool {
			return auth.CheckPermissions(conditionContext(repoID), permissions.Node{
				Permission: permissions.Permission{
					Action:   permissions.ListRepositoriesAction,
					Resource: permissions.RepoArn(repoID),
				},
			}, username, policies, &auth.MissingPermissions{}) == auth.CheckAllow
		}
	}
}
