package catalog_test

import (
	"context"
	"slices"
	"sort"
	"testing"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/permissions"
)

func TestWithListReposPermissionFilter(t *testing.T) {
	t.Parallel()

	const (
		allowedIP = "203.0.113.5"
		otherIP   = "198.51.100.7"
	)
	ipCondition := func(ip string) map[string]map[string][]string {
		return map[string]map[string][]string{
			auth.OperatorNameIpAddress: {"SourceIp": {ip + "/32"}},
		}
	}
	repoMetadataCondition := func(value string) map[string]map[string][]string {
		return map[string]map[string][]string{
			auth.OperatorNameStringEquals: {auth.RepositoryMetadataConditionKeyPrefix + "team": {value}},
		}
	}

	// conditionContext builds the condition context the API handler supplies per repository: the
	// client IP, plus lazily the repository metadata of the repository being evaluated.
	conditionContext := func(ip string, metadata map[string]map[string]string) func(repoID string) context.Context {
		return func(repoID string) context.Context {
			fields := auth.NewRepositoryMetadataFields(func() (map[string]string, error) {
				return metadata[repoID], nil
			})
			return auth.WithConditionContext(context.Background(), auth.NewConditionContextWithProvider(ip, fields))
		}
	}
	allowStatement := func(resource string, condition map[string]map[string][]string) model.Statements {
		return model.Statements{{
			Effect:    model.StatementEffectAllow,
			Action:    []string{permissions.ListRepositoriesAction},
			Resource:  resource,
			Condition: condition,
		}}
	}

	tests := []struct {
		name     string
		username string
		policies []*model.Policy
		clientIP string
		metadata map[string]map[string]string
		repoIDs  []string
		want     []string
	}{
		{
			name:     "no policies filters all",
			username: "user1",
			policies: nil,
			repoIDs:  []string{"repo1", "repo2", "repo3"},
			want:     []string{},
		},
		{
			name:     "wildcard allows all",
			username: "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:*"},
					Resource: "*",
				}},
			}},
			repoIDs: []string{"repo1", "repo2", "repo3"},
			want:    []string{"repo1", "repo2", "repo3"},
		},
		{
			name:     "pattern filters correctly",
			username: "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{{
					Effect:   model.StatementEffectAllow,
					Action:   []string{"fs:ListRepositories"},
					Resource: "arn:lakefs:fs:::repository/analytics-*",
				}},
			}},
			repoIDs: []string{"analytics-prod", "analytics-dev", "other-repo"},
			want:    []string{"analytics-prod", "analytics-dev"},
		},
		{
			name:     "conditional allow from matching IP is honoured",
			username: "user1",
			policies: []*model.Policy{{
				Statement: allowStatement(permissions.RepoArn("repo1"), ipCondition(allowedIP)),
			}},
			clientIP: allowedIP,
			repoIDs:  []string{"repo1", "repo2"},
			want:     []string{"repo1"},
		},
		{
			name:     "conditional allow from other IP is lost",
			username: "user1",
			policies: []*model.Policy{{
				Statement: allowStatement(permissions.RepoArn("repo1"), ipCondition(allowedIP)),
			}},
			clientIP: otherIP,
			repoIDs:  []string{"repo1", "repo2"},
			want:     []string{},
		},
		{
			name:     "conditional deny from matching IP is enforced deny-first",
			username: "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Effect:   model.StatementEffectAllow,
						Action:   []string{permissions.ListRepositoriesAction},
						Resource: permissions.RepoArn("*"),
					},
					{
						Effect:    model.StatementEffectDeny,
						Action:    []string{permissions.ListRepositoriesAction},
						Resource:  permissions.RepoArn("repo1"),
						Condition: ipCondition(allowedIP),
					},
				},
			}},
			clientIP: allowedIP,
			repoIDs:  []string{"repo1", "repo2"},
			want:     []string{"repo2"},
		},
		{
			name:     "conditional deny from other IP does not apply",
			username: "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Effect:   model.StatementEffectAllow,
						Action:   []string{permissions.ListRepositoriesAction},
						Resource: permissions.RepoArn("*"),
					},
					{
						Effect:    model.StatementEffectDeny,
						Action:    []string{permissions.ListRepositoriesAction},
						Resource:  permissions.RepoArn("repo1"),
						Condition: ipCondition(allowedIP),
					},
				},
			}},
			clientIP: otherIP,
			repoIDs:  []string{"repo1", "repo2"},
			want:     []string{"repo1", "repo2"},
		},
		{
			name:     "repository metadata conditioned allow with matching metadata is honoured",
			username: "user1",
			policies: []*model.Policy{{
				Statement: allowStatement(permissions.RepoArn("repo1"), repoMetadataCondition("data")),
			}},
			clientIP: otherIP,
			metadata: map[string]map[string]string{"repo1": {"team": "data"}},
			repoIDs:  []string{"repo1", "repo2"},
			want:     []string{"repo1"},
		},
		{
			name:     "repository metadata conditioned deny with matching metadata is enforced",
			username: "user1",
			policies: []*model.Policy{{
				Statement: model.Statements{
					{
						Effect:   model.StatementEffectAllow,
						Action:   []string{permissions.ListRepositoriesAction},
						Resource: permissions.RepoArn("*"),
					},
					{
						Effect:    model.StatementEffectDeny,
						Action:    []string{permissions.ListRepositoriesAction},
						Resource:  permissions.RepoArn("repo1"),
						Condition: repoMetadataCondition("data"),
					},
				},
			}},
			clientIP: otherIP,
			metadata: map[string]map[string]string{"repo1": {"team": "data"}},
			repoIDs:  []string{"repo1", "repo2"},
			want:     []string{"repo2"},
		},
		{
			name:     "repository metadata conditioned grant with other metadata is lost",
			username: "user1",
			policies: []*model.Policy{{
				Statement: allowStatement(permissions.RepoArn("repo1"), repoMetadataCondition("data")),
			}},
			clientIP: otherIP,
			metadata: map[string]map[string]string{"repo1": {"team": "other"}},
			repoIDs:  []string{"repo1", "repo2"},
			want:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := catalog.NewListRepositoriesOptions([]catalog.ListRepositoriesOptionsFunc{
				catalog.WithListReposPermissionFilter(conditionContext(tt.clientIP, tt.metadata), tt.username, tt.policies),
			})

			var visible []string
			for _, repoID := range tt.repoIDs {
				if opts.FilterFunc(repoID) {
					visible = append(visible, repoID)
				}
			}

			// ListRepositories does not guarantee an order, so compare sets: the same repositories
			// must be visible, in any order.
			sort.Strings(visible)
			want := append([]string(nil), tt.want...)
			sort.Strings(want)
			if !slices.Equal(visible, want) {
				t.Errorf("visible repos = %v, want %v", visible, want)
			}
		})
	}
}
