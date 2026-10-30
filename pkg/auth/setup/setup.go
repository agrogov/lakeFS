package setup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	AdminsGroup     = "Admins"
	SuperUsersGroup = "SuperUsers"
	DevelopersGroup = "Developers"
	ViewersGroup    = "Viewers"
)

// Built-in datasets policies (DATASETS_DESIGN.md §9.5).
const (
	DatasetsFullAccessPolicy = "DatasetsFullAccess"
	DatasetsReadAllPolicy    = "DatasetsReadAll"
)

// defaultDatasetsEnabled is the production default of the datasets feature switch: off.
//
// The datasets feature is not part of this binary yet, so nothing can turn it on: the
// production bootstrap (pkg/sso.EnsureRBACBase) calls the bootstrap without options and
// therefore gets the off behaviour. When the feature lands, the flag has to be plumbed from
// configuration into EnsureRBACBaseGroups, and that wiring has to be coordinated with
// RBACBootstrapVersion in pkg/sso/rbac_bootstrap.go: the version guard skips the bootstrap
// once a version has been recorded, so a later switch from off to on must either bump the
// version or bypass the guard, otherwise the datasets policies are never created.
const defaultDatasetsEnabled = false

// BaseGroupsOption configures optional parts of the RBAC bootstrap.
type BaseGroupsOption func(*baseGroupsConfig)

type baseGroupsConfig struct {
	// datasetsEnabled creates the datasets built-in policies and attaches them to the base groups.
	datasetsEnabled bool
}

// WithDatasetsEnabled turns the datasets built-in policy set on (RBAC_DESIGN.md §5.4): the
// policies are created *and* attached to the base groups. With it off - the production
// default - CreateRBACBasePolicies does not create the datasets policies at all, so an
// installation that does not use datasets never gets unattached policies in the Policies UI.
func WithDatasetsEnabled(enabled bool) BaseGroupsOption {
	return func(c *baseGroupsConfig) {
		c.datasetsEnabled = enabled
	}
}

func newBaseGroupsConfig(opts ...BaseGroupsOption) *baseGroupsConfig {
	cfg := &baseGroupsConfig{
		// Policy creation and attachment are gated by the same flag, so the bootstrap can
		// never leave an orphan datasets policy behind. The default is stated explicitly
		// rather than left to the zero value.
		datasetsEnabled: defaultDatasetsEnabled,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func createGroups(ctx context.Context, authService auth.Service, groups []*model.Group) error {
	for _, group := range groups {
		_, err := authService.CreateGroup(ctx, group)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

func createPolicies(ctx context.Context, authService auth.Service, policies []*model.Policy) error {
	for _, policy := range policies {
		err := authService.WritePolicy(ctx, policy, false)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

// CreateRBACBasePolicies creates the built-in RBAC policies. The datasets policies are part of
// the template only when WithDatasetsEnabled(true) is passed, mirroring the group attachments in
// EnsureRBACBaseGroups: an installation either gets the datasets policies attached or not at all.
func CreateRBACBasePolicies(ctx context.Context, authService auth.Service, ts time.Time, opts ...BaseGroupsOption) error {
	cfg := newBaseGroupsConfig(opts...)
	all := []string{permissions.All}

	policies := []*model.Policy{
		{
			CreatedAt:   ts,
			DisplayName: "FSFullAccess",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("FSFullAccess", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadWriteAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("FSReadWrite", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "FSReadAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("FSRead", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "RepoManagementFullAccess",
			Statement: model.Statements{
				{
					Action: []string{
						"ci:*",
						"retention:*",
						"branches:*",
						"pr:*",
						"fs:ReadConfig",
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			CreatedAt:   ts,
			DisplayName: "PRReadWriteAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("PRReadWrite", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "CatalogReadAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("CatalogRead", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "CatalogReadWriteAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("CatalogReadWrite", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "RepoManagementReadAll",
			Statement:   auth.MakeStatementForPolicyTypeOrDie("RepoManagementRead", all),
		},
		{
			CreatedAt:   ts,
			DisplayName: "AuthFullAccess",
			Statement: model.Statements{
				{
					Action: []string{
						"auth:*",
					},
					Resource: permissions.All,
					Effect:   model.StatementEffectAllow,
				},
			},
		},
		{
			CreatedAt:   ts,
			DisplayName: "AuthManageOwnCredentials",
			Statement: auth.MakeStatementForPolicyTypeOrDie(
				"AuthManageOwnCredentials",
				[]string{permissions.UserArn("${user}")},
			),
		},
	}

	if cfg.datasetsEnabled {
		// Datasets control-plane policies (DATASETS_DESIGN.md §9.5). Created only when the datasets feature is
		// enabled, together with their attachments in EnsureRBACBaseGroups.
		policies = append(policies,
			&model.Policy{
				CreatedAt:   ts,
				DisplayName: DatasetsFullAccessPolicy,
				Statement:   auth.MakeStatementForPolicyTypeOrDie("DatasetsFull", all),
			},
			&model.Policy{
				CreatedAt:   ts,
				DisplayName: DatasetsReadAllPolicy,
				Statement:   auth.MakeStatementForPolicyTypeOrDie("DatasetsRead", all),
			},
		)
	}

	return createPolicies(ctx, authService, policies)
}

func attachPolicies(ctx context.Context, authService auth.Service, groupID string, policyIDs []string) error {
	for _, policyID := range policyIDs {
		err := authService.AttachPolicyToGroup(ctx, policyID, groupID)
		if err != nil && !errors.Is(err, auth.ErrAlreadyExists) {
			return err
		}
	}
	return nil
}

// EnsureRBACBaseGroups creates the groups, built-in policies and attachments of the RBAC base
// template. It is idempotent: existing groups, policies and attachments are left unchanged.
//
// WithDatasetsEnabled(true) adds the datasets built-in policies to both the policy set and the
// group attachments; with the default (off) no datasets policy is created or attached at all.
func EnsureRBACBaseGroups(ctx context.Context, authService auth.Service, ts time.Time, opts ...BaseGroupsOption) error {
	cfg := newBaseGroupsConfig(opts...)

	err := createGroups(ctx, authService, []*model.Group{
		{CreatedAt: ts, DisplayName: AdminsGroup},
		{CreatedAt: ts, DisplayName: SuperUsersGroup},
		{CreatedAt: ts, DisplayName: DevelopersGroup},
		{CreatedAt: ts, DisplayName: ViewersGroup},
	})
	if err != nil {
		return err
	}

	err = CreateRBACBasePolicies(ctx, authService, ts, opts...)
	if err != nil {
		return err
	}

	attachments := []struct {
		group    string
		policies []string
	}{
		{AdminsGroup, []string{"FSFullAccess", "CatalogReadWriteAll", "AuthFullAccess", "RepoManagementFullAccess"}},
		// SuperUsers gets PRReadWriteAll because ACL Super (the template Supers is migrated from) grants
		// PRReadWrite: without it, `rbac-migrate` downgraded every existing Super by taking away the ability
		// to open, update or merge a pull request. FSFullAccess/RepoManagementReadAll only grant pr:Read* and
		// pr:List*. See RBAC_DESIGN.md §4.4, §4.5 #1 and DESIGN_CONFORMANCE.md GAP-06.
		{SuperUsersGroup, []string{"FSFullAccess", "PRReadWriteAll", "CatalogReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll"}},
		{DevelopersGroup, []string{"FSReadWriteAll", "PRReadWriteAll", "CatalogReadWriteAll", "AuthManageOwnCredentials", "RepoManagementReadAll"}},
		{ViewersGroup, []string{"FSReadAll", "CatalogReadAll", "AuthManageOwnCredentials"}},
	}

	if cfg.datasetsEnabled {
		// Datasets control-plane access: admins manage every dataset, the other base groups may only list and read.
		// Data-plane (fs:) access to dataset repositories is granted separately by the datasets service.
		attachments[0].policies = append(attachments[0].policies, DatasetsFullAccessPolicy)
		attachments[1].policies = append(attachments[1].policies, DatasetsFullAccessPolicy)
		attachments[2].policies = append(attachments[2].policies, DatasetsReadAllPolicy)
		attachments[3].policies = append(attachments[3].policies, DatasetsReadAllPolicy)
	}

	for _, attachment := range attachments {
		if err := attachPolicies(ctx, authService, attachment.group, attachment.policies); err != nil {
			return err
		}
	}

	return nil
}

// CreateRBACBaseGroups creates the RBAC base groups, policies and attachments.
// It is idempotent and keeps existing policies unchanged. It takes the same options as
// EnsureRBACBaseGroups, including WithDatasetsEnabled (off by default).
func CreateRBACBaseGroups(ctx context.Context, authService auth.Service, ts time.Time, opts ...BaseGroupsOption) error {
	return EnsureRBACBaseGroups(ctx, authService, ts, opts...)
}

// CreateAdminUser setup base groups, policies and create admin user
func CreateAdminUser(ctx context.Context, authService auth.Service, superuser *model.SuperuserConfiguration) (*model.Credential, error) {
	// Set up the basic groups and policies
	now := time.Now()
	err := CreateBaseGroups(ctx, authService, now)
	if err != nil {
		return nil, err
	}

	return AddAdminUser(ctx, authService, superuser, true)
}

func AddAdminUser(ctx context.Context, authService auth.Service, user *model.SuperuserConfiguration, addToAdmins bool) (*model.Credential, error) {
	// create admin user
	user.Source = "internal"
	_, err := authService.CreateUser(ctx, &user.User)
	if err != nil {
		return nil, fmt.Errorf("create user - %w", err)
	}
	defer func() {
		// delete admin on any error to avoid partial setup
		if err != nil {
			logger := logging.ContextUnavailable()
			logger.WithError(err).Warn("Failed to create admin user, deleting user")
			if delUserErr := authService.DeleteUser(ctx, user.Username); delUserErr != nil {
				logger.WithError(delUserErr).Error("Failed to delete user")
			}
		}
	}()

	if addToAdmins {
		// verify the admin group exists
		_, err = authService.GetGroup(ctx, AdminsGroup)
		if err != nil {
			return nil, fmt.Errorf("admin group - %w", err)
		}

		err = authService.AddUserToGroup(ctx, user.Username, AdminsGroup)
		if err != nil {
			return nil, fmt.Errorf("add user to group - %w", err)
		}
	}

	var creds *model.Credential
	if user.AccessKeyID == "" {
		// Generate and return a key pair
		creds, err = authService.CreateCredentials(ctx, user.Username)
		if err != nil {
			return nil, fmt.Errorf("create credentials for %s: %w", user.Username, err)
		}
	} else {
		creds, err = authService.AddCredentials(ctx, user.Username, user.AccessKeyID, user.SecretAccessKey)
		if err != nil {
			return nil, fmt.Errorf("add credentials for %s: %w", user.Username, err)
		}
	}
	return creds, nil
}

func CreateInitialAdminUser(ctx context.Context, authService auth.Service, cfg config.Config, metadataManger auth.MetadataManager, username string) (*model.Credential, error) {
	return CreateInitialAdminUserWithKeys(ctx, authService, cfg, metadataManger, username, nil, nil)
}

func CreateInitialAdminUserWithKeys(ctx context.Context, authService auth.Service, cfg config.Config, metadataManager auth.MetadataManager, username string, accessKeyID *string, secretAccessKey *string) (*model.Credential, error) {
	adminUser := &model.SuperuserConfiguration{
		User: model.User{
			CreatedAt: time.Now(),
			Username:  username,
		},
	}
	if accessKeyID != nil && secretAccessKey != nil {
		adminUser.AccessKeyID = *accessKeyID
		adminUser.SecretAccessKey = *secretAccessKey
	}

	// create first admin user
	var (
		cred *model.Credential
		err  error
	)
	authUICfg := cfg.AuthConfig().GetAuthUIConfig()
	if authUICfg.IsAuthBasic() {
		if cred, err = AddAdminUser(ctx, authService, adminUser, false); err != nil {
			return nil, err
		}
	} else if cred, err = CreateAdminUser(ctx, authService, adminUser); err != nil {
		return nil, err
	}

	// update setup time with auth type used
	if err = metadataManager.UpdateSetupTimestamp(ctx, time.Now(), authUICfg.RBAC); err != nil {
		logging.FromContext(ctx).WithError(err).Error("Failed the update setup timestamp")
	}

	return cred, err
}

func CreateBaseGroups(ctx context.Context, authService auth.Service, ts time.Time, opts ...BaseGroupsOption) error {
	if !authService.IsAdvancedAuth() {
		return nil
	}
	return CreateRBACBaseGroups(ctx, authService, ts, opts...)
}
