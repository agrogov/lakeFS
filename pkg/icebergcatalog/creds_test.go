package icebergcatalog

// Tests for credential vending (ICEBERG_CATALOG_PLAN.md §8 "Data-plane access", §2.1 "Access
// delegation", §7.2 "Authorization"). Every unit test name starts with TestCredentialVending so
// that the task's validation command, `go test -run TestCredentialVending ./pkg/icebergcatalog/`,
// runs exactly this file, apart from TestVendMinIO - the MinIO integration test, which the second
// half of the validation runs by name and which skips itself when no MinIO identity is given.
//
// The authorization half uses the same simplified-ACL service as authz_test.go, so a read grant
// and a write grant are real policy decisions rather than a stubbed "allow". The STS half is a
// recording fake: the session policy, the duration, the external ID and the session name are
// properties of the AssumeRole request, so they are asserted without a network call, and the
// "best effort" rule of §8 is a fake that fails.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// credsTestRepo, credsTestRef and credsTestTable are the table address of authz_test.go's
	// §7.2 fixture (arn:lakefs:catalog:::table/lakehouse/inventory.books/titles).
	credsTestRepo  = authzRepo
	credsTestRef   = authzRef
	credsTestTable = "titles"

	// credsTestTableUUID is the table UUID the session is scoped by: the policy prefix is
	// <storage namespace>/_iceberg/<uuid>, never the storage namespace itself.
	credsTestTableUUID        = "9c12d441-03fe-4693-9a96-a0705ddf69c1"
	credsTestStorageNamespace = "s3://lakehouse-data"
	credsTestRoot             = credsTestStorageNamespace + "/_iceberg/" + credsTestTableUUID

	// credsTestRoleARN is the role the catalog assumes.
	credsTestRoleARN = "arn:aws:iam::123456789012:role/lakefs-iceberg"

	// credsTestExternalID is the ExternalId of the fixture role's trust policy.
	credsTestExternalID = "lakefs-external-id"

	// The credentials the fake STS answers with.
	credsTestAccessKeyID     = "ASIATESTACCESSKEY"
	credsTestSecretAccessKey = "test-secret-access-key"
	credsTestSessionToken    = "test-session-token"
)

// credsFakeSTS is the recording STS client. It captures the one AssumeRole request of a test and
// answers with a configurable output or error.
type credsFakeSTS struct {
	input  *sts.AssumeRoleInput
	calls  int
	output *sts.AssumeRoleOutput
	err    error
}

func (f *credsFakeSTS) AssumeRole(_ context.Context, params *sts.AssumeRoleInput, _ ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	f.calls++
	f.input = params
	if f.err != nil {
		return nil, f.err
	}
	if f.output != nil {
		return f.output, nil
	}
	return &sts.AssumeRoleOutput{Credentials: &ststypes.Credentials{
		AccessKeyId:     aws.String(credsTestAccessKeyID),
		SecretAccessKey: aws.String(credsTestSecretAccessKey),
		SessionToken:    aws.String(credsTestSessionToken),
		Expiration:      aws.Time(time.Now().Add(time.Hour).Truncate(time.Second)),
	}}, nil
}

// credsTestVending is the setting of a test that vends: the role, the documented default session
// duration and an external ID.
func credsTestVending() CredentialsVendingConfig {
	return CredentialsVendingConfig{
		RoleARN:         credsTestRoleARN,
		SessionDuration: DefaultVendedSessionDuration,
		ExternalID:      credsTestExternalID,
	}
}

// credsTestScope answers the table scope of the fixture table: its root and the location its
// metadata names, which is the root itself.
func credsTestScope(_ context.Context, _ Address, _ AuthorizeFunc) (tableDataScope, error) {
	return tableDataScope{Root: credsTestRoot, Location: credsTestRoot}, nil
}

// credsTestAdapter is an S3 blockstore: the only blockstore that can vend (§8).
func credsTestAdapter() block.Adapter {
	return &signingAdapter{blockstoreType: block.BlockstoreTypeS3, preSignSupported: true}
}

// newTestCredentialService builds the vended-credentials service the way NewCredentialService
// does, with the scope and the STS client supplied by the test.
func newTestCredentialService(adapter block.Adapter, authz *Authorizer, vending CredentialsVendingConfig, assumer STSAssumer, scope func(context.Context, Address, AuthorizeFunc) (tableDataScope, error)) *credentialService {
	if scope == nil {
		scope = credsTestScope
	}
	return &credentialService{
		scope:   scope,
		authz:   authz,
		cfg:     vending,
		logger:  logging.ContextUnavailable(),
		prefix:  CatalogPrefix,
		enabled: vending.Configured() && VendedCredentialsSupported(adapter),
		assumer: assumer,
	}
}

// newTestCredentialRouter mounts the service's routes behind the production middleware stack.
func newTestCredentialRouter(t *testing.T, service *credentialService) http.Handler {
	t.Helper()
	router, err := newRouter(namespacesUser, &Config{}, testServer(&Config{Prefix: CatalogPrefix}, service.Routes()...))
	require.NoError(t, err, "build catalog router")
	return router
}

// credentialsTarget is the credentials URL of one ref of the fixture table.
func credentialsTarget(ref string) string {
	return "/v1/" + CatalogPrefix + "/namespaces/" + nodes(credsTestRepo, ref, "inventory", "books") +
		"/tables/" + credsTestTable + "/credentials"
}

// getCredentials sends one credentials request through router. An empty delegation means "no
// header".
func getCredentials(t *testing.T, router http.Handler, user, ref, delegation string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, credentialsTarget(ref), nil)
	req.Header.Set("X-Test-User", user)
	if delegation != "" {
		req.Header.Set(AccessDelegationHeader, delegation)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// requireVended asserts a successful credentials response and returns its body.
func requireVended(t *testing.T, rec *httptest.ResponseRecorder) LoadCredentialsResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	require.Equal(t, contentTypeJSON, rec.Header().Get("Content-Type"))
	var response LoadCredentialsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	require.NotNil(t, response.StorageCredentials, "storage-credentials is a required member of the spec's model")
	require.Len(t, response.StorageCredentials, 1)
	credential := response.StorageCredentials[0]
	require.Equal(t, credsTestRoot, credential.Prefix, "the credentials apply to the table root, not the storage namespace")
	require.Equal(t, credsTestAccessKeyID, credential.Config[accessKeyIDProperty])
	require.Equal(t, credsTestSecretAccessKey, credential.Config[secretAccessKeyProperty])
	require.Equal(t, credsTestSessionToken, credential.Config[sessionTokenProperty])
	require.NotEmpty(t, credential.Config[sessionTokenExpiresAtMsProperty],
		"the expiry lets a client refresh before the session dies")
	return response
}

// requireNoCredentials asserts the §8 best-effort answer: 200, and an empty list rather than an
// error, so the client falls back to its own storage credentials.
func requireNoCredentials(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var response LoadCredentialsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), "body: %s", rec.Body.String())
	require.NotNil(t, response.StorageCredentials)
	require.Empty(t, response.StorageCredentials)
}

// policyOf decodes the inline session policy of an AssumeRole request.
func policyOf(t *testing.T, input *sts.AssumeRoleInput) policyDocument {
	t.Helper()
	require.NotNil(t, input, "the request must reach STS")
	require.NotNil(t, input.Policy, "every session carries a session policy")
	var document policyDocument
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(input.Policy)), &document),
		"policy: %s", aws.ToString(input.Policy))
	return document
}

func TestCredentialVending(t *testing.T) {
	t.Run("vends a read session scoped to the table prefix", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)
		router := newTestCredentialRouter(t, service)

		rec := getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation)

		requireVended(t, rec)
		require.Equal(t, 1, stsFake.calls)
		require.Equal(t, credsTestRoleARN, aws.ToString(stsFake.input.RoleArn))
		require.Equal(t, credsTestExternalID, aws.ToString(stsFake.input.ExternalId))
		require.Equal(t, int32(3600), aws.ToInt32(stsFake.input.DurationSeconds),
			"the documented default session_duration is one hour")
		require.NotContains(t, sessionName(Caller{Username: "alice"}, Address{
			Repo: credsTestRepo, Ref: credsTestRef, Namespace: Namespace{"inventory", "books"}, Table: credsTestTable,
		}), "%1F", "the session name is sanitized for STS")
		require.Equal(t, sessionName(Caller{Username: "alice"}, Address{
			Repo: credsTestRepo, Ref: credsTestRef, Namespace: Namespace{"inventory", "books"}, Table: credsTestTable,
		}), aws.ToString(stsFake.input.RoleSessionName))

		document := policyOf(t, stsFake.input)
		require.Equal(t, policyDocumentVersion, document.Version)
		objectStatement := document.Statement[0]
		require.Equal(t, []string{"arn:aws:s3:::lakehouse-data/" + DataPrefix + "/" + credsTestTableUUID + "/*"},
			objectStatement.Resource, "object actions are scoped to the table root")
		require.NotContains(t, objectStatement.Action, "s3:PutObject", "a read session must not write")
	})

	t.Run("the documentation's read-only policy vends read-only credentials", func(t *testing.T) {
		// "Read-only access to all tables and namespaces" of the public Iceberg documentation,
		// attached alone: catalog:ReadTable and catalog:ReadTableData on table/*/*/*, and no fs:
		// action at all. It must be enough to vend, and the session must be a read session.
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, docReadOnlyStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)
		router := newTestCredentialRouter(t, service)

		requireVended(t, getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation))
		require.Equal(t, 1, stsFake.calls)
		document := policyOf(t, stsFake.input)
		var actions []string
		for _, statement := range document.Statement {
			actions = append(actions, statement.Action...)
		}
		for _, action := range []string{"s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"} {
			require.NotContains(t, actions, action,
				"catalog:ReadTableData alone must not vend a write session")
		}
		require.Contains(t, actions, "s3:GetObject")
	})

	t.Run("requires ReadTableData for a read session", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		// The write grant set normally includes the read actions; this is it without the one
		// action a read session needs, so a write grant cannot be borrowed.
		authz := newAuthzUser(t, authzGrantsExceptAction(permissions.CatalogReadTableDataAction))
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)
		router := newTestCredentialRouter(t, service)

		rec := getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation)

		requireEnvelope(t, rec, http.StatusForbidden, ErrorTypeForbidden)
		require.Zero(t, stsFake.calls, "an unauthorized request must not reach STS")
	})

	t.Run("does not resolve the table of an unauthorized caller", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzGrantsExceptAction(permissions.CatalogReadTableDataAction))
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake,
			func(context.Context, Address, AuthorizeFunc) (tableDataScope, error) {
				t.Error("the table must not be resolved before the authorization decision")
				return tableDataScope{}, nil
			})
		router := newTestCredentialRouter(t, service)

		requireEnvelope(t, getCredentials(t, router, "alice", credsTestRef, ""), http.StatusForbidden, ErrorTypeForbidden)
		require.Zero(t, stsFake.calls)
	})

	t.Run("a missing table is 404", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake,
			func(context.Context, Address, AuthorizeFunc) (tableDataScope, error) {
				return tableDataScope{}, &NoSuchTableException{
					Repo: credsTestRepo, Ref: credsTestRef,
					Namespace: Namespace{"inventory", "books"}, Table: credsTestTable,
				}
			})
		router := newTestCredentialRouter(t, service)

		rec := getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation)

		requireEnvelope(t, rec, http.StatusNotFound, errTypeNoSuchTable)
		require.Zero(t, stsFake.calls)
	})

	t.Run("best effort: an STS failure vends nothing", func(t *testing.T) {
		stsFake := &credsFakeSTS{err: fmt.Errorf("%w: AccessDenied", ErrCredentialVendingFailed)}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)
		router := newTestCredentialRouter(t, service)

		rec := getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation)

		requireNoCredentials(t, rec)
		require.Equal(t, 1, stsFake.calls)
	})

	t.Run("best effort: an STS answer without credentials vends nothing", func(t *testing.T) {
		for _, output := range []*sts.AssumeRoleOutput{
			{},
			{Credentials: &ststypes.Credentials{}},
			{Credentials: &ststypes.Credentials{AccessKeyId: aws.String(credsTestAccessKeyID)}},
		} {
			stsFake := &credsFakeSTS{output: output}
			authz := newAuthzUser(t, authzReadStatements())
			service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)
			router := newTestCredentialRouter(t, service)

			requireNoCredentials(t, getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation))
		}
	})

	t.Run("best effort: a table outside its own root is not vended", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake,
			func(context.Context, Address, AuthorizeFunc) (tableDataScope, error) {
				return tableDataScope{
					Root:     credsTestRoot,
					Location: credsTestStorageNamespace + "/_iceberg/some-other-table",
				}, nil
			})
		router := newTestCredentialRouter(t, service)

		requireNoCredentials(t, getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation))
		require.Zero(t, stsFake.calls, "a table that lives elsewhere must not be handed a session")
	})

	t.Run("refuses a request that asked for a different delegation", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)
		router := newTestCredentialRouter(t, service)

		for _, delegation := range []string{RemoteSigningDelegation, "none"} {
			requireEnvelope(t, getCredentials(t, router, "alice", credsTestRef, delegation),
				http.StatusBadRequest, ErrTypeBadRequest)
		}
		require.Zero(t, stsFake.calls)

		// A client that sends both mechanisms, or none at all, gets credentials.
		requireVended(t, getCredentials(t, router, "alice", credsTestRef,
			VendedCredentialsDelegation+","+RemoteSigningDelegation))
		requireVended(t, getCredentials(t, router, "alice", credsTestRef, ""))
		require.Equal(t, 2, stsFake.calls)
	})

	t.Run("best effort: an out-of-range duration vends nothing", func(t *testing.T) {
		// Config.Validate rejects this at startup; a service assembled by hand must not send STS a
		// duration it refuses, so the vend is skipped instead.
		vending := credsTestVending()
		vending.SessionDuration = time.Minute
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, vending, stsFake, nil)
		router := newTestCredentialRouter(t, service)

		requireNoCredentials(t, getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation))
		require.Zero(t, stsFake.calls)
	})

	t.Run("fails closed without an authorizer", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		service := newTestCredentialService(credsTestAdapter(), nil, credsTestVending(), stsFake, nil)
		router := newTestCredentialRouter(t, service)

		requireEnvelope(t, getCredentials(t, router, "alice", credsTestRef, VendedCredentialsDelegation),
			http.StatusInternalServerError, ErrTypeInternalServerError)
		require.Zero(t, stsFake.calls, "a missing authorizer must never reach STS")
	})

	t.Run("refuses a prefix that is not the catalog prefix", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)
		router := newTestCredentialRouter(t, service)

		path := "/v1/notlakefs/namespaces/" + nodes(credsTestRepo, credsTestRef, "inventory", "books") +
			"/tables/" + credsTestTable + "/credentials"
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("X-Test-User", "alice")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		requireEnvelope(t, rec, http.StatusNotFound, ErrTypeNoSuchWarehouse)
		require.Zero(t, stsFake.calls)
	})

	t.Run("advertises the endpoint only when vending is configured", func(t *testing.T) {
		configured := newTestCredentialService(credsTestAdapter(), nil, credsTestVending(), &credsFakeSTS{}, nil)
		require.True(t, configured.Enabled())
		routes := configured.Routes()
		require.Len(t, routes, 1)
		require.Equal(t, credentialsEndpoint, routes[0].endpoint)
		require.Contains(t, advertisedEndpoints(routes), credentialsEndpoint)
		require.NotContains(t, nonGoalEndpointFragments,
			"/credentials", "the credentials endpoint is in scope for v1")

		// Not configured: no route, and therefore no line in GET /v1/config endpoints.
		notConfigured := newTestCredentialService(credsTestAdapter(), nil, CredentialsVendingConfig{}, &credsFakeSTS{}, nil)
		require.False(t, notConfigured.Enabled())
		require.Empty(t, notConfigured.Routes())

		// Configured, but the blockstore is not S3: §8 says vending is S3 only, so there is
		// nothing to advertise.
		notS3 := newTestCredentialService(&signingAdapter{blockstoreType: block.BlockstoreTypeMem},
			nil, credsTestVending(), &credsFakeSTS{}, nil)
		require.False(t, notS3.Enabled())
		require.Empty(t, notS3.Routes())

		// The production constructor decides the same way from a store's block adapter.
		require.True(t, VendedCredentialsSupported(credsTestAdapter()))
		require.False(t, VendedCredentialsSupported(nil))
		require.False(t, VendedCredentialsSupported(&signingAdapter{blockstoreType: block.BlockstoreTypeGS}))
		require.False(t, VendedCredentialsSupported(&signingAdapter{blockstoreType: block.BlockstoreTypeAzure}),
			"credential vending is S3 only in OSS; Azure has no vending")

		enabled := NewCredentialService(NewStore(nil, credsTestAdapter()), nil, nil,
			&Config{Prefix: CatalogPrefix, CredentialsVending: credsTestVending()}, &credsFakeSTS{}, nil)
		require.True(t, enabled.Enabled())
		require.Len(t, enabled.Routes(), 1)
		require.Nil(t, NewCredentialService(nil, nil, nil,
			&Config{CredentialsVending: credsTestVending()}, &credsFakeSTS{}, nil).Routes(),
			"a service without a store cannot resolve a table, so it must not advertise")
	})

	t.Run("refuses a request when vending is not enabled", func(t *testing.T) {
		service := newTestCredentialService(credsTestAdapter(), nil, CredentialsVendingConfig{}, &credsFakeSTS{}, nil)
		router := newTestCredentialRouter(t, service)

		// No route is mounted, so the request is the router's 404 in the catalog envelope.
		requireEnvelope(t, getCredentials(t, router, "alice", credsTestRef, ""), http.StatusNotFound, ErrTypeNotFound)
	})
}

// TestCredentialVendingNonS3 pins the OSS decision that credential vending is not implemented
// for non-S3 blockstores. Azure and GCS therefore get no service, no route and no advertisement,
// and a request that reaches the handler anyway is refused with a 501 that names the S3
// requirement rather than silently answering "no credentials".
func TestCredentialVendingNonS3(t *testing.T) {
	for _, blockstore := range []string{block.BlockstoreTypeAzure, block.BlockstoreTypeGS} {
		t.Run(blockstore, func(t *testing.T) {
			adapter := &signingAdapter{blockstoreType: blockstore, preSignSupported: true}
			require.False(t, VendedCredentialsSupported(adapter),
				"vending is S3 only, so a %s blockstore must not be vended for", blockstore)

			service := newTestCredentialService(adapter, nil, credsTestVending(), &credsFakeSTS{}, nil)
			require.False(t, service.Enabled())
			require.Empty(t, service.Routes(), "a service that cannot vend must not mount its route")

			// The router answer: the endpoint does not exist on this deployment.
			handler := newTestCredentialRouter(t, service)
			requireEnvelope(t, getCredentials(t, handler, "alice", credsTestRef, ""),
				http.StatusNotFound, ErrTypeNotFound)

			// The handler answer, should anything reach it directly: a clear 501, not a 200 with
			// an empty credential list that would hide the unsupported blockstore.
			rec := httptest.NewRecorder()
			service.handleLoadCredentials(rec, httptest.NewRequest(http.MethodGet, credentialsTarget(credsTestRef), nil))
			apiError := requireEnvelope(t, rec, http.StatusNotImplemented, ErrTypeNotImplemented)
			require.Equal(t, CredentialVendingUnsupportedMessage, apiError.Message)
			require.Contains(t, apiError.Message, "S3", "the refusal names the supported blockstore")
		})
	}
}

func TestCredentialVendingWriteSession(t *testing.T) {
	addr := Address{Repo: credsTestRepo, Ref: credsTestRef, Namespace: Namespace{"inventory", "books"}, Table: credsTestTable}
	caller := Caller{Username: "alice"}

	t.Run("requires WriteTableData", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzReadStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)

		credentials, err := service.Vend(context.Background(), caller,
			Request{Op: OpWriteTableData, Address: addr, RefIsBranch: true})
		require.Error(t, err, "a read grant must not produce a write session")
		var denied *PermissionDeniedError
		require.ErrorAs(t, err, &denied)
		require.Nil(t, credentials)
		require.Zero(t, stsFake.calls)
	})

	t.Run("vends a read-write session for a writer", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzWriteStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)

		credentials, err := service.Vend(context.Background(), caller,
			Request{Op: OpWriteTableData, Address: addr, RefIsBranch: true})
		require.NoError(t, err)
		require.Len(t, credentials, 1)

		document := policyOf(t, stsFake.input)
		anyAction := func(action string) bool {
			for _, statement := range document.Statement {
				for _, have := range statement.Action {
					if have == action {
						return true
					}
				}
			}
			return false
		}
		for _, action := range []string{"s3:PutObject", "s3:DeleteObject", "s3:GetObject"} {
			require.True(t, anyAction(action), "a write session needs %s", action)
		}
		require.Equal(t, []string{"arn:aws:s3:::lakehouse-data/" + DataPrefix + "/" + credsTestTableUUID + "/*"},
			document.Statement[0].Resource, "even a write session is scoped to the table root")
	})

	t.Run("a write session on a tag is read-only", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzWriteStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)

		_, err := service.Vend(context.Background(), caller,
			Request{Op: OpWriteTableData, Address: addr, RefIsBranch: false})
		var readOnly *ReadOnlyRefError
		require.ErrorAs(t, err, &readOnly, "Q-I4: a tag or a commit is read-only")
		require.Zero(t, stsFake.calls)
	})

	t.Run("refuses an operation that is not a data access operation", func(t *testing.T) {
		stsFake := &credsFakeSTS{}
		authz := newAuthzUser(t, authzWriteStatements())
		service := newTestCredentialService(credsTestAdapter(), authz.authz, credsTestVending(), stsFake, nil)

		_, err := service.Vend(context.Background(), caller, Request{Op: OpReadTable, Address: addr})
		require.ErrorIs(t, err, ErrInvalidCredentialRequest)
		require.Zero(t, stsFake.calls)
	})

	t.Run("a disabled service vends nothing", func(t *testing.T) {
		service := newTestCredentialService(credsTestAdapter(), nil, CredentialsVendingConfig{}, &credsFakeSTS{}, nil)
		_, err := service.Vend(context.Background(), caller, Request{Op: OpReadTableData, Address: addr})
		require.ErrorIs(t, err, ErrCredentialVendingDisabled)
	})
}

// TestCredentialVendingPolicy pins the session policy itself: the narrowest document that works
// (§8, task note of I4-2 - the table prefix, not the storage namespace).
func TestCredentialVendingPolicy(t *testing.T) {
	root, err := parseS3Location(credsTestRoot)
	require.NoError(t, err)

	decode := func(t *testing.T, policy string) policyDocument {
		t.Helper()
		var document policyDocument
		require.NoError(t, json.Unmarshal([]byte(policy), &document), "policy: %s", policy)
		return document
	}

	t.Run("a read session", func(t *testing.T) {
		policy, err := sessionPolicy(root, accessRead)
		require.NoError(t, err)
		document := decode(t, policy)

		require.Equal(t, []policyStatement{
			{
				Effect:   policyEffectAllow,
				Action:   []string{"s3:GetObject", "s3:GetObjectVersion"},
				Resource: []string{"arn:aws:s3:::lakehouse-data/_iceberg/" + credsTestTableUUID + "/*"},
			},
			{
				Effect:   policyEffectAllow,
				Action:   []string{"s3:GetBucketLocation", "s3:ListBucket"},
				Resource: []string{"arn:aws:s3:::lakehouse-data"},
				Condition: map[string]map[string][]string{
					"s3:prefix": {"StringLike": {
						"_iceberg/" + credsTestTableUUID + "/",
						"_iceberg/" + credsTestTableUUID + "/*",
					}},
				},
			},
		}, document.Statement)
	})

	t.Run("the storage namespace is never the prefix", func(t *testing.T) {
		for _, access := range []dataAccess{accessRead, accessWrite} {
			policy, err := sessionPolicy(root, access)
			require.NoError(t, err)
			require.NotContains(t, policy, `"arn:aws:s3:::lakehouse-data/*"`,
				"the policy scope is the table prefix, not the storage namespace")
			require.Contains(t, policy, credsTestTableUUID)
			require.NotContains(t, policy, "_iceberg/*", "the whole _iceberg area must not be granted")
			require.NotContains(t, policy, `"Effect":"Deny"`,
				"a session policy is only ever intersected with the role's policy")
		}
	})

	t.Run("a write session adds the write actions", func(t *testing.T) {
		read, err := sessionPolicy(root, accessRead)
		require.NoError(t, err)
		write, err := sessionPolicy(root, accessWrite)
		require.NoError(t, err)

		writeDocument := decode(t, write)
		readDocument := decode(t, read)
		require.Contains(t, writeDocument.Statement[0].Action, "s3:PutObject")
		require.Contains(t, writeDocument.Statement[0].Action, "s3:AbortMultipartUpload",
			"Iceberg writers use multipart uploads for large data files")
		require.NotContains(t, readDocument.Statement[0].Action, "s3:PutObject")
		require.Equal(t, writeDocument.Statement[1], readDocument.Statement[1],
			"the bucket statement is the same for both kinds of session")
	})

	t.Run("refuses a prefix without a key", func(t *testing.T) {
		bucket, err := parseS3Location("s3://lakehouse-data")
		require.NoError(t, err)
		_, err = sessionPolicy(bucket, accessRead)
		require.ErrorIs(t, err, ErrInvalidCredentialRequest)
	})

	t.Run("the policy is stable", func(t *testing.T) {
		first, err := sessionPolicy(root, accessWrite)
		require.NoError(t, err)
		second, err := sessionPolicy(root, accessWrite)
		require.NoError(t, err)
		require.Equal(t, first, second, "the marshalled policy must not depend on map iteration order")
	})
}

func TestCredentialVendingSessionName(t *testing.T) {
	t.Run("names the caller, the table and the ref", func(t *testing.T) {
		name := sessionName(Caller{Username: "alice"}, Address{
			Repo: "lakehouse", Ref: "main", Namespace: Namespace{"inventory", "books"}, Table: "titles",
		})
		require.Contains(t, name, "alice")
		require.Contains(t, name, "lakehouse")
		require.Contains(t, name, "titles")
		require.NotContains(t, name, NamespaceSeparator, "the namespace separator is not a legal STS character")
	})

	t.Run("sanitizes and truncates", func(t *testing.T) {
		name := sessionName(Caller{Username: "a/b c"}, Address{
			Repo: "lakehouse", Ref: "main", Namespace: Namespace{"д", "ы"}, Table: "t",
		})
		for _, r := range name {
			legal := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
				strings.ContainsRune("_+=,.@-", r)
			require.True(t, legal, "character %q is not allowed in RoleSessionName", r)
		}
		require.LessOrEqual(t, len(name), roleSessionNameMaxLength)

		long := sessionName(Caller{Username: strings.Repeat("x", 100)}, Address{Repo: "r", Table: "t"})
		require.Len(t, long, roleSessionNameMaxLength)
	})

	t.Run("an empty caller still yields a legal name", func(t *testing.T) {
		name := sessionName(Caller{}, Address{})
		require.GreaterOrEqual(t, len(name), 2, "STS requires at least two characters")
		require.LessOrEqual(t, len(name), roleSessionNameMaxLength)
	})
}

// TestCredentialVendingConfig covers the setting of §8. It lives here rather than in config_test.go
// because it belongs to this task; the helper it uses - useConfig - is config_test.go's.
func TestCredentialVendingConfig(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		useConfig(t, "{}\n")
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.False(t, cfg.CredentialsVending.Configured())
		require.Zero(t, cfg.CredentialsVending.SessionDuration,
			"an unconfigured duration stays zero, so it cannot be mistaken for an explicit 1h")
	})

	t.Run("blockstore.s3.credentials_vending", func(t *testing.T) {
		useConfig(t, `
blockstore:
  s3:
    credentials_vending:
      role_arn: arn:aws:iam::123456789012:role/lakefs-iceberg
      external_id: lakefs-external-id
      endpoint: https://sts.example.com
      session_duration: 2h
`)
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, CredentialsVendingConfig{
			RoleARN:         credsTestRoleARN,
			SessionDuration: 2 * time.Hour,
			ExternalID:      credsTestExternalID,
			Endpoint:        "https://sts.example.com",
		}, cfg.CredentialsVending)
	})

	t.Run("iceberg.credentials_vending", func(t *testing.T) {
		useConfig(t, `
iceberg:
  enabled: true
  credentials_vending:
    role_arn: arn:aws:iam::123456789012:role/lakefs-iceberg
`)
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, credsTestRoleARN, cfg.CredentialsVending.RoleARN,
			"the setting is also accepted inside the iceberg section, which a YAML file can carry today")
		require.Equal(t, DefaultVendedSessionDuration, cfg.CredentialsVending.SessionDuration,
			"the documented default session_duration is one hour")
	})

	t.Run("the blockstore key wins over the iceberg key", func(t *testing.T) {
		useConfig(t, `
iceberg:
  credentials_vending:
    role_arn: arn:aws:iam::123456789012:role/from-iceberg
blockstore:
  s3:
    credentials_vending:
      role_arn: arn:aws:iam::123456789012:role/from-blockstore
`)
		cfg, err := LoadConfig()
		require.NoError(t, err)
		require.Equal(t, "arn:aws:iam::123456789012:role/from-blockstore", cfg.CredentialsVending.RoleARN)
	})

	t.Run("rejects settings without a role", func(t *testing.T) {
		for _, yaml := range []string{
			"iceberg:\n  credentials_vending:\n    session_duration: 1h\n",
			"iceberg:\n  credentials_vending:\n    external_id: x\n",
			"iceberg:\n  credentials_vending:\n    endpoint: https://sts.example.com\n",
		} {
			useConfig(t, yaml)
			_, err := LoadConfig()
			require.ErrorIs(t, err, ErrInvalidConfig, "yaml: %s", yaml)
			require.Contains(t, err.Error(), "requires role_arn")
		}
	})

	t.Run("rejects an unusable role, duration or endpoint", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			vending CredentialsVendingConfig
		}{
			{name: "not an arn", vending: CredentialsVendingConfig{RoleARN: "lakefs-iceberg", SessionDuration: time.Hour}},
			{name: "duration below the STS minimum", vending: CredentialsVendingConfig{RoleARN: credsTestRoleARN, SessionDuration: 14 * time.Minute}},
			{name: "duration above the STS maximum", vending: CredentialsVendingConfig{RoleARN: credsTestRoleARN, SessionDuration: 13 * time.Hour}},
			{name: "endpoint is not a url", vending: CredentialsVendingConfig{RoleARN: credsTestRoleARN, SessionDuration: time.Hour, Endpoint: "sts.example.com"}},
			{name: "endpoint is not http", vending: CredentialsVendingConfig{RoleARN: credsTestRoleARN, SessionDuration: time.Hour, Endpoint: "ftp://sts.example.com"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				cfg := &Config{Prefix: CatalogPrefix, CredentialsVending: test.vending}
				cfg.setDefaults()
				require.ErrorIs(t, cfg.Validate(), ErrInvalidConfig)
			})
		}
	})

	t.Run("an absent duration is the one-hour default", func(t *testing.T) {
		cfg := &Config{Prefix: CatalogPrefix, CredentialsVending: CredentialsVendingConfig{RoleARN: credsTestRoleARN}}
		cfg.setDefaults()
		require.NoError(t, cfg.Validate())
		require.Equal(t, DefaultVendedSessionDuration, cfg.CredentialsVending.SessionDuration)
	})

	t.Run("accepts the STS bounds", func(t *testing.T) {
		for _, duration := range []time.Duration{MinimumVendedSessionDuration, MaximumVendedSessionDuration} {
			cfg := &Config{Prefix: CatalogPrefix, CredentialsVending: CredentialsVendingConfig{
				RoleARN: credsTestRoleARN, SessionDuration: duration,
			}}
			cfg.setDefaults()
			require.NoError(t, cfg.Validate(), "duration %s", duration)
		}
	})
}

// TestCredentialVendingReservedTarget is the credential half of the reserved-namespace rule
// (creds.go, sign.go, METADATA_SEARCH_DESIGN.md §5.4): a write vend whose resolved target is a
// namespace the resolver reserved falls back to a read session. The requested address is ordinary,
// so the old check on the requested namespace passed; only the target is reserved. The fixture is
// sign_test.go's redirecting resolver, which proves the check follows a redirect instead of trusting
// the address the caller named.
func TestCredentialVendingReservedTarget(t *testing.T) {
	f := newSignResolverFixture(t)
	front := Address{
		Repo: tablesTestRepo, Ref: tablesTestRef,
		Namespace: Namespace{"inventory", "books"}, Table: signWritableTable,
	}
	target := Address{
		Repo: f.metaRepo, Ref: tablesTestRef,
		Namespace: Namespace{SystemNamespace}, Table: resolverObjectMetadataTable,
	}
	store := NewStore(f.catalog, f.objects, WithTableResolver(signRedirectResolver{
		from: front, to: target, reserved: target,
	}))
	stsFake := &credsFakeSTS{}
	service := NewCredentialService(store, NewAuthorizer(f.authz.svc), f.catalog,
		&Config{Prefix: CatalogPrefix, CredentialsVending: credsTestVending()}, stsFake, nil)
	require.True(t, service.Enabled(), "the fake object store can vend")

	credentials, err := service.TableCredentials(t.Context(), Caller{Username: "carol"}, front, true)
	require.NoError(t, err)
	require.Len(t, credentials, 1)
	require.Equal(t, f.metaTableLocation, credentials[0].Prefix,
		"the session is scoped to the table the resolver read")
	require.Equal(t, 1, stsFake.calls, "the write attempt must not reach STS")

	document := policyOf(t, stsFake.input)
	objectStatement := document.Statement[0]
	require.NotContains(t, objectStatement.Action, "s3:PutObject",
		"a redirect into a reserved namespace must fall back to a read session")
	require.Contains(t, objectStatement.Action, "s3:GetObject")
}

// TestCredentialVendingMetadataSearchVirtualTable is the same rule through the real built-in
// MetadataSearchResolver rather than a synthetic redirect: a branch write request for the
// deprecated catalog's address, R.<ref>.system.object_metadata, resolves to R-metadata's
// system namespace, so the request falls back to a read session and its policy never contains
// s3:PutObject. The redirect alone is the reason: the old identifier's namespace is ordinary in
// the lakeFS catalog now (METADATA_SEARCH_REALIGN.md RA3), and a metadata table is always
// read-only (G8).
func TestCredentialVendingMetadataSearchVirtualTable(t *testing.T) {
	f := newSignResolverFixture(t)
	virtual := resolverVirtualAddress(tablesTestRef)
	store := NewStore(f.catalog, f.objects,
		WithTableResolver(NewDeprecatedMetadataSearchResolver(f.catalog, nil)))
	stsFake := &credsFakeSTS{}
	service := NewCredentialService(store, NewAuthorizer(f.authz.svc), f.catalog,
		&Config{Prefix: CatalogPrefix, CredentialsVending: credsTestVending()}, stsFake, nil)
	require.True(t, service.Enabled(), "the fake object store can vend")

	credentials, err := service.TableCredentials(t.Context(), Caller{Username: "carol"}, virtual, true)
	require.NoError(t, err)
	require.Len(t, credentials, 1)
	require.Equal(t, f.metaTableLocation, credentials[0].Prefix,
		"the read session is scoped to the companion table the deprecated address resolves to")
	require.Equal(t, 1, stsFake.calls, "the write attempt must not reach STS")

	document := policyOf(t, stsFake.input)
	require.NotContains(t, document.Statement[0].Action, "s3:PutObject",
		"a metadata table is read-only even when the caller writes the data repository's branch")
	require.Contains(t, document.Statement[0].Action, "s3:GetObject")
}

// TestVendMinIO is the integration half of the validation: it assumes a real role through STS,
// and then proves with the credentials it got back that the session is scoped to the table prefix
// - a read inside the prefix works, a read outside it and a write from a read session do not.
//
// It is skipped unless a MinIO identity, bucket and role are given, because it needs a running S3
// implementation with STS. MinIO's AssumeRole takes the RoleArn as the name of an existing MinIO
// policy, so the default below is one the server ships with:
//
//	docker run --rm -p 9000:9000 \
//	  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//	  minio/minio server /data
//	LAKEFS_ICEBERG_MINIO_STS_ENDPOINT=http://127.0.0.1:9000 \
//	LAKEFS_ICEBERG_MINIO_ACCESS_KEY_ID=minioadmin \
//	LAKEFS_ICEBERG_MINIO_SECRET_ACCESS_KEY=minioadmin \
//	LAKEFS_ICEBERG_MINIO_BUCKET=lakehouse-data \
//	go test -tags integration -run TestVendMinIO ./pkg/icebergcatalog/
//
// The MinIO creds are handed to the AWS SDK through the environment, so the service builds its
// STS client exactly the way production does (newSTSClient).
func TestVendMinIO(t *testing.T) {
	const (
		endpointEnv  = "LAKEFS_ICEBERG_MINIO_STS_ENDPOINT"
		accessKeyEnv = "LAKEFS_ICEBERG_MINIO_ACCESS_KEY_ID"
		secretKeyEnv = "LAKEFS_ICEBERG_MINIO_SECRET_ACCESS_KEY"
		bucketEnv    = "LAKEFS_ICEBERG_MINIO_BUCKET"
		roleEnv      = "LAKEFS_ICEBERG_MINIO_ROLE_ARN"

		minioDefaultRoleARN = "arn:minio:iam:::role/consoleAdmin"
	)
	endpoint, accessKey, secretKey, bucket := os.Getenv(endpointEnv), os.Getenv(accessKeyEnv),
		os.Getenv(secretKeyEnv), os.Getenv(bucketEnv)
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		t.Skipf("set %s, %s, %s and %s to run the MinIO STS integration test",
			endpointEnv, accessKeyEnv, secretKeyEnv, bucketEnv)
	}
	roleARN := os.Getenv(roleEnv)
	if roleARN == "" {
		roleARN = minioDefaultRoleARN
	}

	// The AWS SDK reads the server's own identity from the environment, exactly like a lakeFS
	// server that runs with static keys.
	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	t.Setenv("AWS_REGION", defaultSTSRegion)

	ctx := context.Background()
	root := "s3://" + bucket + "/" + DataPrefix + "/" + credsTestTableUUID
	scope := func(context.Context, Address, AuthorizeFunc) (tableDataScope, error) {
		return tableDataScope{Root: root, Location: root}, nil
	}
	authz := newAuthzUser(t, authzWriteStatements())
	service := newTestCredentialService(credsTestAdapter(), authz.authz, CredentialsVendingConfig{
		RoleARN:         roleARN,
		SessionDuration: MinimumVendedSessionDuration,
		Endpoint:        endpoint,
	}, nil, scope)
	require.True(t, service.Enabled())
	require.NoError(t, createMinIOBucket(t, endpoint, bucket))

	addr := Address{Repo: credsTestRepo, Ref: credsTestRef, Namespace: Namespace{"inventory", "books"}, Table: credsTestTable}
	caller := Caller{Username: "alice"}

	// An object of the table, written with the server's own credentials, and one outside the
	// table's prefix.
	insideKey := DataPrefix + "/" + credsTestTableUUID + "/data/probe.parquet"
	outsideKey := DataPrefix + "/some-other-table/data/probe.parquet"
	admin := newMinIOS3Client(t, endpoint, accessKey, secretKey, "")
	putObject(t, admin, bucket, insideKey, "inside")
	putObject(t, admin, bucket, outsideKey, "outside")

	t.Run("read session", func(t *testing.T) {
		credentials, err := service.Vend(ctx, caller, Request{Op: OpReadTableData, Address: addr})
		require.NoError(t, err)
		require.Len(t, credentials, 1, "the session must be vended, not a best-effort empty answer")
		require.Equal(t, root, credentials[0].Prefix)

		vended := newMinIOS3Client(t, endpoint,
			credentials[0].Config[accessKeyIDProperty],
			credentials[0].Config[secretAccessKeyProperty],
			credentials[0].Config[sessionTokenProperty])

		require.Equal(t, "inside", getObjectBody(t, vended, bucket, insideKey),
			"the session must read the table it was scoped to")
		requireGetDenied(t, vended, bucket, outsideKey)
		requirePutDenied(t, vended, bucket, DataPrefix+"/"+credsTestTableUUID+"/data/new.parquet")
	})

	t.Run("write session", func(t *testing.T) {
		credentials, err := service.Vend(ctx, caller,
			Request{Op: OpWriteTableData, Address: addr, RefIsBranch: true})
		require.NoError(t, err)
		require.Len(t, credentials, 1)

		vended := newMinIOS3Client(t, endpoint,
			credentials[0].Config[accessKeyIDProperty],
			credentials[0].Config[secretAccessKeyProperty],
			credentials[0].Config[sessionTokenProperty])

		written := DataPrefix + "/" + credsTestTableUUID + "/data/written.parquet"
		putObject(t, vended, bucket, written, "written")
		require.Equal(t, "written", getObjectBody(t, vended, bucket, written))
		requirePutDenied(t, vended, bucket, DataPrefix+"/some-other-table/data/written.parquet")
		requireGetDenied(t, vended, bucket, outsideKey)
	})
}

// newMinIOS3Client builds an S3 client for a MinIO endpoint: path-style addressing, the region the
// STS client signs with, and the given credentials (a session token is optional, because the
// server's own keys have none).
func newMinIOS3Client(t *testing.T, endpoint, accessKeyID, secretAccessKey, sessionToken string) *s3.Client {
	t.Helper()
	return s3.NewFromConfig(aws.Config{
		Region:      defaultSTSRegion,
		Credentials: awscredentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, sessionToken),
	}, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
}

// createMinIOBucket creates the test bucket, reading the MinIO identity from the environment the
// test was given. An existing bucket is not an error.
func createMinIOBucket(t *testing.T, endpoint, bucket string) error {
	t.Helper()
	client := newMinIOS3Client(t, endpoint, os.Getenv("LAKEFS_ICEBERG_MINIO_ACCESS_KEY_ID"),
		os.Getenv("LAKEFS_ICEBERG_MINIO_SECRET_ACCESS_KEY"), "")
	_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" {
			return nil
		}
		return err
	}
	return nil
}

func putObject(t *testing.T, client *s3.Client, bucket, key, body string) {
	t.Helper()
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err, "put s3://%s/%s", bucket, key)
}

func getObjectBody(t *testing.T, client *s3.Client, bucket, key string) string {
	t.Helper()
	output, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err, "get s3://%s/%s", bucket, key)
	defer func() { require.NoError(t, output.Body.Close()) }()
	body, err := io.ReadAll(output.Body)
	require.NoError(t, err)
	return string(body)
}

func requireGetDenied(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	_, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	requireStorageDenied(t, err, "get s3://%s/%s", bucket, key)
}

func requirePutDenied(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()
	_, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("denied"),
	})
	requireStorageDenied(t, err, "put s3://%s/%s", bucket, key)
}

// requireStorageDenied asserts that a storage request failed with an access denial: the proof that
// the vended session is scoped to the table prefix and to its access kind.
func requireStorageDenied(t *testing.T, err error, format string, args ...any) {
	t.Helper()
	require.Errorf(t, err, format, args...)
	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "a storage denial is an API error: %v", err)
	require.Contains(t, []string{"AccessDenied", "Forbidden", "AccessDeniedException"}, apiErr.ErrorCode(),
		"expected an access denial: %v", err)
}
