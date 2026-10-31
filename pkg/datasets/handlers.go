package datasets

// handlers.go implements the datasets control-plane handlers of DATASETS_DESIGN.md §10 on
// top of the Service. It maps the generated apigen types onto the package types, builds the
// authorization caller of each request (§9.1) and maps the typed service errors onto the
// status codes of §10.4:
//
//	400  validation / source problems, with one entry per problem in the errors array
//	403  a permission denial, naming the data item when it was a source check (§9.3)
//	404  unknown dataset, version record, task, proposal or grant
//	409  name taken by a repository or a dataset, a lost publish race (§5.4), a proposal that
//	     is no longer open, or a stale proposal base (§5.6, with latest_version in the body)
//	422  a source repository on a different storage ID (§5.2 rule 4)
//	500  a failure to decide (never a denial) or an internal error
//	501  datasets disabled, a reserved item type, or an operation of a later phase
//	     (metadata-key CRUD, GET /datasets/config)
//
// Authentication itself is enforced in front of these handlers by api.GenericAuthMiddleware
// plus requireUser (see http.go), so every handler can rely on auth.GetUser.
//
// Clean-room note (§2): written from DATASETS_DESIGN.md and the public lakeFS documentation
// only. No code, SQL, protobuf or UI asset from lakeFS Enterprise, and nothing from upstream
// lakeFS >= 1.87, was used.

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// MetadataFilterPrefix is the query-parameter prefix of the metadata filters of
// GET /datasets (§10.1): metadata.<key>=<value>.
const MetadataFilterPrefix = "metadata."

// server implements apigen.ServerInterface.
//
// Every operation delegates to Service: create, get, list, delete, validate, the access
// grants (§10.1), publish and the version reads, the version diff of §10.3, the proposals of
// §5.6, metadata-key CRUD and GET /datasets/config. Only reserved item types still map to
// 501 (ErrUnsupported).
type server struct {
	svc      *Service
	clientIP *httputil.ClientIPResolver
	// authzFailureStatus is the status a permission denial is answered with (DOC-17): the
	// configured auth.authorization_failure_status, defaulting to 401 like the core API.
	authzFailureStatus int
}

// newServer wraps a Service in the generated server interface. clientIP may be nil, in which
// case the safe default of trusting no proxy applies to authorization conditions.
// authzFailureStatus is the configured auth.authorization_failure_status; a zero value means
// http.StatusUnauthorized, the core default.
func newServer(svc *Service, clientIP *httputil.ClientIPResolver, authzFailureStatus int) *server {
	if authzFailureStatus != http.StatusUnauthorized && authzFailureStatus != http.StatusForbidden {
		authzFailureStatus = http.StatusUnauthorized
	}
	return &server{svc: svc, clientIP: clientIP, authzFailureStatus: authzFailureStatus}
}

// caller builds the authorization caller of a request (§9.1): the authenticated principal
// and the client IP/condition context the auth service evaluates conditions with.
func (s *server) caller(r *http.Request) (Caller, error) {
	user, err := auth.GetUser(r.Context())
	if err != nil {
		return Caller{}, err
	}
	clientIP := httputil.ExtractClientIP(r.Header, r.RemoteAddr)
	if s.clientIP != nil {
		clientIP = s.clientIP.Extract(r.Header, r.RemoteAddr)
	}
	return Caller{Username: user.Username, ClientIP: clientIP}, nil
}

// writeServiceError maps a typed service error to the status codes of §10.4.
//
// A permission denial is answered with the configured auth.authorization_failure_status
// (DOC-17): the same setting the core API honours, 401 by default and 403 when the
// installation asks for it. Hard-coding 403 made a client written against the documented
// 401-only denial response fail.
func (s *server) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		permission    *PermissionError
		authorization *AuthorizationError
		validation    *ValidationError
		source        *SourceError
	)
	switch {
	case errors.As(err, &permission):
		httputil.WriteAPIError(w, r, s.authzFailureStatus, permission.Error())
	case errors.As(err, &authorization):
		httputil.WriteAPIError(w, r, http.StatusInternalServerError, authorization.Error())
	case errors.As(err, &validation):
		writeErrorResponse(w, r, http.StatusBadRequest, errorResponse{
			Message: validation.Error(),
			Errors:  problemDetails(validation.Problems),
		})
	case errors.As(err, &source) && errors.Is(source.Err, ErrPermissionDenied):
		// SEC-3: the caller may not read a source object. It is a permission problem, not a
		// document problem, and it follows the same configurable status as a dataset denial.
		httputil.WriteAPIError(w, r, s.authzFailureStatus, err.Error())
	case errors.As(err, &source):
		// §5.2 rule 4: a storage-ID mismatch is a deployment problem (422), every other
		// source problem is a definition problem the client can fix (400).
		if errors.Is(err, ErrStorageIDMismatch) {
			httputil.WriteAPIError(w, r, http.StatusUnprocessableEntity, err.Error())
			return
		}
		writeErrorResponse(w, r, http.StatusBadRequest, errorResponse{
			Message: err.Error(),
			Errors: []errorDetail{{
				Path:    itemPath(source.Item, ""),
				Message: source.Err.Error(),
			}},
		})
	case errors.Is(err, ErrUnsupported):
		httputil.WriteAPIError(w, r, http.StatusNotImplemented, err.Error())
	// The 404 union spans the implemented cards: ErrDatasetNotFound, ErrVersionNotFound and
	// ErrTaskNotFound came in with the publish/versions card (§10.1 version and task reads),
	// ErrRefNotFound came in with the diff card (§10.3 bad left/right ref), ErrProposalNotFound
	// came in with the proposals card (§5.6, §10.1), and isNotFound covers the store/catalog
	// path. Every one of them must map to 404: dropping any sends a client-fixable case to 500.
	case errors.Is(err, ErrDatasetNotFound), errors.Is(err, ErrVersionNotFound), errors.Is(err, ErrTaskNotFound), errors.Is(err, ErrRefNotFound), errors.Is(err, ErrProposalNotFound), errors.Is(err, ErrMetadataKeyNotFound), isNotFound(err):
		httputil.WriteAPIError(w, r, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrNameTaken), errors.Is(err, ErrDatasetConflict), errors.Is(err, ErrPublishInProgress), errors.Is(err, ErrProposalNotOpen), errors.Is(err, ErrMetadataKeyTypeChange), errors.Is(err, ErrMetadataKeyRename), errors.Is(err, kv.ErrPredicateFailed):
		httputil.WriteAPIError(w, r, http.StatusConflict, err.Error())
	default:
		httputil.WriteAPIError(w, r, http.StatusInternalServerError, err.Error())
	}
}

// problemDetails renders validation problems in the §10.4 wire shape.
func problemDetails(problems []Problem) []errorDetail {
	details := make([]errorDetail, 0, len(problems))
	for _, problem := range problems {
		details = append(details, errorDetail(problem))
	}
	return details
}

// writeUnauthenticated answers 401 when a handler cannot find the authenticated principal.
// requireUser makes this unreachable in the mounted router; it exists so a handler mounted
// elsewhere still fails closed.
func writeUnauthenticated(w http.ResponseWriter, r *http.Request) {
	writeErrorResponse(w, r, http.StatusUnauthorized, errorResponse{Message: ErrAuthenticationRequired.Error()})
}

// --- generated server interface: implemented operations ---

// logAction writes the audit line of a mutating handler (DATASETS_DESIGN.md §12.4): the
// same dataset/version/user/action Fields shape the core Controller.LogAction uses, so
// dataset history is greppable next to the API audit log whatever endpoint produced it.
func logAction(r *http.Request, action, dataset string, version int) {
	user, _ := auth.GetUser(r.Context()) // requireUser already rejected an anonymous request
	username := ""
	if user != nil {
		username = user.Username
	}
	logging.FromContext(r.Context()).WithFields(logging.Fields{
		"dataset": dataset,
		"version": version,
		"user":    username,
		"action":  action,
	}).Info("datasets: control-plane action")
}

func (s *server) ListDatasets(w http.ResponseWriter, r *http.Request, params apigen.ListDatasetsParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	req := ListRequest{
		After:    string(valueOrZero(params.After)),
		Amount:   int(valueOrZero(params.Amount)),
		Metadata: metadataFilters(r),
	}
	if params.Prefix != nil {
		req.Prefix = string(*params.Prefix)
	}
	if params.Search != nil {
		req.Search = string(*params.Search)
	}
	result, err := s.svc.List(r.Context(), caller, req)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	body := apigen.DatasetList{
		Results: make([]apigen.Dataset, 0, len(result.Datasets)),
		Pagination: apigen.Pagination{
			HasMore:    result.HasMore,
			NextOffset: result.NextOffset,
			Results:    len(result.Datasets),
			MaxPerPage: result.MaxPerPage,
		},
	}
	for _, view := range result.Datasets {
		body.Results = append(body.Results, datasetToAPI(view))
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, body)
}

func (s *server) CreateDataset(w http.ResponseWriter, r *http.Request, body apigen.CreateDatasetJSONRequestBody, params apigen.CreateDatasetParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	// DOC-14: the create always publishes v1; publish.message only names that version.
	// The async task is an extension reachable only through the explicit `async=true`.
	req := CreateRequest{
		Name:       body.Name,
		Definition: definitionFromAPI(body.Definition),
		Async:      valueOrZero(params.Async),
	}
	if body.Publish != nil && body.Publish.Message != nil {
		req.Message = *body.Publish.Message
	}
	if req.Message == "" {
		req.Message = DefaultCreateMessage(req.Name)
	}
	if req.Async {
		outcome, err := s.svc.CreateAsync(r.Context(), caller, req)
		if err != nil {
			s.writeServiceError(w, r, err)
			return
		}
		logAction(r, "CreateDataset", req.Name, 0)
		httputil.WriteAPIResponse(w, r, http.StatusAccepted, taskToAPI(outcome.Task))
		return
	}
	view, err := s.svc.Create(r.Context(), caller, req)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "CreateDataset", view.Record.Name, view.Record.LatestVersion)
	httputil.WriteAPIResponse(w, r, http.StatusCreated, datasetToAPI(view))
}

func (s *server) GetDataset(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	view, err := s.svc.Get(r.Context(), caller, string(dataset))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, datasetToAPI(view))
}

func (s *server) DeleteDataset(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	if err := s.svc.Delete(r.Context(), caller, string(dataset)); err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "DeleteDataset", string(dataset), 0)
	httputil.WriteAPIResponse(w, r, http.StatusNoContent, nil)
}

func (s *server) ValidateDataset(w http.ResponseWriter, r *http.Request, body apigen.ValidateDatasetJSONRequestBody, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	result, err := s.svc.Validate(r.Context(), caller, string(dataset), definitionFromAPI(body.Definition))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	stats := statsToAPI(result.Stats)
	httputil.WriteAPIResponse(w, r, http.StatusOK, apigen.DatasetValidateResponse{
		Definition: definitionToAPI(result.Definition),
		Stats:      &stats,
	})
}

func (s *server) ListAccess(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, params apigen.ListAccessParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	result, err := s.svc.ListAccess(r.Context(), caller, string(dataset),
		string(valueOrZero(params.After)), int(valueOrZero(params.Amount)))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	body := apigen.AccessGrantList{
		Results: make([]apigen.AccessGrant, 0, len(result.Grants)),
		Pagination: apigen.Pagination{
			HasMore:    result.HasMore,
			NextOffset: result.NextOffset,
			Results:    len(result.Grants),
			MaxPerPage: result.MaxPerPage,
		},
	}
	for _, grant := range result.Grants {
		body.Results = append(body.Results, grantToAPI(grant))
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, body)
}

func (s *server) GrantAccess(w http.ResponseWriter, r *http.Request, body apigen.GrantAccessJSONRequestBody, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	grant, err := s.svc.GrantAccess(r.Context(), caller, string(dataset), body.PrincipalType, body.PrincipalId)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "GrantAccess", string(dataset), 0)
	httputil.WriteAPIResponse(w, r, http.StatusCreated, grantToAPI(grant))
}

func (s *server) RevokeAccess(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, principalType apigen.PrincipalType, principalID apigen.PrincipalID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	if err := s.svc.RevokeAccess(r.Context(), caller, string(dataset), string(principalType), string(principalID)); err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "RevokeAccess", string(dataset), 0)
	httputil.WriteAPIResponse(w, r, http.StatusNoContent, nil)
}

// --- generated server interface: configuration and metadata keys ---

func (s *server) GetDatasetsConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.svc.Config()
	accessModel := cfg.AccessModel
	maxItems := cfg.MaxItems
	storageID := cfg.StorageID
	syncPublishMaxObjects := int64(cfg.SyncPublishMaxObjects)
	httputil.WriteAPIResponse(w, r, http.StatusOK, apigen.DatasetsConfig{
		Enabled:               cfg.Enabled,
		StorageId:             &storageID,
		MaxItems:              &maxItems,
		AccessModel:           &accessModel,
		SyncPublishMaxObjects: &syncPublishMaxObjects,
	})
}

func (s *server) ListMetadataKeys(w http.ResponseWriter, r *http.Request, params apigen.ListMetadataKeysParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	prefix := string(valueOrZero(params.Prefix))
	result, err := s.svc.ListMetadataKeys(r.Context(), caller, prefix,
		string(valueOrZero(params.After)), int(valueOrZero(params.Amount)))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	body := apigen.MetadataKeyList{
		Results: make([]apigen.MetadataKey, 0, len(result.Keys)),
		Pagination: apigen.Pagination{
			HasMore:    result.HasMore,
			NextOffset: result.NextOffset,
			Results:    len(result.Keys),
			MaxPerPage: result.MaxPerPage,
		},
	}
	for _, key := range result.Keys {
		body.Results = append(body.Results, metadataKeyToAPI(key))
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, body)
}

func (s *server) CreateMetadataKey(w http.ResponseWriter, r *http.Request, body apigen.CreateMetadataKeyJSONRequestBody) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	key, err := s.svc.CreateMetadataKey(r.Context(), caller, metadataKeyFromCreateAPI(apigen.MetadataKeyCreateRequest(body)))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, ActionCreateMetadataKey, key.Name, 0)
	httputil.WriteAPIResponse(w, r, http.StatusCreated, metadataKeyToAPI(*key))
}

func (s *server) GetMetadataKey(w http.ResponseWriter, r *http.Request, name apigen.MetadataKeyName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	key, err := s.svc.GetMetadataKey(r.Context(), caller, string(name))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, metadataKeyToAPI(*key))
}

func (s *server) UpdateMetadataKey(w http.ResponseWriter, r *http.Request, body apigen.UpdateMetadataKeyJSONRequestBody, name apigen.MetadataKeyName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	key, err := s.svc.UpdateMetadataKey(r.Context(), caller, string(name), metadataKeyFromUpdateAPI(string(name), apigen.MetadataKeyUpdateRequest(body)))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, ActionUpdateMetadataKey, key.Name, 0)
	httputil.WriteAPIResponse(w, r, http.StatusOK, metadataKeyToAPI(*key))
}

func (s *server) DeleteMetadataKey(w http.ResponseWriter, r *http.Request, name apigen.MetadataKeyName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	if err := s.svc.DeleteMetadataKey(r.Context(), caller, string(name)); err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, ActionDeleteMetadataKey, string(name), 0)
	httputil.WriteAPIResponse(w, r, http.StatusNoContent, nil)
}

func (s *server) ListVersions(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, params apigen.ListVersionsParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	result, err := s.svc.ListVersions(r.Context(), caller, string(dataset),
		string(valueOrZero(params.After)), int(valueOrZero(params.Amount)))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	body := apigen.DatasetVersionList{
		Results: make([]apigen.DatasetVersion, 0, len(result.Versions)),
		Pagination: apigen.Pagination{
			HasMore:    result.HasMore,
			NextOffset: result.NextOffset,
			Results:    len(result.Versions),
			MaxPerPage: result.MaxPerPage,
		},
	}
	for _, version := range result.Versions {
		body.Results = append(body.Results, versionToAPI(version))
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, body)
}

func (s *server) PublishVersion(w http.ResponseWriter, r *http.Request, body apigen.PublishVersionJSONRequestBody, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	outcome, err := s.svc.PublishVersion(r.Context(), caller, string(dataset), PublishVersionRequest{
		Definition:      definitionFromAPI(body.Definition),
		Message:         body.Message,
		ExpectedVersion: body.ExpectedVersion,
	})
	if err != nil {
		if writeVersionConflict(w, r, err) {
			return
		}
		s.writeServiceError(w, r, err)
		return
	}
	// §6.5: a publish above datasets.sync_publish_max_objects answers 202 with the task
	// to poll at GET /datasets/{ds}/tasks/{task} instead of blocking the request.
	if outcome.Accepted() {
		logAction(r, "PublishVersion", string(dataset), 0)
		httputil.WriteAPIResponse(w, r, http.StatusAccepted, taskToAPI(outcome.Task))
		return
	}
	logAction(r, "PublishVersion", string(dataset), outcome.Version.Version)
	httputil.WriteAPIResponse(w, r, http.StatusCreated, versionToAPI(outcome.Version))
}

func (s *server) GetVersion(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, version apigen.VersionRef) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	rec, err := s.svc.GetVersion(r.Context(), caller, string(dataset), string(version))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, versionToAPI(rec))
}

func (s *server) GetTask(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, taskID apigen.TaskID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	task, err := s.svc.GetTask(r.Context(), caller, string(dataset), string(taskID))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, taskToAPI(task))
}

// DiffRefs answers GET /datasets/{ds}/refs/{leftRef}/diff/{rightRef} (§10.3). The response
// body is a DefinitionDiff or a DataDiffList, so it is written directly instead of through
// a generated response type.
func (s *server) DiffRefs(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, leftRef apigen.LeftRef, rightRef apigen.RightRef, params apigen.DiffRefsParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	req := DiffRequest{
		Dataset:  string(dataset),
		LeftRef:  string(leftRef),
		RightRef: string(rightRef),
		After:    string(valueOrZero(params.After)),
		Amount:   int(valueOrZero(params.Amount)),
	}
	if params.Type != nil {
		req.Type = *params.Type
	}
	if params.Prefix != nil {
		req.Prefix = string(*params.Prefix)
	}
	result, err := s.svc.Diff(r.Context(), caller, req)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	if result.Type == DiffTypeData {
		httputil.WriteAPIResponse(w, r, http.StatusOK, dataDiffToAPI(result.Data))
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, definitionDiffToAPI(result.Definition))
}

func (s *server) UpdateDataset(w http.ResponseWriter, r *http.Request, body apigen.UpdateDatasetJSONRequestBody, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	view, err := s.svc.UpdateDataset(r.Context(), caller, string(dataset), PublishVersionRequest{
		Definition: definitionFromAPI(body.Definition),
		Message:    body.Publish.Message,
	})
	if err != nil {
		if writeVersionConflict(w, r, err) {
			return
		}
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "UpdateDataset", string(dataset), view.Record.LatestVersion)
	httputil.WriteAPIResponse(w, r, http.StatusOK, datasetToAPI(view))
}

func (s *server) GetDatasetByRef(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, ref apigen.Ref) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	view, err := s.svc.GetByRef(r.Context(), caller, string(dataset), string(ref))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, datasetToAPI(view))
}

// ListPullRequests answers GET /datasets/pulls, the global pull-request listing (§10.1).
func (s *server) ListPullRequests(w http.ResponseWriter, r *http.Request, params apigen.ListPullRequestsParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	result, err := s.svc.ListAllPullRequests(r.Context(), caller, PullRequestFilter{
		Dataset: string(valueOrZero(params.Dataset)),
		Status:  string(valueOrZero(params.Status)),
		After:   string(valueOrZero(params.After)),
		Amount:  int(valueOrZero(params.Amount)),
	})
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, pullRequestListToAPI(result.PullRequests, result.HasMore, result.NextOffset, result.MaxPerPage))
}

func (s *server) ListDatasetPullRequests(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, params apigen.ListDatasetPullRequestsParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	result, err := s.svc.ListProposals(r.Context(), caller, string(dataset),
		string(valueOrZero(params.Status)), string(valueOrZero(params.After)), int(valueOrZero(params.Amount)))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	views := make([]*ProposalView, 0, len(result.Proposals))
	for _, record := range result.Proposals {
		views = append(views, &ProposalView{
			Record:        record,
			LatestVersion: result.LatestVersion,
			Stale:         record.Status == ProposalStatusOpen && result.LatestVersion != record.BaseVersion,
			Conflicted:    record.Status == ProposalStatusOpen && result.Conflicted,
		})
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, pullRequestListToAPI(views, result.HasMore, result.NextOffset, result.MaxPerPage))
}

func (s *server) CreatePullRequest(w http.ResponseWriter, r *http.Request, body apigen.CreatePullRequestJSONRequestBody, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	req := CreateProposalRequest{
		Definition: definitionFromAPI(body.Definition),
		Title:      body.Title,
	}
	if body.Description != nil {
		req.Description = *body.Description
	}
	view, err := s.svc.CreateProposal(r.Context(), caller, string(dataset), req)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "CreatePullRequest", string(dataset), 0)
	httputil.WriteAPIResponse(w, r, http.StatusCreated, pullRequestToAPI(view))
}

func (s *server) GetPullRequest(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, pullRequest apigen.PullRequestID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	view, err := s.svc.GetProposal(r.Context(), caller, string(dataset), string(pullRequest))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, pullRequestToAPI(view))
}

func (s *server) UpdatePullRequest(w http.ResponseWriter, r *http.Request, body apigen.UpdatePullRequestJSONRequestBody, dataset apigen.DatasetName, pullRequest apigen.PullRequestID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	req := UpdateProposalRequest{Title: body.Title, Description: body.Description}
	if body.Definition != nil {
		definition := definitionFromAPI(*body.Definition)
		req.Definition = &definition
	}
	if body.Status != nil {
		req.Status = *body.Status
	}
	view, err := s.svc.UpdateProposal(r.Context(), caller, string(dataset), string(pullRequest), req)
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, pullRequestToAPI(view))
}

func (s *server) ClosePullRequest(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, pullRequest apigen.PullRequestID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	if _, err := s.svc.CloseProposal(r.Context(), caller, string(dataset), string(pullRequest)); err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "ClosePullRequest", string(dataset), 0)
	httputil.WriteAPIResponse(w, r, http.StatusNoContent, nil)
}

func (s *server) GetPullRequestDiff(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, pullRequest apigen.PullRequestID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	view, err := s.svc.GetProposal(r.Context(), caller, string(dataset), string(pullRequest))
	if err != nil {
		s.writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, definitionDiffToAPI(view.Diff))
}

func (s *server) MergePullRequest(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, pullRequest apigen.PullRequestID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	version, err := s.svc.MergeProposal(r.Context(), caller, string(dataset), string(pullRequest), MergeProposalRequest{})
	if err != nil {
		// §5.6: a stale pull-request base is the 409 whose body carries latest_version, the
		// same shape as a lost expected_version check (§5.4).
		if writeVersionConflict(w, r, err) {
			return
		}
		s.writeServiceError(w, r, err)
		return
	}
	logAction(r, "MergePullRequest", string(dataset), version.Version)
	httputil.WriteAPIResponse(w, r, http.StatusOK, apigen.DatasetPullRequestMerge{
		Version:   version.Version,
		Reference: fmt.Sprintf("v%d", version.Version),
	})
}

// --- API type conversions (§10.2) ---

// datasetToAPI renders a dataset view as the §10.2 Dataset schema.
func datasetToAPI(view *DatasetView) apigen.Dataset {
	version := view.Record.LatestVersion
	createdBy := view.Record.CreatedBy
	state := view.Record.State
	out := apigen.Dataset{
		Id:           view.Record.Name,
		CreationDate: view.Record.CreatedAt.Unix(),
		CreatedBy:    &createdBy,
		Version:      &version,
		State:        &state,
	}
	if view.Record.Description != "" {
		description := view.Record.Description
		out.Description = &description
	}
	if view.Version != nil {
		definition := definitionStateToAPI(view.Version.Definition, view.ItemStatuses)
		stats := statsToAPI(view.Version.Stats)
		out.Definition = &definition
		out.Stats = &stats
	}
	return out
}

// definitionStateToAPI renders a definition with the per-item source-resolution status of
// §10.2 DatasetDefinitionState. statuses is indexed like the definition's data items; a
// missing entry is reported as ok, because the definition itself resolved.
func definitionStateToAPI(in Definition, statuses []ItemStatus) apigen.DatasetDefinitionState {
	formatVersion := in.FormatVersion
	if formatVersion == 0 {
		formatVersion = FormatVersion
	}
	data := make([]apigen.DataItemState, 0, len(in.Data))
	out := apigen.DatasetDefinitionState{
		FormatVersion: &formatVersion,
		Data:          &data,
	}
	if in.Description != "" {
		description := in.Description
		out.Description = &description
	}
	if in.Metadata != nil {
		metadata := metadataToAPI(in.Metadata)
		out.Metadata = &metadata
	}
	for i, item := range in.Data {
		status := string(ItemStatusOK)
		if i < len(statuses) && statuses[i] != "" {
			status = string(statuses[i])
		}
		data = append(data, apigen.DataItemState{
			DataItem: dataItemToAPI(item),
			Status:   &status,
		})
	}
	out.Data = &data
	return out
}

// definitionFromAPI converts the wire definition into the package model.
//
// format_version is set by the server on write (DOC-12): an omitted value, and the zero the
// generated type carries for an omission, both mean FormatVersion. The spec leaves the field
// optional, so a client that follows ent-swagger and omits it is accepted.
func definitionFromAPI(in apigen.DatasetDefinition) Definition {
	formatVersion := FormatVersion
	if in.FormatVersion != nil && *in.FormatVersion != 0 {
		formatVersion = *in.FormatVersion
	}
	out := Definition{
		FormatVersion: formatVersion,
		Data:          make([]DataItem, len(in.Data)),
	}
	if in.Description != nil {
		out.Description = *in.Description
	}
	if in.Metadata != nil {
		out.Metadata = map[string]any(*in.Metadata)
	}
	for i, item := range in.Data {
		out.Data[i] = dataItemFromAPI(item)
	}
	return out
}

// definitionToAPI converts the package model into the wire definition.
func definitionToAPI(in Definition) apigen.DatasetDefinition {
	formatVersion := in.FormatVersion
	if formatVersion == 0 {
		formatVersion = FormatVersion
	}
	out := apigen.DatasetDefinition{
		FormatVersion: &formatVersion,
		Data:          make([]apigen.DataItem, len(in.Data)),
	}
	if in.Description != "" {
		description := in.Description
		out.Description = &description
	}
	if in.Metadata != nil {
		metadata := metadataToAPI(in.Metadata)
		out.Metadata = &metadata
	}
	for i, item := range in.Data {
		out.Data[i] = dataItemToAPI(item)
	}
	return out
}

// metadataToAPI renders metadata in the documented wire shape (DOC-11): every value is an
// array of strings. The input side accepts both a JSON scalar and an array, because the
// published docs use scalars; the output side is always arrays, which is the
// `additionalProperties: {type: array, items: string}` shape of ent-swagger v1.108, so a
// client generated from that spec decodes the response.
func metadataToAPI(in map[string]any) apigen.DatasetMetadata {
	out := make(apigen.DatasetMetadata, len(in))
	for key, value := range in {
		out[key] = metadataValueStrings(value)
	}
	return out
}

// metadataValueStrings flattens one metadata value into the string list of the wire format.
func metadataValueStrings(value any) []string {
	switch typed := value.(type) {
	case nil:
		return []string{}
	case []string:
		return append([]string(nil), typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, element := range typed {
			values = append(values, fmt.Sprint(element))
		}
		return values
	default:
		return []string{fmt.Sprint(typed)}
	}
}

// dataItemFromAPI converts one wire data item. The reserved table/namespace descriptors are
// carried through so that a structural validation problem (or a 501) describes them instead
// of silently dropping them.
func dataItemFromAPI(in apigen.DataItem) DataItem {
	out := DataItem{
		Target:     in.Target,
		Repository: in.Repository,
		Type:       ItemType(in.Type),
		Ref:        SourceRef{Type: RefType(in.Ref.Type), ID: in.Ref.Id},
	}
	if in.Object != nil {
		out.Object = &PathSource{Path: in.Object.Path}
	}
	if in.Prefix != nil {
		out.Prefix = &PathSource{Path: in.Prefix.Path}
	}
	// DOC-13: ent-swagger declares a namespace item's `namespace` member as a string, so the
	// generated type is *string and the package model keeps just the name.
	if in.Namespace != nil {
		namespace := *in.Namespace
		out.Namespace = &namespace
	}
	return out
}

// dataItemToAPI converts one item into the wire shape.
func dataItemToAPI(in DataItem) apigen.DataItem {
	out := apigen.DataItem{
		Target:     in.Target,
		Repository: in.Repository,
		Type:       string(in.Type),
		Ref:        apigen.SourceRef{Type: string(in.Ref.Type), Id: in.Ref.ID},
	}
	if in.Object != nil {
		out.Object = &apigen.PathSource{Path: in.Object.Path}
	}
	if in.Prefix != nil {
		out.Prefix = &apigen.PathSource{Path: in.Prefix.Path}
	}
	if in.Namespace != nil {
		namespace := *in.Namespace
		out.Namespace = &namespace
	}
	return out
}

// definitionDiffToAPI converts a definition diff into the §10.2 DatasetDiff schema.
func definitionDiffToAPI(in *DefinitionDiff) apigen.DatasetDiff {
	out := apigen.DatasetDiff{Data: []apigen.ItemChange{}}
	if in == nil {
		return out
	}
	if in.Description != nil {
		out.Description = &apigen.StringChange{Left: in.Description.From, Right: in.Description.To}
	}
	if len(in.Metadata) > 0 {
		metadata := make([]apigen.MetadataChange, 0, len(in.Metadata))
		for _, change := range in.Metadata {
			metadata = append(metadata, apigen.MetadataChange{
				Type:  string(change.Change),
				Key:   change.Key,
				Left:  metadataValues(change.From),
				Right: metadataValues(change.To),
			})
		}
		out.Metadata = &metadata
	}
	for _, change := range in.Items {
		item := apigen.ItemChange{Type: string(change.Change), Target: change.Target}
		if change.From != nil {
			left := dataItemToAPI(*change.From)
			item.Left = &left
		}
		if change.To != nil {
			right := dataItemToAPI(*change.To)
			item.Right = &right
		}
		out.Data = append(out.Data, item)
	}
	return out
}

// metadataValues renders one side of a metadata change as the string list the §10.2
// MetadataChange schema uses. A nil side (added/removed) stays nil.
func metadataValues(value *any) *[]string {
	if value == nil {
		return nil
	}
	values := metadataValueStrings(*value)
	return &values
}

// dataDiffToAPI converts one page of a data diff into the DataDiffList schema (§10.2).
func dataDiffToAPI(in *DataDiff) apigen.DataDiffList {
	if in == nil {
		return apigen.DataDiffList{Results: []apigen.DataDiffEntry{}}
	}
	out := apigen.DataDiffList{
		Results: make([]apigen.DataDiffEntry, 0, len(in.Entries)),
		Pagination: apigen.Pagination{
			HasMore:    in.HasMore,
			NextOffset: in.NextOffset,
			Results:    len(in.Entries),
			MaxPerPage: in.MaxPerPage,
		},
	}
	for _, entry := range in.Entries {
		size := entry.SizeBytes
		out.Results = append(out.Results, apigen.DataDiffEntry{
			Path:      entry.Path,
			Type:      string(entry.Type),
			SizeBytes: &size,
		})
	}
	return out
}

// statsToAPI renders the item stats of §10.2.
func statsToAPI(in ItemStats) apigen.DatasetStats {
	out := apigen.DatasetStats{
		Objects: in.Objects,
		Bytes:   in.Bytes,
		Items:   make([]apigen.ItemStats, 0, len(in.Items)),
	}
	for _, item := range in.Items {
		out.Items = append(out.Items, apigen.ItemStats{
			Target:  item.Target,
			Objects: item.Objects,
			Bytes:   item.Bytes,
		})
	}
	return out
}

// versionToAPI renders a VersionRecord as the §10.2 DatasetVersion schema.
func versionToAPI(rec *VersionRecord) apigen.DatasetVersion {
	commitID := rec.CommitID
	out := apigen.DatasetVersion{
		Id:           fmt.Sprintf("v%d", rec.Version),
		CommitId:     &commitID,
		CreationDate: rec.PublishedAt.Unix(),
		Author:       rec.Publisher,
	}
	if rec.Message != "" {
		message := rec.Message
		out.Message = &message
	}
	if rec.ProposalID != "" {
		proposalID := rec.ProposalID
		out.ProposalId = &proposalID
	}
	if !definitionIsZero(rec.Definition) {
		definition := definitionToAPI(rec.Definition)
		out.Definition = &definition
	}
	stats := statsToAPI(rec.Stats)
	out.Stats = &stats
	return out
}

// taskToAPI renders a TaskRecord as the §10.2 Task schema.
func taskToAPI(rec *TaskRecord) apigen.Task {
	out := apigen.Task{
		Id:        rec.ID,
		Status:    rec.Status,
		CreatedAt: rec.CreatedAt.UnixMilli(),
		UpdatedAt: rec.UpdatedAt.UnixMilli(),
	}
	if rec.Version > 0 {
		version := rec.Version
		out.Version = &version
	}
	if rec.Error != nil {
		// SEC-8: the record already holds only the sanitised code and message; the API never
		// adds a raw error.
		out.Error = &apigen.TaskError{Code: rec.Error.Code, Message: rec.Error.Message}
	}
	return out
}

// versionConflictBody is the 409 body of §5.4 and §5.6: the core Error shape plus the current
// latest_version, so a client whose expected_version lost - or whose proposal base went
// stale - can retry with the right value instead of re-reading the dataset.
type versionConflictBody struct {
	Message       string `json:"message"`
	LatestVersion int    `json:"latest_version"`
}

// staleVersionError is a 409 that carries the current latest_version in its body: the
// expected_version mismatch of §5.4 (VersionConflictError) and the base version of a
// proposal that must be rebased first (§5.6, RebaseRequiredError).
type staleVersionError interface {
	error
	latestVersion() int
}

// writeVersionConflict writes the stale-version 409 and reports whether it did.
func writeVersionConflict(w http.ResponseWriter, r *http.Request, err error) bool {
	var conflict staleVersionError
	if !errors.As(err, &conflict) {
		return false
	}
	httputil.WriteAPIResponse(w, r, http.StatusConflict, versionConflictBody{
		Message:       err.Error(),
		LatestVersion: conflict.latestVersion(),
	})
	return true
}

// grantToAPI renders one access grant (§10.2).
func grantToAPI(grant *GrantRecord) apigen.AccessGrant {
	grantedAt := grant.GrantedAt.UnixMilli()
	grantedBy := grant.GrantedBy
	return apigen.AccessGrant{
		PrincipalType: grant.PrincipalType,
		PrincipalId:   grant.PrincipalID,
		GrantedBy:     &grantedBy,
		GrantedAt:     &grantedAt,
	}
}

// metadataKeyToAPI renders a metadata key definition as the §10.2 MetadataKey schema.
func metadataKeyToAPI(key MetadataKey) apigen.MetadataKey {
	out := apigen.MetadataKey{
		Name:          key.Name,
		Type:          key.Type,
		Required:      key.Required,
		AllowMultiple: key.AllowMultiple,
		CreatedAt:     key.CreatedAt.UnixMilli(),
		CreatedBy:     key.CreatedBy,
	}
	if len(key.AllowedValues) > 0 {
		allowed := append([]string(nil), key.AllowedValues...)
		out.AllowedValues = &allowed
	}
	if key.Description != "" {
		description := key.Description
		out.Description = &description
	}
	return out
}

// metadataKeyFromCreateAPI converts a create request into the package model. The
// optional fields default to false/empty, which is the full definition.
func metadataKeyFromCreateAPI(in apigen.MetadataKeyCreateRequest) MetadataKey {
	out := MetadataKey{Name: in.Name, Type: in.Type}
	if in.Required != nil {
		out.Required = *in.Required
	}
	if in.AllowMultiple != nil {
		out.AllowMultiple = *in.AllowMultiple
	}
	if in.AllowedValues != nil {
		out.AllowedValues = append([]string(nil), (*in.AllowedValues)...)
	}
	if in.Description != nil {
		out.Description = *in.Description
	}
	return out
}

// metadataKeyFromUpdateAPI converts a full-replacement update into the package model.
// The name travels in the path, not the body.
func metadataKeyFromUpdateAPI(name string, in apigen.MetadataKeyUpdateRequest) MetadataKey {
	out := MetadataKey{Name: name, Type: in.Type}
	if in.Required != nil {
		out.Required = *in.Required
	}
	if in.AllowMultiple != nil {
		out.AllowMultiple = *in.AllowMultiple
	}
	if in.AllowedValues != nil {
		out.AllowedValues = append([]string(nil), (*in.AllowedValues)...)
	}
	if in.Description != nil {
		out.Description = *in.Description
	}
	return out
}

// pullRequestToAPI renders a pull-request view as the §10.2 DatasetPullRequest schema. The
// diffs of the detail read are optional: a list row has none, and a diff the service could
// not compute is left out rather than failing the read (see Service.proposalView).
func pullRequestToAPI(view *ProposalView) apigen.DatasetPullRequest {
	rec := view.Record
	previewBranch := previewBranchOf(rec)
	out := apigen.DatasetPullRequest{
		Id:            rec.ID,
		Dataset:       rec.Dataset,
		BaseVersion:   rec.BaseVersion,
		LatestVersion: view.LatestVersion,
		Stale:         view.Stale,
		Conflicted:    view.Conflicted,
		SourceRef:     PullRequestRef(rec.ID),
		Title:         rec.Title,
		Author:        rec.Author,
		Status:        rec.Status,
		CreationDate:  rec.CreatedAt.Unix(),
		PreviewBranch: &previewBranch,
	}
	if rec.Description != "" {
		description := rec.Description
		out.Description = &description
	}
	if rec.ClosedAt != nil {
		closedDate := rec.ClosedAt.Unix()
		out.ClosedDate = &closedDate
	}
	if rec.MergedVersion > 0 {
		mergedVersion := rec.MergedVersion
		out.MergedVersion = &mergedVersion
	}
	if !definitionIsZero(rec.Definition) {
		definition := definitionToAPI(rec.Definition)
		out.Definition = &definition
	}
	if view.Diff != nil {
		diff := definitionDiffToAPI(view.Diff)
		out.Diff = &diff
	}
	if view.DataDiff != nil {
		dataDiff := dataDiffToAPI(view.DataDiff)
		out.DataDiff = &dataDiff
	}
	return out
}

// pullRequestListToAPI renders one page of pull requests.
func pullRequestListToAPI(views []*ProposalView, hasMore bool, nextOffset string, maxPerPage int) apigen.DatasetPullRequestList {
	body := apigen.DatasetPullRequestList{
		Results: make([]apigen.DatasetPullRequest, 0, len(views)),
		Pagination: apigen.Pagination{
			HasMore:    hasMore,
			NextOffset: nextOffset,
			Results:    len(views),
			MaxPerPage: maxPerPage,
		},
	}
	for _, view := range views {
		body.Results = append(body.Results, pullRequestToAPI(view))
	}
	return body
}

// metadataFilters extracts the dynamic metadata.<key>=<value> query parameters of §10.1.
// OpenAPI 3.0 cannot declare dynamic parameter names, so the request validator passes them
// through and the handler reads them from the raw query.
func metadataFilters(r *http.Request) map[string]string {
	query := r.URL.Query()
	filters := make(map[string]string)
	for key, values := range query {
		name, ok := cutMetadataFilterPrefix(key)
		if !ok || name == "" || len(values) == 0 {
			continue
		}
		filters[name] = values[0]
	}
	if len(filters) == 0 {
		return nil
	}
	return filters
}

// cutMetadataFilterPrefix reports whether key is a metadata filter and returns the metadata
// key part.
func cutMetadataFilterPrefix(key string) (string, bool) {
	const prefix = MetadataFilterPrefix
	if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
		return "", false
	}
	return key[len(prefix):], true
}

// valueOrZero returns the pointed-to value of an optional generated parameter, or the zero
// value when it is absent.
func valueOrZero[T any](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}
