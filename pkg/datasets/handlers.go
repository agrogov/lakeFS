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

// ErrNotImplemented is wrapped by every datasets control-plane handler that has no business
// logic yet. The HTTP answer is always 501 with the datasets error body (§10.4).
var ErrNotImplemented = errors.New("not implemented")

// MetadataFilterPrefix is the query-parameter prefix of the metadata filters of
// GET /datasets (§10.1): metadata.<key>=<value>.
const MetadataFilterPrefix = "metadata."

// server implements apigen.ServerInterface.
//
// The in-scope operations delegate to Service: create, get, list, delete, validate, the
// access grants (§10.1), publish and the version reads, the version diff of §10.3 and the
// proposals of §5.6. The remaining operations - metadata-key CRUD and GET /datasets/config -
// still answer 501 with ErrNotImplemented, so a client gets a precise answer instead of a
// routing error.
type server struct {
	svc      *Service
	clientIP *httputil.ClientIPResolver
}

// newServer wraps a Service in the generated server interface. clientIP may be nil, in which
// case the safe default of trusting no proxy applies to authorization conditions.
func newServer(svc *Service, clientIP *httputil.ClientIPResolver) *server {
	return &server{svc: svc, clientIP: clientIP}
}

// notImplemented writes the 501 answer for an operation that a later phase implements.
func notImplemented(w http.ResponseWriter, r *http.Request, operation string) {
	httputil.WriteAPIError(w, r, http.StatusNotImplemented,
		fmt.Errorf("%w: %s", ErrNotImplemented, operation))
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
func writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		permission    *PermissionError
		authorization *AuthorizationError
		validation    *ValidationError
		source        *SourceError
	)
	switch {
	case errors.As(err, &permission):
		httputil.WriteAPIError(w, r, http.StatusForbidden, permission.Error())
	case errors.As(err, &authorization):
		httputil.WriteAPIError(w, r, http.StatusInternalServerError, authorization.Error())
	case errors.As(err, &validation):
		writeErrorResponse(w, r, http.StatusBadRequest, errorResponse{
			Message: validation.Error(),
			Errors:  problemDetails(validation.Problems),
		})
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
	case errors.Is(err, ErrDatasetNotFound), errors.Is(err, ErrVersionNotFound), errors.Is(err, ErrTaskNotFound), errors.Is(err, ErrRefNotFound), errors.Is(err, ErrProposalNotFound), isNotFound(err):
		httputil.WriteAPIError(w, r, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrNameTaken), errors.Is(err, ErrDatasetConflict), errors.Is(err, ErrPublishInProgress), errors.Is(err, ErrProposalNotOpen), errors.Is(err, kv.ErrPredicateFailed):
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
		writeServiceError(w, r, err)
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

func (s *server) CreateDataset(w http.ResponseWriter, r *http.Request, body apigen.CreateDatasetJSONRequestBody) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	req := CreateRequest{
		Name:       body.Name,
		Definition: definitionFromAPI(body.Definition),
		Publish:    body.Publish != nil,
	}
	if body.Publish != nil && body.Publish.Message != nil {
		req.Message = *body.Publish.Message
	}
	view, err := s.svc.Create(r.Context(), caller, req)
	if err != nil {
		writeServiceError(w, r, err)
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
		writeServiceError(w, r, err)
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
		writeServiceError(w, r, err)
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
		writeServiceError(w, r, err)
		return
	}
	stats := statsToAPI(result.Stats)
	httputil.WriteAPIResponse(w, r, http.StatusOK, apigen.DatasetValidateResponse{
		Definition: definitionToAPI(result.Definition),
		Stats:      &stats,
	})
}

func (s *server) ListAccess(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	grants, err := s.svc.ListAccess(r.Context(), caller, string(dataset))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	body := apigen.AccessGrantList{
		Results: make([]apigen.AccessGrant, 0, len(grants)),
		Pagination: apigen.Pagination{
			Results:    len(grants),
			MaxPerPage: len(grants),
		},
	}
	for _, grant := range grants {
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
		writeServiceError(w, r, err)
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
		writeServiceError(w, r, err)
		return
	}
	logAction(r, "RevokeAccess", string(dataset), 0)
	httputil.WriteAPIResponse(w, r, http.StatusNoContent, nil)
}

// --- generated server interface: operations of later phases ---

func (s *server) GetDatasetsConfig(w http.ResponseWriter, r *http.Request) {
	notImplemented(w, r, "GetDatasetsConfig")
}

func (s *server) ListMetadataKeys(w http.ResponseWriter, r *http.Request, _ apigen.ListMetadataKeysParams) {
	notImplemented(w, r, "ListMetadataKeys")
}

func (s *server) CreateMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.CreateMetadataKeyJSONRequestBody) {
	notImplemented(w, r, "CreateMetadataKey")
}

func (s *server) GetMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.MetadataKeyName) {
	notImplemented(w, r, "GetMetadataKey")
}

func (s *server) UpdateMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.UpdateMetadataKeyJSONRequestBody, _ apigen.MetadataKeyName) {
	notImplemented(w, r, "UpdateMetadataKey")
}

func (s *server) DeleteMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.MetadataKeyName) {
	notImplemented(w, r, "DeleteMetadataKey")
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
		writeServiceError(w, r, err)
		return
	}
	body := apigen.VersionList{
		Results: make([]apigen.Version, 0, len(result.Versions)),
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
		writeServiceError(w, r, err)
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
		writeServiceError(w, r, err)
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
		writeServiceError(w, r, err)
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
		writeServiceError(w, r, err)
		return
	}
	if result.Type == DiffTypeData {
		httputil.WriteAPIResponse(w, r, http.StatusOK, dataDiffToAPI(result.Data))
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, definitionDiffToAPI(result.Definition))
}

func (s *server) ListProposals(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, params apigen.ListProposalsParams) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	result, err := s.svc.ListProposals(r.Context(), caller, string(dataset),
		valueOrZero(params.Status), string(valueOrZero(params.After)), int(valueOrZero(params.Amount)))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	body := apigen.ProposalList{
		Results: make([]apigen.Proposal, 0, len(result.Proposals)),
		Pagination: apigen.Pagination{
			HasMore:    result.HasMore,
			NextOffset: result.NextOffset,
			Results:    len(result.Proposals),
			MaxPerPage: result.MaxPerPage,
		},
	}
	for _, proposal := range result.Proposals {
		// The list endpoint returns the records without diffs: §10.1 puts the diff summary
		// on the detail read, and a data diff per row would be a diff per row of the page.
		body.Results = append(body.Results, proposalToAPI(&ProposalView{Record: proposal}))
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, body)
}

func (s *server) CreateProposal(w http.ResponseWriter, r *http.Request, body apigen.CreateProposalJSONRequestBody, dataset apigen.DatasetName) {
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
		writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusCreated, proposalToAPI(view))
}

func (s *server) GetProposal(w http.ResponseWriter, r *http.Request, dataset apigen.DatasetName, id apigen.ProposalID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	view, err := s.svc.GetProposal(r.Context(), caller, string(dataset), string(id))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, proposalToAPI(view))
}

func (s *server) UpdateProposal(w http.ResponseWriter, r *http.Request, body apigen.UpdateProposalJSONRequestBody, dataset apigen.DatasetName, id apigen.ProposalID) {
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
	view, err := s.svc.UpdateProposal(r.Context(), caller, string(dataset), string(id), req)
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusOK, proposalToAPI(view))
}

func (s *server) MergeProposal(w http.ResponseWriter, r *http.Request, body apigen.MergeProposalJSONRequestBody, dataset apigen.DatasetName, id apigen.ProposalID) {
	caller, err := s.caller(r)
	if err != nil {
		writeUnauthenticated(w, r)
		return
	}
	version, err := s.svc.MergeProposal(r.Context(), caller, string(dataset), string(id), MergeProposalRequest{
		Message: valueOrZero(body.Message),
	})
	if err != nil {
		// §5.6: a stale proposal base is the 409 whose body carries latest_version, the same
		// shape as a lost expected_version check (§5.4).
		if writeVersionConflict(w, r, err) {
			return
		}
		writeServiceError(w, r, err)
		return
	}
	httputil.WriteAPIResponse(w, r, http.StatusCreated, versionToAPI(version))
}

// --- API type conversions (§10.2) ---

// datasetToAPI renders a dataset view as the §10.2 Dataset schema.
func datasetToAPI(view *DatasetView) apigen.Dataset {
	out := apigen.Dataset{
		Name:          view.Record.Name,
		CreatedAt:     view.Record.CreatedAt.UnixMilli(),
		CreatedBy:     view.Record.CreatedBy,
		LatestVersion: view.Record.LatestVersion,
		State:         view.Record.State,
	}
	if view.Record.Description != "" {
		description := view.Record.Description
		out.Description = &description
	}
	if view.Version != nil {
		definition := definitionToAPI(view.Version.Definition)
		stats := statsToAPI(view.Version.Stats)
		out.Definition = &definition
		out.Stats = &stats
	}
	return out
}

// definitionFromAPI converts the wire definition into the package model.
func definitionFromAPI(in apigen.DatasetDefinition) Definition {
	out := Definition{
		FormatVersion: in.FormatVersion,
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
	out := apigen.DatasetDefinition{
		FormatVersion: in.FormatVersion,
		Data:          make([]apigen.DataItem, len(in.Data)),
	}
	if in.Description != "" {
		description := in.Description
		out.Description = &description
	}
	if in.Metadata != nil {
		metadata := apigen.DatasetMetadata(in.Metadata)
		out.Metadata = &metadata
	}
	for i, item := range in.Data {
		out.Data[i] = dataItemToAPI(item)
	}
	return out
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
	return out
}

// definitionDiffToAPI converts a definition diff into the DefinitionDiff schema (§10.2).
func definitionDiffToAPI(in *DefinitionDiff) apigen.DefinitionDiff {
	out := apigen.DefinitionDiff{Items: []apigen.ItemChange{}}
	if in == nil {
		return out
	}
	if in.Description != nil {
		out.Description = &apigen.StringChange{From: in.Description.From, To: in.Description.To}
	}
	if len(in.Metadata) > 0 {
		metadata := make([]apigen.MetadataChange, 0, len(in.Metadata))
		for _, change := range in.Metadata {
			metadata = append(metadata, apigen.MetadataChange{
				Key:    change.Key,
				Change: string(change.Change),
				From:   change.From,
				To:     change.To,
			})
		}
		out.Metadata = &metadata
	}
	for _, change := range in.Items {
		item := apigen.ItemChange{Target: change.Target, Change: string(change.Change)}
		if change.From != nil {
			from := dataItemToAPI(*change.From)
			item.From = &from
		}
		if change.To != nil {
			to := dataItemToAPI(*change.To)
			item.To = &to
		}
		out.Items = append(out.Items, item)
	}
	return out
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

// versionToAPI renders a VersionRecord as the §10.2 Version schema.
func versionToAPI(rec *VersionRecord) apigen.Version {
	out := apigen.Version{
		Version:     rec.Version,
		CommitId:    rec.CommitID,
		PublishedAt: rec.PublishedAt.UnixMilli(),
		Publisher:   rec.Publisher,
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
	if rec.Error != "" {
		message := rec.Error
		out.Error = &message
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

// proposalToAPI renders a proposal view as the §10.2 Proposal schema. The diffs of the
// detail read are optional: a list row has none, and a diff the service could not compute
// is left out rather than failing the read (see Service.proposalView).
func proposalToAPI(view *ProposalView) apigen.Proposal {
	rec := view.Record
	out := apigen.Proposal{
		Id:            rec.ID,
		Dataset:       rec.Dataset,
		BaseVersion:   rec.BaseVersion,
		Title:         rec.Title,
		Author:        rec.Author,
		Status:        rec.Status,
		CreatedAt:     rec.CreatedAt.UnixMilli(),
		PreviewBranch: rec.PreviewBranch,
	}
	if rec.Description != "" {
		description := rec.Description
		out.Description = &description
	}
	if rec.ClosedAt != nil {
		closedAt := rec.ClosedAt.UnixMilli()
		out.ClosedAt = &closedAt
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
