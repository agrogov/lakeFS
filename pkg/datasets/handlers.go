package datasets

// handlers.go implements the datasets control-plane handlers of DATASETS_DESIGN.md §10 on
// top of the Service. It maps the generated apigen types onto the package types, builds the
// authorization caller of each request (§9.1) and maps the typed service errors onto the
// status codes of §10.4:
//
//	400  validation / source problems, with one entry per problem in the errors array
//	403  a permission denial, naming the data item when it was a source check (§9.3)
//	404  unknown dataset, version record or grant
//	409  name taken by a repository or a dataset, or a lost publish race (§5.4)
//	422  a source repository on a different storage ID (§5.2 rule 4)
//	500  a failure to decide (never a denial) or an internal error
//	501  datasets disabled, a reserved item type, or an operation of a later phase
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
)

// ErrNotImplemented is wrapped by every datasets control-plane handler that has no business
// logic yet. The HTTP answer is always 501 with the datasets error body (§10.4).
var ErrNotImplemented = errors.New("not implemented")

// MetadataFilterPrefix is the query-parameter prefix of the metadata filters of
// GET /datasets (§10.1): metadata.<key>=<value>.
const MetadataFilterPrefix = "metadata."

// server implements apigen.ServerInterface.
//
// The in-scope operations of this phase (create, get, list, delete, validate and the access
// grants, §10.1) delegate to Service. The remaining operations still answer 501 with
// ErrNotImplemented, so a client gets a precise answer instead of a routing error.
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
	case errors.Is(err, ErrDatasetNotFound) || isNotFound(err):
		httputil.WriteAPIError(w, r, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrNameTaken), errors.Is(err, ErrDatasetConflict), errors.Is(err, kv.ErrPredicateFailed):
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

func (s *server) ListVersions(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.ListVersionsParams) {
	notImplemented(w, r, "ListVersions")
}

func (s *server) PublishVersion(w http.ResponseWriter, r *http.Request, _ apigen.PublishVersionJSONRequestBody, _ apigen.DatasetName) {
	notImplemented(w, r, "PublishVersion")
}

func (s *server) GetVersion(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.VersionRef) {
	notImplemented(w, r, "GetVersion")
}

func (s *server) DiffRefs(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.LeftRef, _ apigen.RightRef, _ apigen.DiffRefsParams) {
	notImplemented(w, r, "DiffRefs")
}

func (s *server) ListProposals(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.ListProposalsParams) {
	notImplemented(w, r, "ListProposals")
}

func (s *server) CreateProposal(w http.ResponseWriter, r *http.Request, _ apigen.CreateProposalJSONRequestBody, _ apigen.DatasetName) {
	notImplemented(w, r, "CreateProposal")
}

func (s *server) GetProposal(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.ProposalID) {
	notImplemented(w, r, "GetProposal")
}

func (s *server) UpdateProposal(w http.ResponseWriter, r *http.Request, _ apigen.UpdateProposalJSONRequestBody, _ apigen.DatasetName, _ apigen.ProposalID) {
	notImplemented(w, r, "UpdateProposal")
}

func (s *server) MergeProposal(w http.ResponseWriter, r *http.Request, _ apigen.MergeProposalJSONRequestBody, _ apigen.DatasetName, _ apigen.ProposalID) {
	notImplemented(w, r, "MergeProposal")
}

func (s *server) GetTask(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.TaskID) {
	notImplemented(w, r, "GetTask")
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
