package datasets

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/treeverse/lakefs/pkg/datasets/apigen"
	"github.com/treeverse/lakefs/pkg/httputil"
)

// ErrNotImplemented is wrapped by every datasets control-plane handler that has no business
// logic yet. The HTTP answer is always 501 with the datasets error body (§10.4).
var ErrNotImplemented = errors.New("not implemented")

// stubServer implements apigen.ServerInterface so the control plane compiles and every route
// is authenticated, validated and answered before the service lands.
//
// Each handler below is a placeholder: it validates nothing beyond the request validator and
// returns 501. The status codes each operation must eventually answer are declared in
// api/datasets.yml and summarised in DATASETS_DESIGN.md §10.4. When the service lands
// (service.go, §4.3), the stubs are replaced one by one and this type is deleted.
//
// Authentication is already enforced in front of these handlers by api.GenericAuthMiddleware
// (see RoutesBuilder), so an unauthenticated request never reaches them.
type stubServer struct{}

// newStubServer returns the not-implemented implementation of the generated server interface.
func newStubServer() *stubServer {
	return &stubServer{}
}

// notImplemented writes the 501 answer for an operation the stub does not implement yet.
func notImplemented(w http.ResponseWriter, r *http.Request, operation string) {
	httputil.WriteAPIError(w, r, http.StatusNotImplemented,
		fmt.Errorf("%w: %s (datasets control plane stub)", ErrNotImplemented, operation))
}

func (s *stubServer) ListDatasets(w http.ResponseWriter, r *http.Request, _ apigen.ListDatasetsParams) {
	notImplemented(w, r, "ListDatasets")
}

func (s *stubServer) CreateDataset(w http.ResponseWriter, r *http.Request, _ apigen.CreateDatasetJSONRequestBody) {
	notImplemented(w, r, "CreateDataset")
}

func (s *stubServer) GetDatasetsConfig(w http.ResponseWriter, r *http.Request) {
	notImplemented(w, r, "GetDatasetsConfig")
}

func (s *stubServer) ListMetadataKeys(w http.ResponseWriter, r *http.Request, _ apigen.ListMetadataKeysParams) {
	notImplemented(w, r, "ListMetadataKeys")
}

func (s *stubServer) CreateMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.CreateMetadataKeyJSONRequestBody) {
	notImplemented(w, r, "CreateMetadataKey")
}

func (s *stubServer) GetMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.MetadataKeyName) {
	notImplemented(w, r, "GetMetadataKey")
}

func (s *stubServer) UpdateMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.UpdateMetadataKeyJSONRequestBody, _ apigen.MetadataKeyName) {
	notImplemented(w, r, "UpdateMetadataKey")
}

func (s *stubServer) DeleteMetadataKey(w http.ResponseWriter, r *http.Request, _ apigen.MetadataKeyName) {
	notImplemented(w, r, "DeleteMetadataKey")
}

func (s *stubServer) GetDataset(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName) {
	notImplemented(w, r, "GetDataset")
}

func (s *stubServer) DeleteDataset(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName) {
	notImplemented(w, r, "DeleteDataset")
}

func (s *stubServer) ValidateDataset(w http.ResponseWriter, r *http.Request, _ apigen.ValidateDatasetJSONRequestBody, _ apigen.DatasetName) {
	notImplemented(w, r, "ValidateDataset")
}

func (s *stubServer) ListVersions(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.ListVersionsParams) {
	notImplemented(w, r, "ListVersions")
}

func (s *stubServer) PublishVersion(w http.ResponseWriter, r *http.Request, _ apigen.PublishVersionJSONRequestBody, _ apigen.DatasetName) {
	notImplemented(w, r, "PublishVersion")
}

func (s *stubServer) GetVersion(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.VersionRef) {
	notImplemented(w, r, "GetVersion")
}

func (s *stubServer) DiffRefs(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.LeftRef, _ apigen.RightRef, _ apigen.DiffRefsParams) {
	notImplemented(w, r, "DiffRefs")
}

func (s *stubServer) ListProposals(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.ListProposalsParams) {
	notImplemented(w, r, "ListProposals")
}

func (s *stubServer) CreateProposal(w http.ResponseWriter, r *http.Request, _ apigen.CreateProposalJSONRequestBody, _ apigen.DatasetName) {
	notImplemented(w, r, "CreateProposal")
}

func (s *stubServer) GetProposal(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.ProposalID) {
	notImplemented(w, r, "GetProposal")
}

func (s *stubServer) UpdateProposal(w http.ResponseWriter, r *http.Request, _ apigen.UpdateProposalJSONRequestBody, _ apigen.DatasetName, _ apigen.ProposalID) {
	notImplemented(w, r, "UpdateProposal")
}

func (s *stubServer) MergeProposal(w http.ResponseWriter, r *http.Request, _ apigen.MergeProposalJSONRequestBody, _ apigen.DatasetName, _ apigen.ProposalID) {
	notImplemented(w, r, "MergeProposal")
}

func (s *stubServer) ListAccess(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName) {
	notImplemented(w, r, "ListAccess")
}

func (s *stubServer) GrantAccess(w http.ResponseWriter, r *http.Request, _ apigen.GrantAccessJSONRequestBody, _ apigen.DatasetName) {
	notImplemented(w, r, "GrantAccess")
}

func (s *stubServer) RevokeAccess(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.PrincipalType, _ apigen.PrincipalID) {
	notImplemented(w, r, "RevokeAccess")
}

func (s *stubServer) GetTask(w http.ResponseWriter, r *http.Request, _ apigen.DatasetName, _ apigen.TaskID) {
	notImplemented(w, r, "GetTask")
}
