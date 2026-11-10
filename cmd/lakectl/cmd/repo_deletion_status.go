package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

const (
	// Repository deletion states, as the API reports them (R14).
	repositoryDeletionStateDone   = "done"
	repositoryDeletionStateFailed = "failed"

	repositoryDeletionTemplate = `repository: {{.Repository}}
storage namespace: {{.StorageNamespace}}
state: {{.State}}
objects deleted: {{.ObjectsDeleted}}
requested by: {{.RequestedBy}}
requested at: {{.RequestedAt | date}}
updated at: {{.UpdatedAt | date}}
attempts: {{.Attempts}}
{{if .LastError}}last error: {{.LastError}}
{{end}}`
)

// repoDeletionStatusCmd prints the storage deletion record of a repository (R14, R23).
var repoDeletionStatusCmd = &cobra.Command{
	Use:               "deletion-status <repository URI>",
	Short:             "Show the status of a repository's storage deletion",
	Example:           "lakectl repo deletion-status " + myRepoExample,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		resp, err := clt.GetRepositoryDeletionWithResponse(cmd.Context(), u.Repository)
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
		if resp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		writeRepositoryDeletion(resp.JSON200)
	},
}

// repositoryDeletionView is the printed form of the record: LastError is a pointer in the API and
// is printed only when set, and the timestamps are unix seconds in the API (R14).
type repositoryDeletionView struct {
	Repository       string
	StorageNamespace string
	State            string
	ObjectsDeleted   int64
	RequestedBy      string
	RequestedAt      int64
	UpdatedAt        int64
	Attempts         int
	LastError        string
}

func writeRepositoryDeletion(deletion *apigen.RepositoryDeletion) {
	Write(repositoryDeletionTemplate, repositoryDeletionView{
		Repository:       deletion.Repository,
		StorageNamespace: deletion.StorageNamespace,
		State:            deletion.State,
		ObjectsDeleted:   deletion.ObjectsDeleted,
		RequestedBy:      deletion.RequestedBy,
		RequestedAt:      deletion.RequestedAt,
		UpdatedAt:        deletion.UpdatedAt,
		Attempts:         deletion.Attempts,
		LastError:        apiutil.Value(deletion.LastError),
	})
}

//nolint:gochecknoinits
func init() {
	repoCmd.AddCommand(repoDeletionStatusCmd)
}
