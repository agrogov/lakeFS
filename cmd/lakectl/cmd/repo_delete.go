package cmd

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
)

const (
	repoDeleteStorageFlagName = "delete-storage"
	repoDeleteWaitFlagName    = "wait"
	repoDeleteForceFlagName   = "force"

	// repoDeletionPollInterval is how often `--wait` reads the deletion status (R23).
	repoDeletionPollInterval = 2 * time.Second
)

// repoDeleteCmd represents the delete repo command
// lakectl delete lakefs://myrepo
var repoDeleteCmd = &cobra.Command{
	Use:               "delete <repository URI>",
	Short:             "Delete existing repository",
	Example:           "lakectl repo delete " + myRepoExample,
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: ValidArgsRepository,
	Run: func(cmd *cobra.Command, args []string) {
		clt := getClient()
		u := MustParseRepoURI("repository URI", args[0])
		flags := cmd.Flags()
		deleteStorage := Must(flags.GetBool(repoDeleteStorageFlagName))
		wait := Must(flags.GetBool(repoDeleteWaitFlagName))
		force := Must(flags.GetBool(repoDeleteForceFlagName))
		if wait && !deleteStorage {
			DieFmt("--%s is only meaningful with --%s", repoDeleteWaitFlagName, repoDeleteStorageFlagName)
		}

		fmt.Println("Repository:", u)
		if deleteStorage {
			deleteRepositoryStorage(cmd, clt, u.Repository, force, wait)
			return
		}
		deleteRepository(cmd, clt, u.Repository, force)
	},
}

// deleteRepository deletes the repository's metadata only.  This is the default and its output
// and status codes are unchanged (R2, A6).
func deleteRepository(cmd *cobra.Command, clt *apigen.ClientWithResponses, repository string, force bool) {
	confirmation, err := Confirm(cmd.Flags(), "Are you sure you want to delete repository: "+repository)
	if err != nil || !confirmation {
		DieFmt("Delete Repository '%s' aborted\n", repository)
	}
	params := &apigen.DeleteRepositoryParams{}
	if force {
		params.Force = apiutil.Ptr(true)
	}
	resp, err := clt.DeleteRepositoryWithResponse(cmd.Context(), repository, params)
	DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusNoContent)
	fmt.Printf("Repository '%s' deleted\n", repository)
}

// deleteRepositoryStorage deletes the repository, its metadata and every object under its
// storage namespace (R23): the confirmation names the namespace, the server answers 202, and
// `--wait` follows the deletion record until it is done or failed.
func deleteRepositoryStorage(cmd *cobra.Command, clt *apigen.ClientWithResponses, repository string, force, wait bool) {
	ctx := cmd.Context()

	// The namespace is what the prompt is about, and it is the only thing that identifies the
	// storage a repeated request resumes (R15), so it is read before anything else.  The warning
	// is printed even when `--yes` skips the prompt: entries in other repositories can reference
	// objects under this namespace and break when they are deleted (L1).
	storageNamespace := repositoryStorageNamespace(ctx, clt, repository)
	Warning(fmt.Sprintf("Deleting repository '%s' also deletes every object under '%s'. Objects referenced from other repositories will break.", repository, storageNamespace))

	question := fmt.Sprintf("Are you sure you want to delete repository %s and every object under %s", repository, storageNamespace)
	confirmation, err := Confirm(cmd.Flags(), question)
	if err != nil || !confirmation {
		DieFmt("Delete Repository '%s' aborted\n", repository)
	}

	params := &apigen.DeleteRepositoryParams{DeleteStorage: apiutil.Ptr(true)}
	if force {
		params.Force = apiutil.Ptr(true)
	}
	resp, err := clt.DeleteRepositoryWithResponse(ctx, repository, params)
	if resp != nil && resp.StatusCode() == http.StatusNoContent {
		// No record and no error: there is no deletion left to wait for (design 4.1).
		fmt.Printf("Repository '%s' has no pending storage deletion\n", repository)
		return
	}
	DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusAccepted)
	if resp.JSON202 == nil {
		Die("Bad response from server", 1)
	}
	fmt.Printf("Repository '%s' storage deletion requested\n", repository)
	if wait {
		waitForRepositoryDeletion(ctx, clt, repository)
	}
}

// repositoryStorageNamespace returns the storage namespace shown in the confirmation prompt.
// Once the metadata was deleted the repository record is gone and the deletion record is the only
// place the namespace is left, which is also the resume path of a repeated request (R15).
func repositoryStorageNamespace(ctx context.Context, clt *apigen.ClientWithResponses, repository string) string {
	resp, err := clt.GetRepositoryWithResponse(ctx, repository)
	if err != nil {
		DieErr(err)
	}
	if resp.StatusCode() == http.StatusNotFound {
		deletionResp, err := clt.GetRepositoryDeletionWithResponse(ctx, repository)
		dieOnResponseError(deletionResp, err)
		if deletionResp.JSON200 == nil {
			Die("Bad response from server", 1)
		}
		return deletionResp.JSON200.StorageNamespace
	}
	dieOnResponseError(resp, err)
	if resp.JSON200 == nil {
		Die("Bad response from server", 1)
	}
	return resp.JSON200.StorageNamespace
}

// waitForRepositoryDeletion polls the deletion record until it is done or failed (R14, R23).
func waitForRepositoryDeletion(ctx context.Context, clt *apigen.ClientWithResponses, repository string) {
	ticker := time.NewTicker(repoDeletionPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			DieErr(ctx.Err())
		case <-ticker.C:
			resp, err := clt.GetRepositoryDeletionWithResponse(ctx, repository)
			DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)
			if resp.JSON200 == nil {
				Die("Bad response from server", 1)
			}
			deletion := resp.JSON200
			WriteIfVerbose("state: {{.State}}, objects deleted: {{.ObjectsDeleted}}\n", deletion)
			switch deletion.State {
			case repositoryDeletionStateDone:
				writeRepositoryDeletion(deletion)
				return
			case repositoryDeletionStateFailed:
				writeRepositoryDeletion(deletion)
				DieFmt("Repository '%s' storage deletion failed: %s", repository, apiutil.Value(deletion.LastError))
			}
		}
	}
}

//nolint:gochecknoinits
func init() {
	AssignAutoConfirmFlag(repoDeleteCmd.Flags())
	repoDeleteCmd.Flags().Bool(repoDeleteStorageFlagName, false, "delete every object under the repository's storage namespace as well")
	repoDeleteCmd.Flags().Bool(repoDeleteWaitFlagName, false, "wait for the storage deletion to finish (requires --"+repoDeleteStorageFlagName+")")
	repoDeleteCmd.Flags().Bool(repoDeleteForceFlagName, false, "delete the repository even if it is read-only")

	repoCmd.AddCommand(repoDeleteCmd)
}
