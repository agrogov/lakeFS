package cmd

import (
	"net/http"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// TestRootCommandTree pins the command tree of DATASETS_DESIGN.md §12.1, so a command cannot
// disappear from the plugin without a test failure.
func TestRootCommandTree(t *testing.T) {
	root := NewRoot()
	require.Equal(t, CommandName, root.Name())

	// commandPaths maps each command tree of §12.1 to the subcommands it must provide.
	commandPaths := map[string][]string{
		"datasets":               {"list", "show", "create", "publish", "validate", "versions", "diff", "delete", "proposals", "access", "metadata-keys"},
		"datasets proposals":     {"list", "show", "create", "merge", "close"},
		"datasets access":        {"list", "grant", "revoke"},
		"datasets metadata-keys": {"list", "create", "delete"},
	}
	for path, children := range commandPaths {
		args := splitPath(path)
		// The datasets command is the tree's root; the other paths are its subcommands.
		command := root
		if len(args) > 1 {
			var err error
			command, _, err = root.Find(args[1:])
			require.NoErrorf(t, err, "command %q is missing", path)
		}
		require.Equalf(t, args[len(args)-1], command.Name(), "command %q resolved to %q", path, command.Name())

		names := make([]string, 0, len(command.Commands()))
		for _, child := range command.Commands() {
			names = append(names, child.Name())
		}
		for _, child := range children {
			require.Containsf(t, names, child, "%q is missing subcommand %q", path, child)
		}
	}
}

// splitPath splits a command path on spaces.
func splitPath(path string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(path); i++ {
		if i == len(path) || path[i] == ' ' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	return parts
}

// TestOutputFlag pins the persistent --output flag: table by default, json on request, and an
// error for anything else.
func TestOutputFlag(t *testing.T) {
	// newRootWithProbe returns a command tree whose single subcommand reports the parsed format.
	newRootWithProbe := func(t *testing.T, captured *outputFormat) (*cobra.Command, *cobra.Command) {
		t.Helper()
		root := NewRoot()
		require.NotNil(t, root.PersistentFlags().Lookup(outputFlagName))
		probe := &cobra.Command{
			Use: "probe",
			RunE: func(cmd *cobra.Command, _ []string) error {
				format, err := outputOf(cmd)
				if err != nil {
					return err
				}
				*captured = format
				return nil
			},
		}
		root.AddCommand(probe)
		return root, probe
	}

	for _, format := range []outputFormat{outputTable, outputJSON} {
		t.Run(string(format), func(t *testing.T) {
			var captured outputFormat
			root, _ := newRootWithProbe(t, &captured)
			root.SetArgs([]string{"probe", "-o", string(format)})
			require.NoError(t, root.Execute())
			require.Equal(t, format, captured)
		})
	}

	t.Run("invalid", func(t *testing.T) {
		var captured outputFormat
		root, _ := newRootWithProbe(t, &captured)
		root.SetArgs([]string{"probe", "-o", "yaml"})
		err := root.Execute()
		require.ErrorContains(t, err, "unsupported output format")
	})
}

// TestCheckResponse pins the error mapping of §10.4: one message plus one line per problem.
func TestCheckResponse(t *testing.T) {
	t.Run("expected status", func(t *testing.T) {
		response := &http.Response{StatusCode: http.StatusOK, Status: "200 OK"}
		require.NoError(t, checkResponse(response, []byte(`{}`), http.StatusOK, http.StatusCreated))
	})

	t.Run("unexpected status", func(t *testing.T) {
		response := &http.Response{StatusCode: http.StatusNotFound, Status: "404 Not Found"}
		err := checkResponse(response, []byte(`{"message": "dataset not found"}`), http.StatusOK)
		require.ErrorContains(t, err, "dataset not found")

		var apiFailure *apiError
		require.ErrorAs(t, err, &apiFailure)
		require.Equal(t, http.StatusNotFound, apiFailure.Status)
	})

	t.Run("validation details", func(t *testing.T) {
		response := &http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request"}
		body := []byte(`{"message": "invalid definition", "errors": [
		  {"path": "data[2].target", "message": "target overlaps data[0]"},
		  {"message": "at least one item is required"}
		]}`)
		err := checkResponse(response, body, http.StatusOK)
		require.EqualError(t, err, "invalid definition\n  data[2].target: target overlaps data[0]\n  at least one item is required")
	})

	t.Run("non JSON body", func(t *testing.T) {
		response := &http.Response{StatusCode: http.StatusBadGateway, Status: "502 Bad Gateway"}
		err := checkResponse(response, []byte("<html>gateway</html>"), http.StatusOK)
		require.ErrorContains(t, err, "unexpected status 502 Bad Gateway")
	})

	t.Run("no response", func(t *testing.T) {
		require.ErrorContains(t, checkResponse(nil, nil, http.StatusOK), "no response")
	})
}

// TestAPIClientPath pins the control-plane base path the plugin client uses (§10). lakectl's
// endpoint is used as the server, so the generated operations resolve under /api/v1/datasets.
func TestAPIClientPath(t *testing.T) {
	require.Equal(t, "/api/v1/datasets", controlPlanePath)
}

// TestPluginArgumentsShape pins how the plugin is invoked: lakectl strips the plugin name, so
// `lakectl datasets list --prefix customer` runs `lakectl-datasets list --prefix customer`, and
// main re-inserts the command name in front of those arguments for lakectl's root. This tree is
// the root itself, so the same arguments arrive without the command name.
func TestPluginArgumentsShape(t *testing.T) {
	fake := newFakeControlPlane(t, fakeResponse{Status: http.StatusOK, Body: `{"results": [], "pagination": ` + emptyPagination + `}`})
	out, err := executeCommand(t, "list", "--prefix", "customer")
	require.NoError(t, err)
	require.Empty(t, out)
	require.Equal(t, []string{"customer"}, fake.OnlyRequest().Query["prefix"])
}
