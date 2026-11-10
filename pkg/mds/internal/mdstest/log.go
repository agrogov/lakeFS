// Package mdstest holds the test helpers the Metadata Search packages share. It is internal so the
// helpers stay out of the production API.
package mdstest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/logging"
)

// CapturePackageLog redirects the package logger (the one logging.FromContext returns, where the
// Metadata Search code writes its warnings) to a file and returns a reader for it.
func CapturePackageLog(t *testing.T) func() string {
	t.Helper()
	logFile := filepath.Join(t.TempDir(), "mds-package.log")
	require.NoError(t, logging.SetOutputs([]string{logFile}, 0, 0), "redirect the package logger")
	t.Cleanup(func() {
		_ = logging.CloseWriters()
		require.NoError(t, logging.SetOutputs([]string{"-"}, 0, 0), "restore the package logger")
	})
	return func() string {
		require.NoError(t, logging.CloseWriters(), "flush the captured log")
		data, err := os.ReadFile(logFile)
		require.NoError(t, err, "read the captured log")
		return string(data)
	}
}
