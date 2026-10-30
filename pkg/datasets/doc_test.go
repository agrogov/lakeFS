package datasets

import (
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPackageDocListsEverySourceFile guards the "layer by layer" list in doc.go.
//
// doc.go is the package map: every non-test source file of package datasets is expected to be
// named in the package doc comment, next to the layer it belongs to. That list is edited by hand
// and has gone stale repeatedly (a new file lands, the prose does not), so this check makes the
// failure visible in CI rather than in review.
//
// The check reads the package doc comment through go/doc, from the parsed files, not from the raw
// bytes of doc.go. A file mentioned only in an ordinary code comment therefore does not satisfy
// it: the requirement is that the package documentation names the file.
//
// Exclusions, and why they are safe:
//
//   - doc.go - it is the map, so it does not list itself.
//   - *_test.go - tests describe behaviour, not layers. This covers test-only helpers
//     (helpers_test.go, spikes_helpers_test.go) and the design-phase spike evidence
//     (spikes_*_test.go), which the doc names as a group ("S2 and S4") rather than file by file.
//   - generated files, detected by the canonical ast.IsGenerated "// Code generated ... DO NOT
//     EDIT." header. There are none in the package root today; they are excluded because they are
//     produced from api/datasets.yml and the doc refers to them by generator and package
//     (api/datasets.yml, apigen) instead of by output file name.
//
// Everything else is required, including small files such as metrics.go and metadata_keys.go: the
// point is coverage of the layer list, not prose quality. The check says nothing about where in
// doc.go a name appears or how it is described.
func TestPackageDocListsEverySourceFile(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	require.NoError(t, err, "read package directory")

	var (
		docFiles []*ast.File // every non-test file: the input for the package doc
		want     []string    // the files the package doc must mention
	)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		require.NoErrorf(t, err, "parse %s", name)
		docFiles = append(docFiles, f)
		switch {
		case name == "doc.go":
			// The map itself.
		case ast.IsGenerated(f):
			// Generated from api/datasets.yml; named in the doc by generator, not by file.
		default:
			want = append(want, name)
		}
	}
	require.NotEmpty(t, want, "found no non-test source files; the guard would pass vacuously")

	pkgDoc, err := doc.NewFromFiles(fset, docFiles, "github.com/treeverse/lakefs/pkg/datasets")
	require.NoError(t, err, "build package doc")
	require.NotEmpty(t, pkgDoc.Doc, "package datasets has no package doc comment to check")

	for _, name := range want {
		if !docMentionsFile(pkgDoc.Doc, name) {
			t.Errorf("pkg/datasets/%s is not mentioned in the package doc; add it to the layer list in doc.go", name)
		}
	}
}

// docMentionsFile reports whether docText names the file as a standalone token. Plain substring
// matching is not enough: "validate.go" is a suffix of "metadata_validate.go", so a doc that
// listed only the latter would wrongly satisfy the former. Requiring a non-identifier byte on
// both sides keeps the check honest without imposing anything on the prose.
func docMentionsFile(docText, name string) bool {
	for offset := 0; offset < len(docText); {
		i := strings.Index(docText[offset:], name)
		if i < 0 {
			return false
		}
		start := offset + i
		end := start + len(name)
		beforeOK := start == 0 || !isIdentifierByte(docText[start-1])
		afterOK := end == len(docText) || !isIdentifierByte(docText[end])
		if beforeOK && afterOK {
			return true
		}
		offset = start + 1
	}
	return false
}

func isIdentifierByte(c byte) bool {
	return c == '_' ||
		c >= '0' && c <= '9' ||
		c >= 'a' && c <= 'z' ||
		c >= 'A' && c <= 'Z'
}
