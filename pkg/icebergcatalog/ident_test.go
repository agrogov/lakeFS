package icebergcatalog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests for ICEBERG_CATALOG_PLAN.md §5 "Addressing" and §12 "identifier parsing". Every test
// name starts with TestIdent so that the task's validation command,
// `go test -run TestIdent ./pkg/icebergcatalog/`, runs exactly this file.
//
// The parsing is pure, so the tests are table-driven over identifiers: the accepted forms, the
// namespace rules from https://docs.lakefs.io/iceberg/ and the escape attempts that must not be
// able to move the repository or the ref.

func TestIdentParseNamespace(t *testing.T) {
	tests := []struct {
		name    string
		param   string
		want    Address
		wantErr string
	}{
		// The /iceberg/api form: repo and ref are the first two levels (§5 row 2).
		{
			name:  "repo and ref",
			param: "repo%1Fmain",
			want:  Address{Repo: "repo", Ref: "main"},
		},
		{
			name:  "lowercase hex separator",
			param: "repo%1fmain",
			want:  Address{Repo: "repo", Ref: "main"},
		},
		{
			name:  "already decoded separator",
			param: "repo\x1fmain",
			want:  Address{Repo: "repo", Ref: "main"},
		},
		{
			name:  "one namespace level",
			param: "repo%1Fmain%1Finventory",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"inventory"}},
		},
		{
			name:  "two namespace levels",
			param: "repo%1Fmain%1Finventory%1Fbooks",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"inventory", "books"}},
		},
		{
			name:  "documented example",
			param: "my-repo%1Ffeature-branch%1Finventory%1Fbooks",
			want:  Address{Repo: "my-repo", Ref: "feature-branch", Namespace: Namespace{"inventory", "books"}},
		},
		{
			name:  "dot is an ordinary character in a level",
			param: "repo%1Fmain%1Fa.b",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"a.b"}},
		},
		{
			name:  "case is preserved and significant",
			param: "repo%1Fmain%1FInventory",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"Inventory"}},
		},
		{
			name:  "namespace level named like the ref",
			param: "repo%1Fmain%1Fmain",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"main"}},
		},
		{
			name:  "namespace level named like a repository",
			param: "repo%1Fmain%1Fother-repo",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"other-repo"}},
		},
		{
			// §5: "GET namespaces with no parent lists repositories"; with parent repo it
			// lists branches. A single level is therefore a valid namespace-listing address.
			name:  "repository only",
			param: "repo",
			want:  Address{Repo: "repo"},
		},

		// Rejections.
		{name: "empty", param: "", wantErr: "namespace level 1 is empty"},
		{name: "leading separator", param: "%1Fmain", wantErr: "namespace level 1 is empty"},
		{name: "empty ref", param: "repo%1F%1Fns", wantErr: "namespace level 2 is empty"},
		{name: "trailing separator", param: "repo%1Fmain%1F", wantErr: "namespace level 3 is empty"},
		{name: "empty namespace level", param: "repo%1Fmain%1F%1Fns", wantErr: "namespace level 3 is empty"},
		{name: "uppercase repository", param: "Repo%1Fmain", wantErr: "not a valid lakeFS repository name"},
		{name: "short repository", param: "ab%1Fmain", wantErr: "not a valid lakeFS repository name"},
		{name: "dotted repository", param: "my%2Erepo%1Fmain%1Fns", wantErr: "not a valid lakeFS repository name"},
		{name: "encoded slash in ref", param: "repo%1Fmain%2Fref%1Fns", wantErr: "contains a path separator"},
		{name: "control character in ref", param: "repo%1Fma%00in%1Fns", wantErr: "unsupported character"},
		{name: "dot level", param: "repo%1Fmain%1F.", wantErr: "dot-only levels are reserved"},
		{name: "dot dot level", param: "repo%1Fmain%1F..", wantErr: "dot-only levels are reserved"},
		{name: "encoded dot dot level", param: "repo%1Fmain%1F%2E%2E", wantErr: "dot-only levels are reserved"},
		{name: "three dot level", param: "repo%1Fmain%1F...", wantErr: "dot-only levels are reserved"},
		{name: "slash in namespace", param: "repo%1Fmain%1Fns%2Fescape", wantErr: "unsupported character"},
		{name: "space in namespace", param: "repo%1Fmain%1Fns%20name", wantErr: "unsupported character"},
		{name: "star in namespace", param: "repo%1Fmain%1Fns*", wantErr: "unsupported character"},
		{name: "at sign in namespace", param: "repo%1Fmain%1Fns@name", wantErr: "unsupported character"},
		{name: "unicode replacement in namespace", param: "repo%1Fmain%1Fns%FF", wantErr: "unsupported character"},
		{name: "invalid percent encoding", param: "repo%1Fmain%1Fns%zz", wantErr: "not valid percent-encoding"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseNamespace(tt.param)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidIdentifier)
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, Address{}, got, "a rejected identifier must not leak a partial address")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			require.False(t, got.HasTable())
		})
	}
}

func TestIdentParseTable(t *testing.T) {
	longName := strings.Repeat("t", MaxNamespacePathLength)
	tests := []struct {
		name    string
		param   string
		table   string
		want    Address
		wantErr string
	}{
		{
			name:  "namespace and table",
			param: "repo%1Fmain%1Finventory",
			table: "books",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"inventory"}, Table: "books"},
		},
		{
			name:  "nested namespace and table",
			param: "repo%1Fmain%1Finventory%1Fbooks",
			table: "titles",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"inventory", "books"}, Table: "titles"},
		},
		{
			name:  "dot in table name",
			param: "repo%1Fmain%1Finventory",
			table: "books.2024",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"inventory"}, Table: "books.2024"},
		},
		{
			name:  "percent-encoded table name",
			param: "repo%1Fmain%1Finventory",
			table: "books%2D2024",
			want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"inventory"}, Table: "books-2024"},
		},

		{name: "no namespace level", param: "repo%1Fmain", table: "books", wantErr: "needs a namespace"},
		{name: "repository only", param: "repo", table: "books", wantErr: "must start with a repository and a ref"},
		{name: "empty table", param: "repo%1Fmain%1Finventory", table: "", wantErr: "the table name is empty"},
		{name: "dot table", param: "repo%1Fmain%1Finventory", table: ".", wantErr: "is reserved"},
		{name: "dot dot table", param: "repo%1Fmain%1Finventory", table: "..", wantErr: "is reserved"},
		{name: "slash in table", param: "repo%1Fmain%1Finventory", table: "a/b", wantErr: "contains a path separator"},
		{name: "encoded slash in table", param: "repo%1Fmain%1Finventory", table: "a%2Fb", wantErr: "contains a path separator"},
		{name: "backslash in table", param: "repo%1Fmain%1Finventory", table: `a\b`, wantErr: "contains a path separator"},
		{name: "separator in table", param: "repo%1Fmain%1Finventory", table: "a%1Fb", wantErr: "contains a namespace separator"},
		{name: "control character in table", param: "repo%1Fmain%1Finventory", table: "a%00b", wantErr: "unsupported character"},
		{name: "table name too long", param: "repo%1Fmain%1Finventory", table: longName, wantErr: "the limit is 254"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTable(tt.param, tt.table)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidIdentifier)
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, Address{}, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
			require.True(t, got.HasTable())
		})
	}

	t.Run("longest accepted table name", func(t *testing.T) {
		got, err := ParseTable("repo%1Fmain%1Finventory", strings.Repeat("t", MaxNamespacePathLength-1))
		require.NoError(t, err)
		require.Len(t, got.Table, MaxNamespacePathLength-1)
	})
}

func TestIdentParseWarehouse(t *testing.T) {
	tests := []struct {
		name    string
		segment string
		want    Scope
		wantErr string
	}{
		{
			// §5 row 4: /iceberg/relative_to/{repo}.{branch}/api
			name:    "repository and ref",
			segment: "my-repo.main",
			want:    Scope{Repo: "my-repo", Ref: "main"},
		},
		{
			// §5 row 5: /iceberg/relative_to/{repo}/api
			name:    "repository only",
			segment: "my-repo",
			want:    Scope{Repo: "my-repo"},
		},
		{
			name:    "percent-encoded segment",
			segment: "my%2Drepo.main",
			want:    Scope{Repo: "my-repo", Ref: "main"},
		},
		{
			name:    "branch-like ref",
			segment: "my-repo.feature-branch",
			want:    Scope{Repo: "my-repo", Ref: "feature-branch"},
		},
		{
			// A repository name cannot contain a dot, so everything after the first dot is
			// the ref: a tag ref may contain dots.
			name:    "dotted tag ref",
			segment: "my-repo.release.v1.2",
			want:    Scope{Repo: "my-repo", Ref: "release.v1.2"},
		},

		{name: "empty", segment: "", wantErr: "the repository is empty"},
		{name: "empty repository", segment: ".main", wantErr: "the repository is empty"},
		{name: "empty ref", segment: "my-repo.", wantErr: "the ref is empty"},
		{name: "uppercase repository", segment: "My-Repo.main", wantErr: "not a valid lakeFS repository name"},
		{name: "underscore in repository", segment: "my_repo.main", wantErr: "not a valid lakeFS repository name"},
		{name: "slash in ref", segment: "my-repo.bad%2Fref", wantErr: "contains a path separator"},
		{name: "dot ref", segment: "my-repo.%2E", wantErr: "is reserved"},
		{name: "dot dot ref", segment: "my-repo.%2E%2E", wantErr: "is reserved"},
		{name: "control character in ref", segment: "my-repo.ma%00in", wantErr: "unsupported character"},
		{name: "invalid percent encoding", segment: "my-repo.%zz", wantErr: "not valid percent-encoding"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWarehouse(tt.segment)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrInvalidIdentifier)
				require.ErrorContains(t, err, tt.wantErr)
				require.Equal(t, Scope{}, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestIdentRelativeAddressing(t *testing.T) {
	t.Run("fixed repository and ref: every level is a namespace", func(t *testing.T) {
		scope := Scope{Repo: "my-repo", Ref: "main"}
		got, err := ParseRelativeNamespace(scope, "inventory%1Fbooks")
		require.NoError(t, err)
		require.Equal(t, Address{
			Repo:      "my-repo",
			Ref:       "main",
			Namespace: Namespace{"inventory", "books"},
		}, got)

		got, err = ParseRelativeTable(scope, "inventory", "books")
		require.NoError(t, err)
		require.Equal(t, Address{
			Repo:      "my-repo",
			Ref:       "main",
			Namespace: Namespace{"inventory"},
			Table:     "books",
		}, got)
	})

	t.Run("fixed repository only: the first level is the ref", func(t *testing.T) {
		scope := Scope{Repo: "my-repo"}
		got, err := ParseRelativeNamespace(scope, "main%1Finventory%1Fbooks")
		require.NoError(t, err)
		require.Equal(t, Address{
			Repo:      "my-repo",
			Ref:       "main",
			Namespace: Namespace{"inventory", "books"},
		}, got)

		got, err = ParseRelativeNamespace(scope, "main")
		require.NoError(t, err)
		require.Equal(t, Address{Repo: "my-repo", Ref: "main"}, got)

		got, err = ParseRelativeTable(scope, "main%1Finventory", "books")
		require.NoError(t, err)
		require.Equal(t, Address{
			Repo:      "my-repo",
			Ref:       "main",
			Namespace: Namespace{"inventory"},
			Table:     "books",
		}, got)
	})

	t.Run("zero scope is the /iceberg/api form", func(t *testing.T) {
		got, err := ParseRelativeTable(Scope{}, "repo%1Fmain%1Finventory", "books")
		require.NoError(t, err)
		require.Equal(t, Address{
			Repo:      "repo",
			Ref:       "main",
			Namespace: Namespace{"inventory"},
			Table:     "books",
		}, got)
	})

	t.Run("client levels cannot move the fixed repository or ref", func(t *testing.T) {
		// The client's levels look exactly like a repository, a ref and a namespace. In the
		// fixed-repo-and-ref form they are all namespace levels; Repo and Ref stay fixed.
		got, err := ParseRelativeNamespace(Scope{Repo: "my-repo", Ref: "main"},
			"other-repo%1Fother-ref%1Finventory")
		require.NoError(t, err)
		require.Equal(t, "my-repo", got.Repo)
		require.Equal(t, "main", got.Ref)
		require.Equal(t, Namespace{"other-repo", "other-ref", "inventory"}, got.Namespace)
	})

	errors := []struct {
		name    string
		scope   Scope
		param   string
		table   string
		wantErr string
	}{
		{
			name:    "fixed ref without a repository",
			scope:   Scope{Ref: "main"},
			param:   "inventory",
			wantErr: "a fixed ref requires a fixed repository",
		},
		{
			name:    "invalid fixed repository",
			scope:   Scope{Repo: "My_Repo", Ref: "main"},
			param:   "inventory",
			wantErr: "not a valid lakeFS repository name",
		},
		{
			name:    "fixed ref with a path separator",
			scope:   Scope{Repo: "my-repo", Ref: "bad/ref"},
			param:   "inventory",
			wantErr: "contains a path separator",
		},
		{
			name:    "empty namespace under a fixed repository and ref",
			scope:   Scope{Repo: "my-repo", Ref: "main"},
			param:   "",
			wantErr: "namespace level 1 is empty",
		},
		{
			name:    "escaping namespace under a fixed repository and ref",
			scope:   Scope{Repo: "my-repo", Ref: "main"},
			param:   "..%1F..",
			wantErr: "dot-only levels are reserved",
		},
		{
			name:    "table without a namespace under a fixed repository",
			scope:   Scope{Repo: "my-repo"},
			param:   "main",
			table:   "books",
			wantErr: "needs a namespace",
		},
		{
			name:    "table without a namespace under a fixed repository and ref",
			scope:   Scope{Repo: "my-repo", Ref: "main"},
			param:   "%1Fbooks",
			table:   "books",
			wantErr: "namespace level 1 is empty",
		},
	}
	for _, tt := range errors {
		t.Run(tt.name, func(t *testing.T) {
			var (
				got Address
				err error
			)
			if tt.table == "" {
				got, err = ParseRelativeNamespace(tt.scope, tt.param)
			} else {
				got, err = ParseRelativeTable(tt.scope, tt.param, tt.table)
			}
			require.ErrorIs(t, err, ErrInvalidIdentifier)
			require.ErrorContains(t, err, tt.wantErr)
			require.Equal(t, Address{}, got)
		})
	}
}

func TestIdentNamespaceRules(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		tests := []struct {
			name string
			raw  string
			want Namespace
		}{
			{name: "one level", raw: "inventory", want: Namespace{"inventory"}},
			{name: "two levels", raw: "inventory%1Fbooks", want: Namespace{"inventory", "books"}},
			{name: "lowercase separator", raw: "inventory%1fbooks", want: Namespace{"inventory", "books"}},
			{name: "dots are ordinary characters", raw: "a.b", want: Namespace{"a.b"}},
			{name: "encoded dot inside a level", raw: "a%2Eb", want: Namespace{"a.b"}},
			{name: "underscore and hyphen", raw: "my_ns%1Fmy-ns", want: Namespace{"my_ns", "my-ns"}},
			{name: "case preserved", raw: "Books%1Fbooks", want: Namespace{"Books", "books"}},
			{name: "letters and digits", raw: "ns123", want: Namespace{"ns123"}},
			{name: "unicode letters", raw: "café", want: Namespace{"café"}},
			{name: "longest accepted path", raw: strings.Repeat("a", MaxNamespacePathLength-1), want: Namespace{strings.Repeat("a", MaxNamespacePathLength-1)}},
			{
				name: "longest accepted multi-level path",
				raw:  strings.Repeat("a", MaxNamespacePathLength-4) + "%1Fbc",
				want: Namespace{strings.Repeat("a", MaxNamespacePathLength-4), "bc"},
			},
			{
				// The docs count characters, not bytes: 200 two-byte runes are 200
				// characters and fit under the 255-character limit.
				name: "character count is not byte count",
				raw:  strings.Repeat("é", 200),
				want: Namespace{strings.Repeat("é", 200)},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := DecodeNamespacePath(tt.raw)
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
				require.Equal(t, strings.Join(tt.want, "\x1f"), got.String())
			})
		}
	})

	t.Run("rejected", func(t *testing.T) {
		tests := []struct {
			name    string
			raw     string
			wantErr string
		}{
			{name: "empty", raw: "", wantErr: "namespace level 1 is empty"},
			{name: "empty level", raw: "a%1F%1Fb", wantErr: "namespace level 2 is empty"},
			{name: "too long", raw: strings.Repeat("a", MaxNamespacePathLength), wantErr: "fewer than 255"},
			{
				name:    "too long across levels",
				raw:     strings.Repeat("a", MaxNamespacePathLength-3) + "%1Fbc",
				wantErr: "fewer than 255",
			},
			{name: "dot level", raw: ".", wantErr: "dot-only levels are reserved"},
			{name: "dot dot level", raw: "..", wantErr: "dot-only levels are reserved"},
			{name: "encoded dot dot level", raw: "%2e%2e", wantErr: "dot-only levels are reserved"},
			{name: "slash", raw: "a/b", wantErr: "unsupported character"},
			{name: "encoded slash", raw: "a%2Fb", wantErr: "unsupported character"},
			{name: "backslash", raw: `a\b`, wantErr: "unsupported character"},
			{name: "space", raw: "a b", wantErr: "unsupported character"},
			{name: "star", raw: "a*b", wantErr: "unsupported character"},
			{name: "at sign", raw: "a@b", wantErr: "unsupported character"},
			{name: "colon", raw: "a:b", wantErr: "unsupported character"},
			{name: "percent", raw: "a%25b", wantErr: "unsupported character"},
			{name: "tab", raw: "a%09b", wantErr: "unsupported character"},
			{name: "nul", raw: "a%00b", wantErr: "unsupported character"},
			{name: "emoji", raw: "a%F0%9F%98%80", wantErr: "unsupported character"},
			{name: "invalid utf-8", raw: "a%FFb", wantErr: "unsupported character"},
			{name: "double encoded separator", raw: "a%251Fb", wantErr: "unsupported character"},
			{name: "invalid percent encoding", raw: "a%zzb", wantErr: "not valid percent-encoding"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := DecodeNamespacePath(tt.raw)
				require.ErrorIs(t, err, ErrInvalidIdentifier)
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
			})
		}
	})
}

func TestIdentSeparators(t *testing.T) {
	// The catalog advertises the separator it implements (§5: "Advertise namespace-separator
	// only if ident actually implements it").
	require.Equal(t, "\x1f", NamespaceSeparator)
	require.Equal(t, "%1F", EncodedNamespaceSeparator)

	t.Run("advertised separator and the spec unit separator are both accepted", func(t *testing.T) {
		// rest-catalog-open-api.yaml, parameters.namespace: "servers must use both the
		// advertised separator and 0x1F as valid separators when decoding namespaces".
		for _, raw := range []string{"inventory.books", "inventory%1Fbooks", "inventory\x1fbooks"} {
			got, err := DecodeNamespacePathWithSeparator(raw, ".")
			require.NoError(t, err, raw)
			require.Equal(t, Namespace{"inventory", "books"}, got, raw)
		}
	})

	t.Run("default separator keeps dots inside a level", func(t *testing.T) {
		got, err := DecodeNamespacePathWithSeparator("inventory.books", NamespaceSeparator)
		require.NoError(t, err)
		require.Equal(t, Namespace{"inventory.books"}, got)
	})

	t.Run("multi-character separator", func(t *testing.T) {
		got, err := DecodeNamespacePathWithSeparator("inventory::books", "::")
		require.NoError(t, err)
		require.Equal(t, Namespace{"inventory", "books"}, got)
	})

	t.Run("empty separator is rejected", func(t *testing.T) {
		got, err := DecodeNamespacePathWithSeparator("inventory", "")
		require.ErrorIs(t, err, ErrInvalidIdentifier)
		require.ErrorContains(t, err, "separator is empty")
		require.Nil(t, got)
	})

	t.Run("namespace rendering", func(t *testing.T) {
		namespace := Namespace{"inventory", "books"}
		require.Equal(t, "inventory\x1fbooks", namespace.String())
		// ARNs use the dot-separated notation (https://docs.lakefs.io/iceberg/), not the
		// wire encoding.
		require.Equal(t, "inventory.books", namespace.Dotted())
	})
}

// TestIdentNoEscape pins the security property of §5: a client namespace can add levels, but it
// can never change, replace or prepend the repository or the ref of the address, and it can
// never walk out of the catalog's _lakefs_iceberg/ns/… layout.
func TestIdentNoEscape(t *testing.T) {
	t.Run("cannot shift the repository or the ref", func(t *testing.T) {
		tests := []struct {
			name  string
			param string
			want  Address
		}{
			{
				name:  "repeated repository and ref names",
				param: "repo%1Fmain%1Frepo%1Fmain%1Fns",
				want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"repo", "main", "ns"}},
			},
			{
				name:  "ref-looking namespace",
				param: "repo%1Fmain%1Fmain%1Fmain",
				want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"main", "main"}},
			},
			{
				name:  "repository-looking namespace",
				param: "repo%1Fmain%1Fother-repo%1Fother-ref",
				want:  Address{Repo: "repo", Ref: "main", Namespace: Namespace{"other-repo", "other-ref"}},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got, err := ParseNamespace(tt.param)
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("separator and path traversal attempts are rejected", func(t *testing.T) {
		for _, param := range []string{
			"repo%1Fmain%1F..%1F..%1Fetc",
			"repo%1Fmain%1F%2E%2E%2F%2E%2E",
			"repo%1Fmain%1F..%2F..%2Fetc%2Fpasswd",
			"repo%1Fmain%1Fns%251Fescape",
			"repo%1Fmain%1Fns%5Cescape",
			"repo%1Fmain%1F%2Fabsolute",
		} {
			got, err := ParseNamespace(param)
			require.ErrorIs(t, err, ErrInvalidIdentifier, param)
			require.Equal(t, Address{}, got, param)
		}
	})

	t.Run("mismatched repository in a table address is rejected", func(t *testing.T) {
		got, err := ParseTable("Repo%1Fmain%1Finventory", "books")
		require.ErrorIs(t, err, ErrInvalidIdentifier)
		require.Equal(t, Address{}, got)
	})
}
