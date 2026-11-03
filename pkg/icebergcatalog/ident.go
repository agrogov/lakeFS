package icebergcatalog

// Identifier parsing for the Iceberg REST catalog (ICEBERG_CATALOG_PLAN.md §5 "Addressing",
// §12 "unit tests: identifier parsing").
//
// The catalog addresses a table by four things: a lakeFS repository, a ref (branch, tag or
// commit), a namespace and a table name. Iceberg itself has no repository level, so the
// mapping is lakeFS's extension of the namespace tree: the first two namespace levels are
// always the repository and the ref, and every level after them is a real namespace level.
//
//	/iceberg/api/v1/lakefs/namespaces/{repo<sep>ref<sep>ns…}             (§5 row 2)
//	/iceberg/api/v1/lakefs/namespaces/{repo<sep>ref<sep>ns…}/tables/{t} (§5 row 3)
//	/iceberg/relative_to/{repo}.{branch}/api/v1/…/namespaces/{ns…}      (§5 row 4)
//	/iceberg/relative_to/{repo}/api/v1/…/namespaces/{ref<sep>ns…}       (§5 row 5)
//
// The relative_to forms fix part of the address in the URL, so the client's namespace path
// holds only the levels below it (ICEBERG_CATALOG_PLAN.md §5, "Relative Namespace Support" in
// https://docs.lakefs.io/iceberg/).
//
// Security property: because the repository and the ref are the first two levels of the path
// parameter (or are fixed by the URL), a client can add namespace levels but can never move,
// replace or prepend either of them. Every address this file returns has therefore the
// repository and ref that the route chose, never one the client smuggled in as a namespace
// level. Escaped separators are decoded exactly once, so "%251F" is a literal "%1F" inside a
// level (and rejected as a special character), not a second separator.
//
// Namespace rules are the published lakeFS rules
// (https://docs.lakefs.io/iceberg/, "Namespace Restrictions"):
//
//   - namespaces are case-sensitive;
//   - the total namespace path is shorter than 255 characters;
//   - no namespace level is empty;
//   - no special characters, except dots, which are ordinary characters inside a level.
//
// Separators follow the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml,
// components.parameters.namespace): multipart namespace levels are joined with the advertised
// `namespace-separator`, which defaults to the ASCII unit separator 0x1F (URL-encoded %1F), and
// "servers must use both the advertised separator and 0x1F as valid separators when decoding
// namespaces". This file implements exactly that, and the catalog advertises
// EncodedNamespaceSeparator, i.e. the separator it implements. Dots are not a separator here:
// advertising them would make a dotted namespace name unrepresentable, which is why the RCK
// runs with supports-names-with-dot=false (ICEBERG_CATALOG_PLAN.md §5).
//
// Everything in this file is a pure function: no I/O, no context.

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/treeverse/lakefs/pkg/validator"
)

const (
	// NamespaceSeparator is the decoded separator between the levels of a multipart
	// namespace: the Iceberg default, the ASCII unit separator byte 0x1F. It is what
	// EncodedNamespaceSeparator is the URL-encoded form of, and what Namespace.String()
	// joins with.
	NamespaceSeparator = "\x1f"

	// EncodedNamespaceSeparator is NamespaceSeparator in the form the catalog advertises in
	// the `namespace-separator` client property of GET /v1/config. The REST OpenAPI spec says
	// "The namespace-separator should be provided in a url encoded form", so the advertised
	// value is the URL-encoded unit separator, not a raw control byte in the JSON response.
	EncodedNamespaceSeparator = "%1F"

	// macroLevels is the number of levels the /iceberg/api form reserves for the repository
	// and the ref: ICEBERG_CATALOG_PLAN.md §5, "Levels 1 and 2 are always repo and ref".
	macroLevels = 2

	// MaxNamespacePathLength is the exclusive upper bound of the length of a namespace path,
	// in characters: the lakeFS docs require "less than 255 characters", so 254 is the longest
	// accepted path and 255 is rejected.
	MaxNamespacePathLength = 255
)

// ErrInvalidIdentifier is wrapped by every identifier parsing and validation error in this
// package. The HTTP layer maps it to 400 BadRequestException. It never means that a repository,
// ref or table does not exist: that is decided after resolution (404 NoSuch*Exception), not
// while parsing the request.
var ErrInvalidIdentifier = errors.New("invalid iceberg identifier")

// Namespace is an ordered list of namespace levels, without the repository and ref levels that
// the /iceberg/api form carries in the same path parameter. It is case-sensitive and its order
// is significant: ["inventory", "books"] is not ["books", "inventory"].
type Namespace []string

// String joins the levels with NamespaceSeparator, the wire form of a namespace inside a path
// parameter.
func (n Namespace) String() string {
	return strings.Join(n, NamespaceSeparator)
}

// Dotted joins the levels with dots, the notation lakeFS uses for a namespace outside a URL
// (catalog ARNs, e.g. arn:lakefs:catalog:::namespace/{repo}/{namespace} in
// https://docs.lakefs.io/iceberg/). It is not the wire encoding of a namespace path parameter;
// String is.
func (n Namespace) Dotted() string {
	return strings.Join(n, ".")
}

// Address is a parsed catalog address (ICEBERG_CATALOG_PLAN.md §5). Repo is always set. Ref
// is empty only for the repository-only level of the namespace listing. Namespace is the client
// namespace (nil when empty); it is empty only for the repository and repository.ref levels that
// the namespace listing addresses. Table is empty for a namespace address and for every view
// address; View is set only by a view address.
type Address struct {
	Repo      string
	Ref       string
	Namespace Namespace
	Table     string
	View      string
}

// HasTable reports whether the address names a table rather than a namespace or a view.
func (a Address) HasTable() bool {
	return a.Table != ""
}

// HasView reports whether the address names a view rather than a namespace or a table.
//
// A view and a table can carry the same name in the same namespace - they live in different
// directories of the reserved area (store.go ViewPointerPath, tablePointerPath) and the Iceberg
// spec addresses them by different endpoints - so the two fields are not exclusive by name, only
// by which endpoint parsed the address.
func (a Address) HasView() bool {
	return a.View != ""
}

// Scope is the part of an address that a relative_to URL fixes (ICEBERG_CATALOG_PLAN.md §5).
// The zero Scope fixes nothing: that is the /iceberg/api form, where the first two levels of
// the namespace path parameter are the repository and the ref.
type Scope struct {
	// Repo is the repository fixed by the URL; empty when the identifier starts with it.
	Repo string

	// Ref is the ref fixed by the URL; empty when the identifier carries it. A fixed Ref
	// requires a fixed Repo.
	Ref string
}

// ParseWarehouse parses the relative_to URL segment, which is either "<repo>.<branch>" or just
// "<repo>" (ICEBERG_CATALOG_PLAN.md §5):
//
//	/iceberg/relative_to/{repo}.{branch}/api → Scope{Repo: repo, Ref: branch}
//	/iceberg/relative_to/{repo}/api          → Scope{Repo: repo}
//
// The segment is percent-decoded first, like every other path parameter. The split is at the
// first dot: a lakeFS repository name cannot contain a dot (validator.ReValidRepositoryID
// allows lowercase letters, digits and hyphens only), so this is unambiguous even though a tag
// ref may itself contain dots ("my-repo.release.v1.2" is the tag "release.v1.2"). Whether the
// ref is a branch, a tag or a commit is decided when the ref is resolved, not here: both
// relative_to forms support any-ref reads, while writes are rejected on a non-branch ref.
func ParseWarehouse(segment string) (Scope, error) {
	decoded, err := unescapeSegment(segment)
	if err != nil {
		return Scope{}, err
	}
	repo, ref, hasRef := strings.Cut(decoded, ".")
	if err := validateRepo(repo); err != nil {
		return Scope{}, err
	}
	if !hasRef {
		return Scope{Repo: repo}, nil
	}
	if err := validateRef(ref); err != nil {
		return Scope{}, err
	}
	return Scope{Repo: repo, Ref: ref}, nil
}

// ParseNamespace parses a namespace address of the /iceberg/api form
// (/v1/{prefix}/namespaces/{namespace}, ICEBERG_CATALOG_PLAN.md §5). The path parameter carries
// the macro levels and the client namespace in one value:
//
//	repo                     → the repository level (list repositories)
//	repo<sep>ref             → the repository.ref level (list branches, list namespaces in ref)
//	repo<sep>ref<sep>ns…     → a real namespace
//
// The first two levels are always the repository and the ref (§5: "Levels 1 and 2 are always
// repo and ref"); every level after them is a namespace level.
func ParseNamespace(namespaceParam string) (Address, error) {
	return parseAddressWith(Scope{}, namespaceParam, "", noName)
}

// ParseTable parses a table address of the /iceberg/api form
// (/v1/{prefix}/namespaces/{namespace}/tables/{table}, ICEBERG_CATALOG_PLAN.md §5). It requires
// the namespace parameter to carry repo, ref and at least one namespace level: a table always
// lives in a namespace, so "repo<sep>ref" is not a table address.
func ParseTable(namespaceParam, table string) (Address, error) {
	return parseAddressWith(Scope{}, namespaceParam, table, tableName)
}

// ParseView parses a view address of the /iceberg/api form
// (/v1/{prefix}/namespaces/{namespace}/views/{view}). It requires the same shape ParseTable
// requires - a repository, a ref and at least one namespace level - because a view lives in a
// namespace exactly as a table does.
func ParseView(namespaceParam, view string) (Address, error) {
	return parseAddressWith(Scope{}, namespaceParam, view, viewName)
}

// ParseRelativeNamespace parses a namespace address of the relative_to forms
// (ICEBERG_CATALOG_PLAN.md §5), where the URL has already fixed part of the address:
//
//	/iceberg/relative_to/{repo}.{branch}/api → every level of the parameter is a namespace
//	level, because Scope has both Repo and Ref;
//	/iceberg/relative_to/{repo}/api          → the first level of the parameter is the ref and
//	the rest is the namespace, because Scope has only Repo.
//
// An empty Scope parses the parameter exactly like ParseNamespace.
func ParseRelativeNamespace(scope Scope, namespaceParam string) (Address, error) {
	return parseAddressWith(scope, namespaceParam, "", noName)
}

// ParseRelativeTable is ParseRelativeNamespace for a table address: it additionally requires at
// least one namespace level below the fixed repo and ref (or below the ref carried by the
// parameter).
func ParseRelativeTable(scope Scope, namespaceParam, table string) (Address, error) {
	return parseAddressWith(scope, namespaceParam, table, tableName)
}

// ParseRelativeView is ParseRelativeTable for a view address: the same relative_to forms, with the
// view name where the table name would be.
func ParseRelativeView(scope Scope, namespaceParam, view string) (Address, error) {
	return parseAddressWith(scope, namespaceParam, view, viewName)
}

// DecodeNamespacePath decodes and validates a namespace path parameter that is entirely a
// client namespace (the relative_to/{repo}.{branch} form, or the `parent` query parameter of
// list namespaces once the repository and ref have been removed). It splits on
// NamespaceSeparator and applies the lakeFS namespace rules.
func DecodeNamespacePath(raw string) (Namespace, error) {
	return DecodeNamespacePathWithSeparator(raw, NamespaceSeparator)
}

// DecodeNamespacePathWithSeparator is DecodeNamespacePath for an explicit separator: pass the
// decoded form of whatever the catalog advertises as `namespace-separator` (currently
// NamespaceSeparator, advertised as EncodedNamespaceSeparator). The spec-mandated unit
// separator is always accepted in addition, so a client that ignores the advertised separator
// still works. The separator must be non-empty.
//
// The separator is what decides whether a dot is a nesting separator or an ordinary character:
// with the default unit separator, "a.b" is one level named "a.b", as the lakeFS docs require
// dots to be allowed in a level. A deployment that advertises "." splits "a.b" into two levels
// and can therefore not represent a dotted level name.
func DecodeNamespacePathWithSeparator(raw, separator string) (Namespace, error) {
	levels, err := splitPathParam(raw, separator)
	if err != nil {
		return nil, err
	}
	if err := validateLevelShape(levels); err != nil {
		return nil, err
	}
	namespace := Namespace(levels)
	if err := validateNamespace(namespace); err != nil {
		return nil, err
	}
	return namespace, nil
}

// nameKind is what a parsed address names below its namespace: nothing (a namespace address),
// a table or a view. It is a type rather than a bool because there are now three cases, and the
// three differ only in which field of Address is filled and which decoder validates the name.
type nameKind int

const (
	// noName is a namespace address: nothing is named below the namespace.
	noName nameKind = iota
	// tableName is a table address (/…/tables/{table}).
	tableName
	// viewName is a view address (/…/views/{view}).
	viewName
)

// parseAddressWith implements all six Parse* forms. kind says what the address names below its
// namespace, and so whether a name is required and how it is validated.
func parseAddressWith(scope Scope, namespaceParam, name string, kind nameKind) (Address, error) {
	if scope.Ref != "" && scope.Repo == "" {
		return Address{}, fmt.Errorf("%w: a fixed ref requires a fixed repository", ErrInvalidIdentifier)
	}
	levels, err := splitPathParam(namespaceParam, NamespaceSeparator)
	if err != nil {
		return Address{}, err
	}
	if err := validateLevelShape(levels); err != nil {
		return Address{}, err
	}

	var addr Address
	switch {
	case scope.Repo == "":
		// /iceberg/api form: the first two levels are the repository and the ref.
		if len(levels) == 1 && kind == noName {
			// A repository-only address: the root of the namespace listing.
			addr.Repo = levels[0]
			break
		}
		if len(levels) < macroLevels {
			return Address{}, fmt.Errorf("%w: namespace %q must start with a repository and a ref",
				ErrInvalidIdentifier, namespaceParam)
		}
		addr.Repo, addr.Ref = levels[0], levels[1]
		addr.Namespace = Namespace(levels[macroLevels:])
	case scope.Ref == "":
		// /iceberg/relative_to/{repo}/api: the first level is the ref.
		addr.Repo = scope.Repo
		addr.Ref = levels[0]
		addr.Namespace = Namespace(levels[1:])
	default:
		// /iceberg/relative_to/{repo}.{branch}/api: every level is a namespace level.
		addr.Repo, addr.Ref = scope.Repo, scope.Ref
		addr.Namespace = Namespace(levels)
	}

	if err := validateRepo(addr.Repo); err != nil {
		return Address{}, err
	}
	if addr.Ref != "" {
		if err := validateRef(addr.Ref); err != nil {
			return Address{}, err
		}
	}
	if len(addr.Namespace) == 0 {
		// Normalize the empty namespace so an address is deterministic: a caller can compare
		// it with Address{...} without worrying about nil-versus-empty slices.
		addr.Namespace = nil
	}
	if err := validateNamespace(addr.Namespace); err != nil {
		return Address{}, err
	}

	if kind == noName {
		return addr, nil
	}
	if len(addr.Namespace) == 0 {
		return Address{}, fmt.Errorf("%w: a %s address needs a namespace between the ref and the %s",
			ErrInvalidIdentifier, kind, kind)
	}
	decoded, err := decodeName(name, kind.String())
	if err != nil {
		return Address{}, err
	}
	if kind == viewName {
		addr.View = decoded
	} else {
		addr.Table = decoded
	}
	return addr, nil
}

// String is the noun of a name kind, for messages and for the decoders below.
func (k nameKind) String() string {
	switch k {
	case tableName:
		return "table"
	case viewName:
		return "view"
	default:
		return "name"
	}
}

// splitPathParam percent-decodes one path parameter and splits it into levels. It accepts the
// advertised separator and the spec-mandated unit separator, as the REST OpenAPI spec requires.
// The value is decoded exactly once: a double-encoded separator stays a literal "%1F" in a
// level and is rejected later as a special character, rather than silently becoming a level
// boundary.
func splitPathParam(raw, separator string) ([]string, error) {
	if separator == "" {
		return nil, fmt.Errorf("%w: the namespace separator is empty", ErrInvalidIdentifier)
	}
	decoded, err := unescapeSegment(raw)
	if err != nil {
		return nil, err
	}
	if separator != NamespaceSeparator {
		// "servers must use both the advertised separator and 0x1F as valid separators when
		// decoding namespaces" (rest-catalog-open-api.yaml, parameters.namespace).
		decoded = strings.ReplaceAll(decoded, separator, NamespaceSeparator)
	}
	return strings.Split(decoded, NamespaceSeparator), nil
}

// unescapeSegment percent-decodes a path parameter. chi's URLParam returns the raw segment when
// net/http saw an escaped path and the already-decoded segment otherwise (chi matches on
// URL.RawPath when it is set, see chi/v5 mux.go), so both "ns2%1Fsub" and "ns2\x1fsub" are
// accepted.
func unescapeSegment(raw string) (string, error) {
	decoded, err := url.PathUnescape(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %q is not valid percent-encoding: %w", ErrInvalidIdentifier, raw, err)
	}
	return decoded, nil
}

// validateLevelShape applies the rules that hold for every level of a path parameter, including
// the repository and ref levels: a level is non-empty and is not made only of dots. The latter
// is not one of the docs' rules but a reserved-name rule of this package's own: "." and ".." are
// the only special-character levels the docs would otherwise allow, and they must not reach the
// _lakefs_iceberg/ns/<level>/… layout (ICEBERG_CATALOG_PLAN.md §6.1) as path components.
func validateLevelShape(levels []string) error {
	for i, level := range levels {
		if level == "" {
			return fmt.Errorf("%w: namespace level %d is empty", ErrInvalidIdentifier, i+1)
		}
		if strings.Trim(level, ".") == "" {
			return fmt.Errorf("%w: namespace level %d is %q, and dot-only levels are reserved",
				ErrInvalidIdentifier, i+1, level)
		}
	}
	return nil
}

// validateNamespace applies the lakeFS namespace rules to the client namespace levels. An empty
// namespace is allowed here: it is the repository or repository.ref listing address.
func validateNamespace(namespace Namespace) error {
	for _, level := range namespace {
		for _, r := range level {
			if !validNamespaceRune(r) {
				return fmt.Errorf("%w: namespace level %q contains the unsupported character %q; "+
					"only letters, digits, dots, underscores and hyphens are allowed",
					ErrInvalidIdentifier, level, r)
			}
		}
	}
	if n := utf8.RuneCountInString(namespace.String()); n >= MaxNamespacePathLength {
		return fmt.Errorf("%w: namespace path is %d characters, the docs require fewer than %d",
			ErrInvalidIdentifier, n, MaxNamespacePathLength)
	}
	return nil
}

// validNamespaceRune reports whether r may appear in a namespace level. The lakeFS docs allow
// "no special characters except dots"; underscore and hyphen are the other characters lakeFS
// itself allows in repository and branch names, so namespace levels accept them too (my_namespace
// and my-namespace are ordinary names). Case is preserved, because namespaces are
// case-sensitive.
func validNamespaceRune(r rune) bool {
	if r == utf8.RuneError {
		return false
	}
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.'
}

// validateRepo checks the repository level against the lakeFS repository naming rules, which
// the docs require of the Iceberg identifier's repository component ("<repository> must be a
// valid lakeFS repository name"). The separator check is redundant with the repository regex,
// but it keeps the invariant explicit if the regex ever widens.
func validateRepo(repo string) error {
	if repo == "" {
		return fmt.Errorf("%w: the repository is empty", ErrInvalidIdentifier)
	}
	if strings.Contains(repo, NamespaceSeparator) {
		return fmt.Errorf("%w: repository %q contains a namespace separator", ErrInvalidIdentifier, repo)
	}
	if !validator.ReValidRepositoryID.MatchString(repo) {
		return fmt.Errorf("%w: %q is not a valid lakeFS repository name", ErrInvalidIdentifier, repo)
	}
	return nil
}

// validateRef checks the ref level. A ref may be a branch, a tag or a commit, so the branch
// naming rule is too narrow here; lakeFS's own ref rule is used instead, plus the constraints
// this package's addressing needs: no separator (it would have split the level) and no slash
// (the ref is carried in a URL path segment). Whether the ref is a branch is decided at
// resolution, not at parse time (ICEBERG_CATALOG_PLAN.md §5, Q-I4).
func validateRef(ref string) error {
	if ref == "" {
		return fmt.Errorf("%w: the ref is empty", ErrInvalidIdentifier)
	}
	if strings.Contains(ref, NamespaceSeparator) {
		return fmt.Errorf("%w: ref %q contains a namespace separator", ErrInvalidIdentifier, ref)
	}
	if strings.ContainsAny(ref, "/\\") {
		return fmt.Errorf("%w: ref %q contains a path separator", ErrInvalidIdentifier, ref)
	}
	if strings.Trim(ref, ".") == "" {
		return fmt.Errorf("%w: ref %q is reserved", ErrInvalidIdentifier, ref)
	}
	for _, r := range ref {
		if r == utf8.RuneError || unicode.IsControl(r) {
			return fmt.Errorf("%w: ref %q contains an unsupported character", ErrInvalidIdentifier, ref)
		}
	}
	if !validator.ReValidRef.MatchString(ref) {
		return fmt.Errorf("%w: %q is not a valid lakeFS ref", ErrInvalidIdentifier, ref)
	}
	return nil
}

// decodeTableName decodes and validates the table path parameter.
func decodeTableName(raw string) (string, error) {
	return decodeName(raw, "table")
}

// decodeViewName decodes and validates the view path parameter. A view name is validated exactly
// like a table name: both are turned into a key component of the reserved area, so the same rules
// (no separator, no path separator, no dot-only name, no control character, the same length cap)
// are what keeps the key unambiguous.
func decodeViewName(raw string) (string, error) {
	return decodeName(raw, "view")
}

// decodeName decodes and validates a table or view path parameter. These names are not covered
// by the lakeFS namespace rules, so only what this package's addressing needs is rejected: an
// empty name, a separator, a path separator or a dot-only name, all of which would either split
// the address or make the _lakefs_iceberg/tables/<ns>/<table> (and views/<ns>/<view>) key
// ambiguous. Control characters are rejected because they cannot appear in a lakeFS key that is
// also a URI. noun is "table" or "view" and appears in every message.
func decodeName(raw, noun string) (string, error) {
	name, err := unescapeSegment(raw)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", fmt.Errorf("%w: the %s name is empty", ErrInvalidIdentifier, noun)
	}
	if strings.Contains(name, NamespaceSeparator) {
		return "", fmt.Errorf("%w: %s %q contains a namespace separator", ErrInvalidIdentifier, noun, name)
	}
	if strings.ContainsAny(name, "/\\") {
		return "", fmt.Errorf("%w: %s %q contains a path separator", ErrInvalidIdentifier, noun, name)
	}
	if strings.Trim(name, ".") == "" {
		return "", fmt.Errorf("%w: %s %q is reserved", ErrInvalidIdentifier, noun, name)
	}
	for _, r := range name {
		if r == utf8.RuneError || unicode.IsControl(r) {
			return "", fmt.Errorf("%w: %s %q contains an unsupported character", ErrInvalidIdentifier, noun, name)
		}
	}
	if n := utf8.RuneCountInString(name); n >= MaxNamespacePathLength {
		return "", fmt.Errorf("%w: %s name is %d characters, the limit is %d",
			ErrInvalidIdentifier, noun, n, MaxNamespacePathLength-1)
	}
	return name, nil
}
