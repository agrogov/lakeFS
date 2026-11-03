package importer

// Location arithmetic shared by the two modes: a metadata location is inside a storage namespace
// when it is strictly below it, a file is inside a table root when it is strictly below that, and
// the remainder of that check is the key the copy uses on the target side.

import (
	"fmt"
	"strings"
)

// relativeTo returns location's path below root, validating the remainder segment by segment.
//
// The strictness mirrors the catalog's own foreign-location control
// (pkg/icebergcatalog/register.go, files.go): the remainder must not be empty, must not contain an
// empty segment and must not contain "." or "..", because either would name a different directory
// than the string suggests. root and location are compared with a trailing slash, so a sibling
// that merely starts with the same characters ("s3://b/_icebergx" vs "s3://b/_iceberg") is not
// inside.
func relativeTo(root, location string) (string, error) {
	root = strings.TrimRight(root, "/")
	location = strings.TrimRight(location, "/")
	relative, ok := strings.CutPrefix(location, root+"/")
	if !ok || relative == "" {
		return "", fmt.Errorf("%w: %q is not inside %q", ErrTableCopy, location, root)
	}
	for _, segment := range strings.Split(relative, "/") {
		switch {
		case segment == "":
			return "", fmt.Errorf("%w: %q has an empty path segment", ErrTableCopy, location)
		case pointerOrRef(segment):
			return "", fmt.Errorf("%w: %q has the reserved path segment %q", ErrTableCopy, location, segment)
		}
	}
	return relative, nil
}

// inside reports whether location is strictly inside root.
func inside(root, location string) bool {
	_, err := relativeTo(root, location)
	return err == nil
}

// joinLocation joins root and relative with exactly one slash and no surrounding whitespace.
func joinLocation(root, relative string) string {
	return strings.TrimRight(strings.TrimSpace(root), "/") + "/" + strings.TrimLeft(relative, "/")
}

// normalizeLocation trims the whitespace and trailing slashes a document may carry around its
// location. It does not otherwise rewrite the URI: the copy needs the original form to read files.
func normalizeLocation(location string) string {
	return strings.TrimRight(strings.TrimSpace(location), "/")
}

// pointerOrRef reports whether name is "." or "..", the two segments that make a path name
// something other than what it looks like.
func pointerOrRef(name string) bool { return name == "." || name == ".." }
