package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/spf13/cobra"
	lakectlcmd "github.com/treeverse/lakefs/cmd/lakectl/cmd"
	"github.com/treeverse/lakefs/pkg/datasets/apigen"
)

// versionRefPattern matches the refs a definition write and a version read accept
// (DATASETS_DESIGN.md §10.1, §10.3): `vN`, a bare version number, `latest` or `proposal-<id>`.
var versionRefPattern = regexp.MustCompile(`^(latest|v[0-9]+|[0-9]+|proposal-[0-9A-Za-z]+)$`)

// loadedDefinition is the result of reading a -f definition file.
type loadedDefinition struct {
	// Definition is the dataset definition of §5.1.
	Definition apigen.DatasetDefinition
	// Name is the dataset name when the file is the definition envelope of §5.1; empty when the
	// file contains a bare definition.
	Name string
}

// readDefinitionFile reads and parses the -f file. "-" reads the definition from stdin, exactly
// like other lakectl commands that take a file argument.
func readDefinitionFile(path string) (loadedDefinition, error) {
	file, err := lakectlcmd.OpenByPath(path)
	if err != nil {
		return loadedDefinition{}, fmt.Errorf("open definition file %q: %w", path, err)
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(file)
	if err != nil {
		return loadedDefinition{}, fmt.Errorf("read definition file %q: %w", path, err)
	}
	loaded, err := parseDefinitionDocument(data)
	if err != nil {
		return loadedDefinition{}, fmt.Errorf("definition file %q: %w", path, err)
	}
	return loaded, nil
}

// parseDefinitionDocument accepts both definition documents of §5.1:
//
//   - the definition envelope, `{"name": ..., "definition": {...}, "publish": {...}}`, which is the
//     portable Enterprise-compatible form; the embedded dataset name is returned as well, and
//   - a bare definition, `{"format_version": 1, "data": [...]}`, which is what `publish` and
//     `validate` take since the dataset name is already a command argument.
func parseDefinitionDocument(data []byte) (loadedDefinition, error) {
	var properties map[string]json.RawMessage
	if err := json.Unmarshal(data, &properties); err != nil {
		return loadedDefinition{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if embedded, ok := properties[definitionEnvelopeKey]; ok && string(embedded) != "null" {
		var envelope struct {
			Name       string                   `json:"name"`
			Definition apigen.DatasetDefinition `json:"definition"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			return loadedDefinition{}, fmt.Errorf("invalid \"%s\" object: %w", definitionEnvelopeKey, err)
		}
		return loadedDefinition{Definition: envelope.Definition, Name: envelope.Name}, nil
	}
	_, hasFormatVersion := properties["format_version"]
	_, hasData := properties["data"]
	if !hasFormatVersion && !hasData {
		return loadedDefinition{}, invalidArguments(`expected a dataset definition ("format_version" and "data") or a `+
			`definition envelope with a "%s" object`, definitionEnvelopeKey)
	}
	// A bare definition: the document itself is the DatasetDefinition of §5.1.
	var definition apigen.DatasetDefinition
	if err := json.Unmarshal(data, &definition); err != nil {
		return loadedDefinition{}, fmt.Errorf("invalid dataset definition: %w", err)
	}
	return loadedDefinition{Definition: definition}, nil
}

// definitionEnvelopeKey is the property that marks the definition envelope of §5.1.
const definitionEnvelopeKey = "definition"

// parseVersionRef validates a ref accepted by `show --version` and `versions`.
func parseVersionRef(value string) (string, error) {
	if value == "" {
		return "", invalidArguments("empty version: use %q, %q, %q or a commit id", "latest", "vN", "N")
	}
	if !versionRefPattern.MatchString(value) {
		return "", invalidArguments("invalid version %q: use %q, %q, %q or %q", value, "latest", "vN", "N", "proposal-<id>")
	}
	return value, nil
}

// parseExpectedVersion parses the --expected-version flag: the optimistic concurrency check of
// §5.4, which behaves like an ETag. Version 0 is accepted: it asserts that no version has been
// published yet.
func parseExpectedVersion(value string) (*int, error) {
	if value == "" {
		return nil, nil
	}
	expected, err := strconv.Atoi(value)
	if err != nil {
		return nil, invalidArguments("--expected-version expects a version number, got %q", value)
	}
	if expected < 0 {
		return nil, invalidArguments("--expected-version must not be negative, got %d", expected)
	}
	return &expected, nil
}

// definitionReadFlags adds the flags that read a definition file. The commands check the flag
// themselves so the error names the command that needs it.
func definitionReadFlags(cmd *cobra.Command) {
	cmd.Flags().StringP(fileFlagName, "f", "", "dataset definition file (- for stdin); accepts a definition or the definition envelope")
}
