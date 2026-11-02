package icebergcatalog_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// This file is the phase I-0 spike S-I1 evidence (ICEBERG_CATALOG_PLAN.md section
// 4.2 and section 11 I-0): it proves that the pinned github.com/apache/iceberg-go
// model can decode and re-encode every update action and every requirement type of
// the Iceberg REST OpenAPI spec version we advertise, so that phase I-2 commits can
// hand the request body straight to iceberg-go instead of re-parsing it.
//
// Source of truth for the action and requirement names below: the Apache Iceberg
// REST OpenAPI spec (Apache-2.0),
// https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
// — components.schemas.TableUpdate (anyOf) and components.schemas.TableRequirement
// (discriminator mapping). The per-action schemas in that file define which fields
// are required; the example payloads here follow them field by field, in the shape
// a client puts on the wire.

// specTableUpdateActions lists every member of TableUpdate.anyOf in the REST spec,
// in spec order. ViewUpdate members (add-view-version, set-current-view-version) are
// not part of TableUpdate and are deliberately absent.
var specTableUpdateActions = []string{
	"assign-uuid",
	"upgrade-format-version",
	"add-schema",
	"set-current-schema",
	"add-spec",
	"set-default-spec",
	"add-sort-order",
	"set-default-sort-order",
	"add-snapshot",
	"set-snapshot-ref",
	"remove-snapshots",
	"remove-snapshot-ref",
	"set-location",
	"set-properties",
	"remove-properties",
	"set-statistics",
	"remove-statistics",
	"set-partition-statistics",
	"remove-partition-statistics",
	"remove-partition-specs",
	"remove-schemas",
	"add-encryption-key",
	"remove-encryption-key",
}

// tableUpdateExamples holds one example payload per TableUpdate action, written the way
// a client serializes it into the commit request's "updates" array.
var tableUpdateExamples = []struct {
	action  string
	payload string
}{
	{"assign-uuid", `{"action":"assign-uuid","uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1"}`},
	{"upgrade-format-version", `{"action":"upgrade-format-version","format-version":2}`},
	{"add-schema", `{"action":"add-schema","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"x","required":true,"type":"long"},{"id":2,"name":"y","required":false,"type":"string"}]},"last-column-id":2}`},
	{"set-current-schema", `{"action":"set-current-schema","schema-id":-1}`},
	{"add-spec", `{"action":"add-spec","spec":{"spec-id":0,"fields":[{"name":"x_bucket","transform":"bucket[16]","source-id":1,"field-id":1000}]}}`},
	{"set-default-spec", `{"action":"set-default-spec","spec-id":-1}`},
	{"add-sort-order", `{"action":"add-sort-order","sort-order":{"order-id":1,"fields":[{"transform":"identity","source-id":1,"direction":"asc","null-order":"nulls-first"}]}}`},
	{"set-default-sort-order", `{"action":"set-default-sort-order","sort-order-id":-1}`},
	{"add-snapshot", `{"action":"add-snapshot","snapshot":{"snapshot-id":3055729675574597004,"parent-snapshot-id":3051729675574597004,"sequence-number":2,"timestamp-ms":1555100955811,"manifest-list":"s3://bucket/test/location/metadata/snap-3055729675574597004-1-41fb.avro","summary":{"operation":"append","added-data-files":"1"},"schema-id":0}}`},
	{"set-snapshot-ref", `{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":3055729675574597004,"max-ref-age-ms":86400000,"max-snapshot-age-ms":3600000,"min-snapshots-to-keep":2}`},
	{"remove-snapshots", `{"action":"remove-snapshots","snapshot-ids":[3051729675574597004,3055729675574597004]}`},
	{"remove-snapshot-ref", `{"action":"remove-snapshot-ref","ref-name":"audit"}`},
	{"set-location", `{"action":"set-location","location":"s3://bucket/test/location-2"}`},
	{"set-properties", `{"action":"set-properties","updates":{"owner":"lakefs","write.metadata.compression-codec":"gzip"}}`},
	{"remove-properties", `{"action":"remove-properties","removals":["owner","write.metadata.compression-codec"]}`},
	{"set-statistics", `{"action":"set-statistics","snapshot-id":3055729675574597004,"statistics":{"snapshot-id":3055729675574597004,"statistics-path":"s3://bucket/test/location/metadata/2025-01-01/statistic-3055729675574597004-abc.puffin","file-size-in-bytes":1300,"file-footer-size-in-bytes":100,"blob-metadata":[{"type":"apache-datasketches-theta-v1","snapshot-id":3055729675574597004,"sequence-number":2,"fields":[1],"properties":{"ndv":"5"}}]}}`},
	{"remove-statistics", `{"action":"remove-statistics","snapshot-id":3055729675574597004}`},
	{"set-partition-statistics", `{"action":"set-partition-statistics","partition-statistics":{"snapshot-id":3055729675574597004,"statistics-path":"s3://bucket/test/location/metadata/partition-statistic-3055729675574597004-abc.parquet","file-size-in-bytes":1000}}`},
	{"remove-partition-statistics", `{"action":"remove-partition-statistics","snapshot-id":3055729675574597004}`},
	{"remove-partition-specs", `{"action":"remove-partition-specs","spec-ids":[0,1]}`},
	{"remove-schemas", `{"action":"remove-schemas","schema-ids":[0]}`},
	{"add-encryption-key", `{"action":"add-encryption-key","encryption-key":{"key-id":"key-1","encrypted-key-metadata":"AAAA","encrypted-by-id":"key-0","properties":{"kms.key-arn":"arn:aws:kms:us-east-1:111122223333:key/abc"}}}`},
	{"remove-encryption-key", `{"action":"remove-encryption-key","key-id":"key-1"}`},
}

// specTableRequirementTypes lists every member of the REST spec's TableRequirement
// discriminator mapping, in spec order.
var specTableRequirementTypes = []string{
	"assert-create",
	"assert-table-uuid",
	"assert-ref-snapshot-id",
	"assert-last-assigned-field-id",
	"assert-current-schema-id",
	"assert-last-assigned-partition-id",
	"assert-default-spec-id",
	"assert-default-sort-order-id",
}

// requirementExamples holds one example payload per requirement type, plus the
// Validate outcome expected against the two table states below: an existing v2 table
// (baseTableMetadata) and no table at all (a nil Metadata, which Requirement.Validate
// accepts to mean "the table does not exist").
var requirementExamples = []struct {
	reqType          string
	payload          string
	wantErrOnBase    bool
	wantErrOnMissing bool
}{
	// assert-create is the mirror image of every other requirement: it only holds
	// when the table does not exist yet.
	{"assert-create", `{"type":"assert-create"}`, true, false},
	{"assert-table-uuid", `{"type":"assert-table-uuid","uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1"}`, false, true},
	{"assert-ref-snapshot-id", `{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}`, false, true},
	{"assert-last-assigned-field-id", `{"type":"assert-last-assigned-field-id","last-assigned-field-id":3}`, false, true},
	{"assert-current-schema-id", `{"type":"assert-current-schema-id","current-schema-id":0}`, false, true},
	{"assert-last-assigned-partition-id", `{"type":"assert-last-assigned-partition-id","last-assigned-partition-id":999}`, false, true},
	{"assert-default-spec-id", `{"type":"assert-default-spec-id","default-spec-id":0}`, false, true},
	{"assert-default-sort-order-id", `{"type":"assert-default-sort-order-id","default-sort-order-id":0}`, false, true},
}

// baseTableMetadata is a minimal valid format-version 2 table metadata document
// (the same shape as iceberg-go's TableMetadataV2ValidMinimal.json test fixture):
// one struct schema (id 0, last column id 3), one empty partition spec (id 0,
// last partition id 999), the unsorted sort order (id 0) and no refs.
const baseTableMetadata = `{
  "format-version": 2,
  "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
  "location": "s3://bucket/test/location",
  "last-sequence-number": 0,
  "last-updated-ms": 1602638573590,
  "last-column-id": 3,
  "current-schema-id": 0,
  "schemas": [{"type":"struct","schema-id":0,"fields":[
    {"id":1,"name":"x","required":true,"type":"long"},
    {"id":2,"name":"y","required":true,"type":"long"},
    {"id":3,"name":"z","required":true,"type":"long"}]}],
  "default-spec-id": 0,
  "partition-specs": [{"spec-id":0,"fields":[]}],
  "last-partition-id": 999,
  "default-sort-order-id": 0,
  "sort-orders": [{"order-id":0,"fields":[]}],
  "properties": {},
  "current-snapshot-id": -1,
  "refs": {},
  "snapshots": [],
  "snapshot-log": [],
  "metadata-log": [],
  "statistics": [],
  "partition-statistics": []
}`

// TestUpdatesCoverage is the S-I1 gate: every update action and every requirement
// type in the REST spec must decode through iceberg-go's table package, survive a
// re-encode, and report back the same discriminator.
//
// Both halves are driven by the spec's own action lists (specTableUpdateActions,
// specTableRequirementTypes), not by iceberg-go's, so a client sending a payload we
// cannot model fails here rather than in phase I-2.
func TestUpdatesCoverage(t *testing.T) {
	t.Run("updates", testUpdatesCoverage)
	t.Run("requirements", testRequirementsCoverage)
}

func testUpdatesCoverage(t *testing.T) {
	t.Helper()

	// The example table must cover the spec exactly: one case per action, no extras.
	covered := make(map[string]int, len(tableUpdateExamples))
	for _, tc := range tableUpdateExamples {
		covered[tc.action]++
	}
	for _, action := range specTableUpdateActions {
		switch n := covered[action]; {
		case n == 0:
			t.Fatalf("spec update action %q has no example payload: iceberg-go coverage is unproven", action)
		case n > 1:
			t.Fatalf("spec update action %q has %d example payloads, want 1", action, n)
		}
	}
	spec := make(map[string]struct{}, len(specTableUpdateActions))
	for _, action := range specTableUpdateActions {
		spec[action] = struct{}{}
	}
	for _, tc := range tableUpdateExamples {
		if _, ok := spec[tc.action]; !ok {
			t.Fatalf("example payload for %q is not a TableUpdate action in the REST spec", tc.action)
		}
	}

	for _, tc := range tableUpdateExamples {
		t.Run(tc.action, func(t *testing.T) {
			var updates table.Updates
			if err := json.Unmarshal([]byte("["+tc.payload+"]"), &updates); err != nil {
				t.Fatalf("decode %s: %v", tc.action, err)
			}
			assertSingleUpdateAction(t, updates, tc.action, "decoded")

			// Round-trip: what we decoded must re-encode into a body that decodes
			// back to the same action. Phase I-2 relies on this when it echoes an
			// applied update back to a mirror writer.
			encoded, err := json.Marshal(updates[0])
			if err != nil {
				t.Fatalf("re-encode %s: %v", tc.action, err)
			}
			var roundTripped table.Updates
			if err := json.Unmarshal([]byte("["+string(encoded)+"]"), &roundTripped); err != nil {
				t.Fatalf("decode re-encoded %s: %v (payload: %s)", tc.action, err, encoded)
			}
			assertSingleUpdateAction(t, roundTripped, tc.action, "re-encoded")
		})
	}
}

// assertSingleUpdateAction checks that updates holds exactly one update whose Action
// equals want, describing it by stage ("decoded", "re-encoded").
func assertSingleUpdateAction(t *testing.T, updates table.Updates, want, stage string) {
	t.Helper()

	if len(updates) != 1 {
		t.Fatalf("%s %q payload produced %d updates, want 1", stage, want, len(updates))
	}
	if got := updates[0].Action(); got != want {
		t.Fatalf("%s update action = %q, want %q", stage, got, want)
	}
}

func testRequirementsCoverage(t *testing.T) {
	t.Helper()

	covered := make(map[string]int, len(requirementExamples))
	for _, tc := range requirementExamples {
		covered[tc.reqType]++
	}
	for _, reqType := range specTableRequirementTypes {
		switch n := covered[reqType]; {
		case n == 0:
			t.Fatalf("spec requirement type %q has no example payload: iceberg-go coverage is unproven", reqType)
		case n > 1:
			t.Fatalf("spec requirement type %q has %d example payloads, want 1", reqType, n)
		}
	}

	base, err := table.ParseMetadataBytes([]byte(baseTableMetadata))
	if err != nil {
		t.Fatalf("parse base table metadata: %v", err)
	}

	for _, tc := range requirementExamples {
		t.Run(tc.reqType, func(t *testing.T) {
			var requirements table.Requirements
			if err := json.Unmarshal([]byte("["+tc.payload+"]"), &requirements); err != nil {
				t.Fatalf("decode %s: %v", tc.reqType, err)
			}
			if len(requirements) != 1 {
				t.Fatalf("decoded %q payload produced %d requirements, want 1", tc.reqType, len(requirements))
			}

			assertSingleRequirementType(t, requirements[0], tc.reqType, "decoded")

			encoded, err := json.Marshal(requirements[0])
			if err != nil {
				t.Fatalf("re-encode %s: %v", tc.reqType, err)
			}
			var roundTripped table.Requirements
			if err := json.Unmarshal([]byte("["+string(encoded)+"]"), &roundTripped); err != nil {
				t.Fatalf("decode re-encoded %s: %v (payload: %s)", tc.reqType, err, encoded)
			}
			if len(roundTripped) != 1 {
				t.Fatalf("re-encoded %q produced %d requirements, want 1", tc.reqType, len(roundTripped))
			}
			assertSingleRequirementType(t, roundTripped[0], tc.reqType, "re-encoded")

			// Validate is the check I2-3 runs before applying updates: it must be
			// satisfied by a matching table state and rejected by a missing one
			// (or, for assert-create, the other way round).
			assertRequirementValidates(t, requirements[0], tc.reqType, base, tc.wantErrOnBase, "existing table")
			assertRequirementValidates(t, requirements[0], tc.reqType, nil, tc.wantErrOnMissing, "missing table")
		})
	}
}

// assertSingleRequirementType checks that requirement reports want as its type,
// describing it by stage.
func assertSingleRequirementType(t *testing.T, requirement table.Requirement, want, stage string) {
	t.Helper()

	if got := requirement.GetType(); got != want {
		t.Fatalf("%s requirement type = %q, want %q", stage, got, want)
	}
}

// assertRequirementValidates runs requirement.Validate(meta) and checks the outcome
// against wantErr, reporting meta as state.
func assertRequirementValidates(t *testing.T, requirement table.Requirement, reqType string, meta table.Metadata, wantErr bool, state string) {
	t.Helper()

	err := requirement.Validate(meta)
	switch {
	case wantErr && err == nil:
		t.Errorf("%s must fail to validate against a %s, but returned nil", reqType, state)
	case !wantErr && err != nil:
		t.Errorf("%s must validate against a %s, but returned: %v", reqType, state, err)
	case err != nil:
		t.Logf("%s rejected a %s as expected: %v", reqType, state, err)
	}
}

// TestUnknownUpdatePayloadRejected documents how a payload that does not match any
// spec action fails, so phase I-2 knows what to turn into a 400 BadRequestException:
// an unknown action, a missing action, and a missing required field. It is not part
// of the S-I1 gate, and its name keeps `-run TestUpdatesCoverage` off it.
//
// All three wrap iceberg.ErrInvalidArgument, which is the value the commit handler
// keys the 400 on; the wrapped text is not part of the contract.
func TestUnknownUpdatePayloadRejected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"unknown-action", `[{"action":"rewrite-everything"}]`},
		{"missing-action", `[{"uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1"}]`},
		{"missing-required-field", `[{"action":"set-location"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var updates table.Updates
			err := json.Unmarshal([]byte(tc.payload), &updates)
			if err == nil {
				t.Fatalf("payload %s decoded into %d updates, want an error", tc.payload, len(updates))
			}
			if !errors.Is(err, iceberg.ErrInvalidArgument) {
				t.Fatalf("payload %s failed with %v, want an error wrapping iceberg.ErrInvalidArgument", tc.payload, err)
			}
			t.Logf("rejected as expected: %v", err)
		})
	}
}
