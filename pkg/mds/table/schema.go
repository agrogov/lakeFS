// Package table is the Iceberg side of the Metadata Search indexer: the schema of the
// system.object_metadata table, the Arrow rows that are written into it, and the transaction
// that applies one commit of a data repository to it (METADATA_SEARCH_DESIGN.md §3.1, §4.3, §4.6,
// §4.9, §10.2).
//
// The package is a REST client. It talks to the lakeFS Iceberg catalog through the relative_to
// addressing form (<lakefs>/iceberg/relative_to/<repo>.<branch>/api) with iceberg-go's
// catalog/rest, so it imports neither pkg/catalog nor pkg/graveler: everything it needs about the
// table - its schema, its properties, its snapshots - is read and written through the catalog
// (design §2: "the indexer is a REST client of lakeFS").
//
// The table is unpartitioned, format v2, sorted by path, with a fixed schema whose field IDs are
// part of the contract: schema changes only ever add optional fields with new IDs (§3.1).
//
// Clean-room note: upstream Metadata Search is a lakeFS Enterprise feature. This is an independent
// OSS implementation written from METADATA_SEARCH_DESIGN.md, the public Metadata Search guide
// (https://docs.lakefs.io/guides/metadata-search/), the Apache Iceberg table spec
// (https://iceberg.apache.org/spec/) and the Apache Iceberg REST OpenAPI spec
// (https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) only. No
// upstream Enterprise code, and no lakeFS code under the BSL, was used.
package table

import (
	"github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
)

const (
	// Namespace is the namespace of the virtual table in the companion repository R-metadata
	// (§3.1, §3.3). The catalog resolves R.<ref>.system.object_metadata to it (§5.1).
	Namespace = "system"

	// Name is the table name (§3.1). Together with Namespace it is the table identifier.
	Name = "object_metadata"

	// Column names, in schema order (§3.1). They are also the names of the fields of Row.
	ColumnRepository   = "repository"
	ColumnPath         = "path"
	ColumnCommitID     = "commit_id"
	ColumnSizeBytes    = "size_bytes"
	ColumnLastModified = "last_modified"
	ColumnETag         = "etag"
	ColumnUserMetadata = "user_metadata"
	ColumnCommitter    = "committer"
	ColumnContentType  = "content_type"
	ColumnCreationDate = "creation_date"
)

// The fixed field IDs of §3.1. They are the IDs of the created table - the ones written into
// every data and delete file - so they are part of the table's compatibility surface: an added
// column takes a new ID, and an existing ID never changes meaning.
//
// The create request of Schema is built in the same pre-order as the created table, so these IDs
// also appear in the request. The catalog still owns the numbering: iceberg-go's
// NewMetadataWithUUID reassigns every ID in pre-order (AssignFreshSchemaIDs) when it applies the
// request, and that pass is a no-op on a request in this order. Readers and the writer therefore
// take the IDs from the loaded table (Table.Schema()), never from these constants; a test pins
// that the two agree at creation.
const (
	FieldRepository   = 1
	FieldPath         = 2
	FieldCommitID     = 3
	FieldSizeBytes    = 4
	FieldLastModified = 5
	FieldETag         = 6
	FieldUserMetadata = 7

	// FieldUserMetadataKey and FieldUserMetadataValue are the IDs of the map's key and value
	// types of user_metadata (§3.1). They follow the map field in the catalog's pre-order
	// renumbering: the key and value are user_metadata's children, so they take IDs 8 and 9 and
	// the trailing columns continue at 10. The value is optional (§3.1); an empty map - never a
	// null - is how an object without user metadata is stored.
	FieldUserMetadataKey   = 8
	FieldUserMetadataValue = 9

	// The trailing optional columns. Their IDs skip the map's key and value, which come before
	// them in the created table's pre-order numbering (§3.1).
	FieldCommitter    = 10
	FieldContentType  = 11
	FieldCreationDate = 12
)

// The table property names of §3.1 and §4.3.
const (
	// PropertySourceCommit is the property carried by the table and by every snapshot's summary:
	// the full commit ID of the data repository that the table state describes (§3.1, §4.3). It
	// is the indexer's position (§3.3: "the indexing position of branch b is the
	// lakefs.mds.source-commit metadata of R-metadata@b") and the freshness value users compare
	// against the branch head (§9).
	PropertySourceCommit = "lakefs.mds.source-commit"

	// PropertySourceRepository is the property carried by every snapshot of the table: the name
	// of the data repository the commit belongs to (§4.3 step 3).
	PropertySourceRepository = "lakefs.mds.source-repository"

	// PropertySchemaVersion is the version of the fixed schema above (§3.1). It is set at
	// creation and only changes when a schema change is introduced.
	PropertySchemaVersion = "lakefs.mds.schema-version"

	// PropertyRepository is the data repository the table belongs to, set at creation (§3.1).
	PropertyRepository = "lakefs.mds.repository"

	// SchemaVersion is the value of PropertySchemaVersion this package writes (§3.1).
	SchemaVersion = "1"

	// PropertyWriteDeleteMode, PropertyWriteUpdateMode and PropertyWriteCompressionCodec are the
	// Iceberg properties the table is created with (§3.1, §10 decision 2):
	//
	//   - merge-on-read deletes write position delete files, which PyIceberg, DuckDB, Spark and
	//     Trino all read; equality deletes are not read by PyIceberg, so they are not used;
	//   - zstd keeps the data files small.
	//
	// They are the iceberg-go property keys (table.WriteDeleteModeKey and friends), spelled here
	// so that a reader of the schema does not have to know iceberg-go's source.
	PropertyWriteDeleteMode       = "write.delete.mode"
	PropertyWriteUpdateMode       = "write.update.mode"
	PropertyWriteCompressionCodec = "write.parquet.compression-codec"
	WriteModeMergeOnRead          = "merge-on-read"
	WriteModeCopyOnWrite          = "copy-on-write"
	WriteCompressionCodecZSTD     = "zstd"
)

// Schema returns the fixed Iceberg schema of system.object_metadata (§3.1):
//
//	1  repository     string            required
//	2  path           string            required   (identifier field)
//	3  commit_id      string            required
//	4  size_bytes     long              required
//	5  last_modified  timestamptz       required
//	6  etag           string            required
//	7  user_metadata  map<string,string> required  (key 8, value 9)
//	10 committer      string            optional
//	11 content_type   string            optional
//	12 creation_date  timestamptz       optional
//
// The identifier is the path field alone, as §3.1 fixes it. The returned schema is a fresh value
// on every call, so a caller may not rely on pointer identity.
//
// These are the IDs of the created table. The catalog assigns them itself when it applies the
// request (iceberg-go's NewMetadataWithUUID calls AssignFreshSchemaIDs, which renumbers every ID in
// pre-order), so this request is built in exactly that pre-order: the map's key and value are
// user_metadata's children and take 8 and 9, and the trailing optional columns take 10, 11 and 12.
// The result is that the renumbering leaves the request's IDs unchanged, and the constants above
// are the IDs a reader and the writer find on the created table. They still take them from the
// loaded table (Table.Schema()) rather than from the constants, because the catalog owns the
// numbering; the constants are how the create request and the compatibility surface are written
// down, and TestCreateTableFieldIDs pins that both agree.
func Schema() *iceberg.Schema {
	return iceberg.NewSchemaWithIdentifiers(0, []int{FieldPath},
		iceberg.NestedField{
			ID: FieldRepository, Name: ColumnRepository, Type: iceberg.StringType{}, Required: true,
		},
		iceberg.NestedField{
			ID: FieldPath, Name: ColumnPath, Type: iceberg.StringType{}, Required: true,
		},
		iceberg.NestedField{
			ID: FieldCommitID, Name: ColumnCommitID, Type: iceberg.StringType{}, Required: true,
		},
		iceberg.NestedField{
			ID: FieldSizeBytes, Name: ColumnSizeBytes, Type: iceberg.Int64Type{}, Required: true,
		},
		iceberg.NestedField{
			ID: FieldLastModified, Name: ColumnLastModified, Type: iceberg.TimestampTzType{}, Required: true,
		},
		iceberg.NestedField{
			ID: FieldETag, Name: ColumnETag, Type: iceberg.StringType{}, Required: true,
		},
		iceberg.NestedField{
			ID: FieldUserMetadata, Name: ColumnUserMetadata, Required: true,
			Type: &iceberg.MapType{
				KeyID:     FieldUserMetadataKey,
				KeyType:   iceberg.StringType{},
				ValueID:   FieldUserMetadataValue,
				ValueType: iceberg.StringType{},
				// Design §3.1 declares the map value optional: iceberg-go v0.7.0 reads a parquet
				// map column with nullable items, so a required value makes its own copy-on-write
				// rewrite and compaction fail with "record schema does not match writer's". The
				// indexer never writes a null value, and an object without user metadata gets an
				// empty map - never a null - so rows are unaffected.
				ValueRequired: false,
			},
		},
		iceberg.NestedField{
			ID: FieldCommitter, Name: ColumnCommitter, Type: iceberg.StringType{}, Required: false,
		},
		iceberg.NestedField{
			ID: FieldContentType, Name: ColumnContentType, Type: iceberg.StringType{}, Required: false,
		},
		iceberg.NestedField{
			ID: FieldCreationDate, Name: ColumnCreationDate, Type: iceberg.TimestampTzType{}, Required: false,
		},
	)
}

// SortOrder returns the table's sort order: path ascending, nulls first (§3.1 "sort order path
// ASC"). It is applied at creation and records the order the writer writes rows in; the actual
// sorting of each record batch is NewRecordReader's job, because Iceberg only records the order in
// the metadata.
func SortOrder() (icebergtable.SortOrder, error) {
	return icebergtable.NewSortOrder(1, []icebergtable.SortField{{
		SourceIDs: []int{FieldPath},
		Transform: iceberg.IdentityTransform{},
		Direction: icebergtable.SortASC,
		NullOrder: icebergtable.NullsFirst,
	}})
}

// Properties returns the table properties of a new table of data repository repository (§3.1):
// the merge-on-read write modes, zstd, the schema version and the data repository name.
//
// Every other property the table needs is written by the indexer as it commits: the
// PropertySourceCommit property is updated on every indexed commit (§4.3 step 3), so it is not set
// at creation. The caller may extend the returned map; extra properties (a format-version above 2
// is refused by the catalog) are passed to the create request as they are.
func Properties(repository string) iceberg.Properties {
	return iceberg.Properties{
		PropertyWriteDeleteMode:       WriteModeMergeOnRead,
		PropertyWriteUpdateMode:       WriteModeMergeOnRead,
		PropertyWriteCompressionCodec: WriteCompressionCodecZSTD,
		PropertySchemaVersion:         SchemaVersion,
		PropertyRepository:            repository,
	}
}

// SnapshotProperties returns the properties every snapshot the indexer writes carries (§4.3 step
// 3): the source commit and the data repository it belongs to.
func SnapshotProperties(repository, sourceCommit string) iceberg.Properties {
	return iceberg.Properties{
		PropertySourceCommit:     sourceCommit,
		PropertySourceRepository: repository,
	}
}
