package importer

// Rewriting the absolute paths inside Iceberg's Avro files.
//
// A copy moves files to a new root, but a manifest list and a manifest each store the locations of
// the files below them as absolute URIs: `manifest_path` in a manifest-list record, `file_path` in
// a manifest entry's data_file. Those strings are length-prefixed inside the Avro block, so they
// cannot be byte-patched; the file is decoded and re-encoded instead.
//
// The decoder is the generic one of github.com/twmb/avro's OCF reader (the Avro library
// iceberg-go itself depends on): a record decodes into map[string]any, unions decode to their
// natural Go value, and re-encoding the same map with the file's own embedded schema round-trips
// the document. Only two fields are touched, so schemas, partition values, bounds, counts and
// everything a newer format version adds are preserved without the importer knowing about them.
//
// Two size fields have to be updated rather than preserved:
//
//   - manifest_list.manifest_length must become the re-encoded manifest's length, because the file
//     it describes changed size;
//   - nothing else: a data file is copied byte for byte, so its file_size_in_bytes stays true.
//
// The files are Iceberg Avro OCFs; the codec is read from the file's own header and preserved
// (null, deflate, snappy and zstandard are the codecs the OCF format defines and Iceberg uses).

import (
	"bytes"
	"compress/flate"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/twmb/avro/ocf"
)

const (
	// maxAvroFile bounds one manifest or manifest-list file the copy buffers. Iceberg targets
	// small manifests (the spec's default target is 8 MiB), so a file above this is a table this
	// importer does not serve rather than a legitimate document; reading it whole is not an
	// option an unbounded limit would leave.
	maxAvroFile = 256 << 20

	// manifestEntryStatusExisting, ADDED and DELETED are the three values of a manifest entry's
	// `status` field (Iceberg table spec, "Manifests": 0 existing, 1 added, 2 deleted).
	manifestEntryStatusExisting = 0
	manifestEntryStatusAdded    = 1
	manifestEntryStatusDeleted  = 2

	// manifestEntryStatusUnknown is the importer's own marker for an entry whose status could not
	// be read. It is treated like ADDED for copying: copying an extra file is safe, missing one is
	// not.
	manifestEntryStatusUnknown = -1
)

// dataFileRef is one data file a manifest references, with its entry status so the copy can skip
// files whose entries are DELETED and which may legitimately no longer exist.
type dataFileRef struct {
	Location string
	Status   int
}

// rewriteManifestList re-encodes a manifest list with every `manifest_path` mapped by rewritePath
// and every `manifest_length` set to the re-encoded manifest's length from newLength. The paths
// passed to rewritePath and newLength are the original ones.
func rewriteManifestList(data []byte, rewritePath func(string) (string, error), newLength func(string) (int64, error)) ([]byte, error) {
	return transformAvro(data, func(record map[string]any) error {
		location, ok := record["manifest_path"].(string)
		if !ok || location == "" {
			return fmt.Errorf("%w: a manifest-list record carries no manifest_path", ErrTableCopy)
		}
		length, err := newLength(location)
		if err != nil {
			return err
		}
		rewritten, err := rewritePath(location)
		if err != nil {
			return err
		}
		record["manifest_path"] = rewritten
		record["manifest_length"] = length
		return nil
	})
}

// scanManifestList returns the original manifest locations of a manifest list without re-encoding
// it. The copy uses it to discover the manifests before it decides their new lengths.
func scanManifestList(data []byte) ([]string, error) {
	var locations []string
	err := readAvroRecords(data, func(record map[string]any) error {
		location, ok := record["manifest_path"].(string)
		if !ok || location == "" {
			return fmt.Errorf("%w: a manifest-list record carries no manifest_path", ErrTableCopy)
		}
		locations = append(locations, location)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return locations, nil
}

// rewriteManifest re-encodes a manifest with every data file's `file_path` mapped by rewritePath,
// and returns the original and new location of each referenced data file.
func rewriteManifest(data []byte, rewritePath func(string) (string, error)) ([]byte, []dataFileRef, error) {
	var refs []dataFileRef
	rewritten, err := transformAvro(data, func(record map[string]any) error {
		dataFile, ok := record["data_file"].(map[string]any)
		if !ok {
			return fmt.Errorf("%w: a manifest entry carries no data_file", ErrTableCopy)
		}
		location, ok := dataFile["file_path"].(string)
		if !ok || location == "" {
			return fmt.Errorf("%w: a manifest entry's data_file carries no file_path", ErrTableCopy)
		}
		rewritten, err := rewritePath(location)
		if err != nil {
			return err
		}
		status := manifestEntryStatus(record["status"])
		dataFile["file_path"] = rewritten
		refs = append(refs, dataFileRef{Location: location, Status: status})
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return rewritten, refs, nil
}

// manifestEntryStatus decodes a manifest entry's status. An absent or unrecognized value becomes
// manifestEntryStatusUnknown.
func manifestEntryStatus(value any) int {
	switch status := value.(type) {
	case int:
		return status
	case int32:
		return int(status)
	case int64:
		return int(status)
	case float64:
		return int(status)
	default:
		return manifestEntryStatusUnknown
	}
}

// transformAvro decodes every record of an Avro OCF, lets mutate change it, and re-encodes the
// stream with the file's own schema and codec.
func transformAvro(data []byte, mutate func(map[string]any) error) ([]byte, error) {
	reader, err := newAvroReader(data)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	options := []ocf.WriterOpt{}
	codec, err := avroCodec(reader.Metadata())
	if err != nil {
		return nil, err
	}
	if codec != nil {
		options = append(options, ocf.WithCodec(codec))
	}
	if metadata := userAvroMetadata(reader.Metadata()); len(metadata) > 0 {
		options = append(options, ocf.WithMetadata(metadata))
	}
	writer, err := ocf.NewWriter(&out, reader.Schema(), options...)
	if err != nil {
		return nil, fmt.Errorf("%w: write Avro file: %w", ErrTableCopy, err)
	}

	for {
		var value any
		err := reader.Decode(&value)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: decode Avro record: %w", ErrTableCopy, err)
		}
		record, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: Avro record is %T, not an object", ErrTableCopy, value)
		}
		if err := mutate(record); err != nil {
			return nil, err
		}
		if err := writer.Encode(record); err != nil {
			return nil, fmt.Errorf("%w: encode Avro record: %w", ErrTableCopy, err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("%w: close Avro file: %w", ErrTableCopy, err)
	}
	return out.Bytes(), nil
}

// readAvroRecords decodes every record of an Avro OCF and calls visit, without re-encoding.
func readAvroRecords(data []byte, visit func(map[string]any) error) error {
	reader, err := newAvroReader(data)
	if err != nil {
		return err
	}
	for {
		var value any
		err := reader.Decode(&value)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%w: decode Avro record: %w", ErrTableCopy, err)
		}
		record, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: Avro record is %T, not an object", ErrTableCopy, value)
		}
		if err := visit(record); err != nil {
			return err
		}
	}
}

// newAvroReader opens an Avro OCF with the memory bound the copy allows.
func newAvroReader(data []byte) (*ocf.Reader, error) {
	reader, err := ocf.NewReader(bytes.NewReader(data),
		ocf.WithMaxBlockBytes(maxAvroFile),
		ocf.WithMaxDecompressedBlockBytes(maxAvroFile))
	if err != nil {
		return nil, fmt.Errorf("%w: read Avro file: %w", ErrTableCopy, err)
	}
	return reader, nil
}

// userAvroMetadata returns the OCF header metadata the writer may carry over: everything except
// the avro.* keys, which the writer owns (and rejects if supplied).
func userAvroMetadata(metadata map[string][]byte) map[string][]byte {
	user := map[string][]byte{}
	for key, value := range metadata {
		if strings.HasPrefix(key, "avro.") {
			continue
		}
		user[key] = value
	}
	return user
}

// avroCodec returns the codec named by the OCF header, or nil for the null codec. An unknown codec
// is refused: re-encoding with a different codec would be legal but silently changes the file,
// and refusing points at the table that needs a newer importer.
func avroCodec(metadata map[string][]byte) (ocf.Codec, error) {
	name := string(metadata["avro.codec"])
	switch name {
	case "", "null":
		return nil, nil
	case "deflate":
		return ocf.DeflateCodec(flate.DefaultCompression), nil
	case "snappy":
		return ocf.SnappyCodec(), nil
	case "zstandard":
		codec, err := ocf.ZstdCodec(nil, nil)
		if err != nil {
			return nil, fmt.Errorf("%w: zstandard codec: %w", ErrTableCopy, err)
		}
		return codec, nil
	default:
		return nil, fmt.Errorf("%w: Avro codec %q is not supported", ErrTableCopy, name)
	}
}
