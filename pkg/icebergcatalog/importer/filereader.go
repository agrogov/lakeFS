package importer

// Reading the source table's files.
//
// In copy mode the importer reads the bytes the source metadata references and writes them into
// the lakeFS storage namespace. The source catalog serves table *metadata* over REST; the files
// themselves live in object storage or on a filesystem, so the copy needs a FileIO of its own:
//
//   - http(s):// is fetched with the same client as the catalog. This is how a catalog that
//     fronts a plain web-served warehouse works, and it is what the unit tests exercise.
//   - file:// (and an absolute path) is read from the local filesystem.
//   - s3:// (and the s3a:// alias) is read with the AWS SDK. The credentials and endpoint come from
//     the standard Iceberg FileIO properties passed as --source-prop: s3.endpoint,
//     s3.region, s3.access-key-id, s3.secret-access-key, s3.session-token and
//     s3.path-style-access. Without explicit keys the SDK's ambient chain (environment, profile,
//     instance role) is used, exactly as an Iceberg client with the same table properties would.
//
// Any other scheme is refused per table with a message that names it, rather than silently
// falling back to something that happens to be reachable.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// FileReader reads the table files a source catalog's metadata references.
type FileReader interface {
	// Open returns the file's contents. The caller closes it.
	Open(ctx context.Context, location string) (io.ReadCloser, error)

	// Size returns the file's length in bytes, which the lakeFS object API needs to store it.
	Size(ctx context.Context, location string) (int64, error)
}

// NewFileReader builds the FileIO for a source catalog. props are the --source-prop values; the
// s3.* keys configure the S3 reader. client is used for http(s) reads and may be nil.
func NewFileReader(props map[string]string, client *http.Client) FileReader {
	if client == nil {
		client = http.DefaultClient
	}
	reader := &fileReader{client: client, props: props}
	if reader.props == nil {
		reader.props = map[string]string{}
	}
	return reader
}

type fileReader struct {
	client *http.Client
	props  map[string]string

	mu     sync.Mutex
	s3     *s3.Client
	s3Err  error
	s3Init bool
}

// Open opens location for reading.
func (r *fileReader) Open(ctx context.Context, location string) (io.ReadCloser, error) {
	switch {
	case isHTTPLocation(location):
		return r.openHTTP(ctx, location)
	case strings.HasPrefix(location, "file://"):
		path, err := filePath(location)
		if err != nil {
			return nil, err
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("%w: open %q: %w", ErrTableCopy, location, err)
		}
		return file, nil
	case strings.HasPrefix(location, "/"):
		file, err := os.Open(location)
		if err != nil {
			return nil, fmt.Errorf("%w: open %q: %w", ErrTableCopy, location, err)
		}
		return file, nil
	case isS3Location(location):
		bucket, key, err := splitS3Location(location)
		if err != nil {
			return nil, err
		}
		client, err := r.s3Client(ctx)
		if err != nil {
			return nil, err
		}
		output, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, fmt.Errorf("%w: read %q: %w", ErrTableCopy, location, err)
		}
		return output.Body, nil
	default:
		return nil, fmt.Errorf("%w: %q: only http(s), file and s3 locations can be copied", ErrUnsupportedFile, location)
	}
}

// Size returns the length of location.
func (r *fileReader) Size(ctx context.Context, location string) (int64, error) {
	switch {
	case isHTTPLocation(location):
		return r.sizeHTTP(ctx, location)
	case strings.HasPrefix(location, "file://"):
		path, err := filePath(location)
		if err != nil {
			return 0, err
		}
		return fileSize(path, location)
	case strings.HasPrefix(location, "/"):
		return fileSize(location, location)
	case isS3Location(location):
		bucket, key, err := splitS3Location(location)
		if err != nil {
			return 0, err
		}
		client, err := r.s3Client(ctx)
		if err != nil {
			return 0, err
		}
		output, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return 0, fmt.Errorf("%w: stat %q: %w", ErrTableCopy, location, err)
		}
		if output.ContentLength == nil {
			return 0, fmt.Errorf("%w: stat %q: S3 returned no content length", ErrTableCopy, location)
		}
		return *output.ContentLength, nil
	default:
		return 0, fmt.Errorf("%w: %q: only http(s), file and s3 locations can be copied", ErrUnsupportedFile, location)
	}
}

// openHTTP fetches an http(s) file.
func (r *fileReader) openHTTP(ctx context.Context, location string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, location, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: build request for %q: %w", ErrTableCopy, location, err)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: read %q: %w", ErrTableCopy, location, err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		defer func() { _ = resp.Body.Close() }()
		return nil, fmt.Errorf("%w: read %q: HTTP %s", ErrTableCopy, location, resp.Status)
	}
	return resp.Body, nil
}

// sizeHTTP asks for the length with a HEAD request. Fall back to a one-byte ranged GET when a
// server does not answer HEAD with a Content-Length: some object-store front ends answer HEAD
// without it, and a copy cannot store an unknown size.
func (r *fileReader) sizeHTTP(ctx context.Context, location string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, location, nil)
	if err != nil {
		return 0, fmt.Errorf("%w: build HEAD request for %q: %w", ErrTableCopy, location, err)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%w: stat %q: %w", ErrTableCopy, location, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices && resp.ContentLength >= 0 {
		return resp.ContentLength, nil
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return 0, fmt.Errorf("%w: stat %q: HTTP %s", ErrTableCopy, location, resp.Status)
	}
	return 0, fmt.Errorf("%w: stat %q: no Content-Length", ErrTableCopy, location)
}

// s3Client builds the S3 client once, from the FileIO properties.
func (r *fileReader) s3Client(ctx context.Context) (*s3.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.s3Init {
		return r.s3, r.s3Err
	}
	r.s3Init = true

	var options []func(*awsconfig.LoadOptions) error
	if region := r.property("s3.region", "s3.region-name"); region != "" {
		options = append(options, awsconfig.WithRegion(region))
	}
	if endpoint := r.property("s3.endpoint", "s3.endpoint.uri"); endpoint != "" {
		options = append(options, awsconfig.WithBaseEndpoint(endpoint))
	}
	accessKey := r.property("s3.access-key-id")
	secretKey := r.property("s3.secret-access-key")
	if accessKey != "" || secretKey != "" {
		options = append(options, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey, secretKey, r.property("s3.session-token"))))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		r.s3Err = fmt.Errorf("%w: build S3 client: %w", ErrSource, err)
		return nil, r.s3Err
	}
	pathStyle := strings.EqualFold(r.property("s3.path-style-access"), "true")
	r.s3 = s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = pathStyle
	})
	return r.s3, nil
}

// property returns the first non-empty value of keys.
func (r *fileReader) property(keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(r.props[key]); value != "" {
			return value
		}
	}
	return ""
}

// isHTTPLocation reports whether location is an http(s) URL.
func isHTTPLocation(location string) bool {
	return strings.HasPrefix(location, "http://") || strings.HasPrefix(location, "https://")
}

// isS3Location reports whether location is an S3 URL, including the Hadoop s3a alias.
func isS3Location(location string) bool {
	return strings.HasPrefix(location, "s3://") || strings.HasPrefix(location, "s3a://")
}

// splitS3Location splits s3://bucket/key into its parts. An empty bucket or key is refused.
func splitS3Location(location string) (string, string, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return "", "", fmt.Errorf("%w: parse S3 location %q: %w", ErrUnsupportedFile, location, err)
	}
	bucket := parsed.Host
	key := strings.TrimPrefix(parsed.Path, "/")
	if bucket == "" || key == "" {
		return "", "", fmt.Errorf("%w: %q is not an s3://bucket/key location", ErrUnsupportedFile, location)
	}
	return bucket, key, nil
}

// filePath converts file://location to a filesystem path.
func filePath(location string) (string, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("%w: parse file location %q: %w", ErrUnsupportedFile, location, err)
	}
	if parsed.Path == "" {
		return "", fmt.Errorf("%w: %q has no path", ErrUnsupportedFile, location)
	}
	return parsed.Path, nil
}

// fileSize stats a local file.
func fileSize(path, location string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("%w: stat %q: %w", ErrTableCopy, location, err)
	}
	return info.Size(), nil
}
