package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/treeverse/lakefs/pkg/block"
)

const (
	// maxS3DeleteObjects is the DeleteObjects request limit (API_DeleteObjects).
	maxS3DeleteObjects = 1000
)

// DeleteNamespacePage deletes one page of the objects under storageNamespace with ListObjectsV2
// and DeleteObjects (Quiet, at most 1000 keys per request). Per-key DeleteObjects errors fail the
// page with the first error, so an AccessDenied response surfaces instead of looking like success
// (REPO_DELETE_STORAGE_DESIGN.md section 2.3). When the listing is done, incomplete multipart
// uploads under the prefix are aborted too: they are billed and are not listed as objects.
func (a *Adapter) DeleteNamespacePage(ctx context.Context, storageID, storageNamespace string, opts block.DeleteNamespaceOpts) (*block.DeleteNamespaceResult, error) {
	var err error
	defer reportMetrics("DeleteNamespacePage", storageID, time.Now(), nil, &err)

	root, prefix, err := block.NamespaceDeletePrefix(storageNamespace)
	if err != nil {
		return nil, err
	}
	bucketURL, err := url.Parse(root)
	if err != nil {
		return nil, fmt.Errorf("%w: parse namespace root %q: %w", ErrS3, root, err)
	}
	bucket := bucketURL.Host

	limit := opts.Limit
	if limit <= 0 || limit > maxS3DeleteObjects {
		limit = maxS3DeleteObjects
	}
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(int32(limit)), //nolint:gosec // limit is capped at maxS3DeleteObjects
	}
	if opts.After != "" {
		input.StartAfter = aws.String(prefix + opts.After)
	}

	client := a.clients.Get(ctx, bucket)
	listOutput, err := client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("%w: list objects under %q: %w", ErrS3, prefix, err)
	}

	result := &block.DeleteNamespaceResult{
		LastKey: opts.After,
		Done:    !aws.ToBool(listOutput.IsTruncated),
	}
	deleteObjects := make([]types.ObjectIdentifier, 0, len(listOutput.Contents))
	for _, object := range listOutput.Contents {
		key := aws.ToString(object.Key)
		// The second guard (REPO_DELETE_STORAGE_DESIGN.md section 2.2): a listing bug must fail the
		// page rather than delete outside the namespace.
		if !strings.HasPrefix(key, prefix) {
			return nil, fmt.Errorf("%w: %q", block.ErrNamespaceDeleteOutsidePrefix, key)
		}
		relativeKey := strings.TrimPrefix(key, prefix)
		result.LastKey = relativeKey
		if isKeptDeleteNamespaceKey(relativeKey, opts.Keep) {
			continue
		}
		deleteObjects = append(deleteObjects, types.ObjectIdentifier{Key: aws.String(key)})
	}

	if len(deleteObjects) > 0 {
		deleteOutput, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: deleteObjects,
				Quiet:   aws.Bool(true),
			},
		})
		if err != nil {
			return nil, fmt.Errorf("%w: delete objects under %q: %w", ErrS3, prefix, err)
		}
		if len(deleteOutput.Errors) > 0 {
			first := deleteOutput.Errors[0]
			return nil, fmt.Errorf("%w: delete object %q: %s: %s", ErrS3,
				aws.ToString(first.Key), aws.ToString(first.Code), aws.ToString(first.Message))
		}
		result.Deleted = len(deleteObjects)
	}

	if result.Done {
		if err := a.abortIncompleteMultipartUploads(ctx, client, bucket, prefix); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (a *Adapter) abortIncompleteMultipartUploads(ctx context.Context, client *s3.Client, bucket, prefix string) error {
	var (
		keyMarker      *string
		uploadIDMarker *string
	)
	for {
		output, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket:         aws.String(bucket),
			Prefix:         aws.String(prefix),
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIDMarker,
		})
		if err != nil {
			return fmt.Errorf("%w: list multipart uploads under %q: %w", ErrS3, prefix, err)
		}
		for _, upload := range output.Uploads {
			key := aws.ToString(upload.Key)
			if !strings.HasPrefix(key, prefix) {
				return fmt.Errorf("%w: multipart upload %q", block.ErrNamespaceDeleteOutsidePrefix, key)
			}
			if _, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      upload.Key,
				UploadId: upload.UploadId,
			}); err != nil {
				return fmt.Errorf("%w: abort multipart upload of %q: %w", ErrS3, key, err)
			}
		}
		if !aws.ToBool(output.IsTruncated) {
			return nil
		}
		keyMarker = output.NextKeyMarker
		uploadIDMarker = output.NextUploadIdMarker
	}
}

func isKeptDeleteNamespaceKey(key string, keep []string) bool {
	for _, kept := range keep {
		if key == kept {
			return true
		}
	}
	return false
}
