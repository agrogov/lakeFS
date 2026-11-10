package catalog

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// RepositoryDeletionsPartition is the global KV partition that holds repository storage
	// deletion records.  It is deliberately not the repository partition: the metadata delete
	// wipes that partition, and the record has to outlive it (design §3.1).
	RepositoryDeletionsPartition = "repository-deletions"

	// repositoryDeletionsPathPrefix is the key prefix inside RepositoryDeletionsPartition.  The
	// full key is "<prefix>/<repository id>", one record per repository name.
	repositoryDeletionsPathPrefix = "deletions"

	// repositoryDeletionsScanPrefix bounds the scan used by ListRepositoryDeletions to the record
	// keys, and nothing else, under the partition.
	repositoryDeletionsScanPrefix = repositoryDeletionsPathPrefix + kv.PathDelimiter
)

var (
	// ErrRepositoryDeletionNotFound is returned by GetRepositoryDeletion when no record exists for
	// the repository name.  It also matches graveler.ErrNotFound, like a missing repository does,
	// so callers that map both to 404 need one check.
	ErrRepositoryDeletionNotFound = fmt.Errorf("repository deletion not found: %w", graveler.ErrNotFound)

	// ErrDeletionInProgress is returned when a storage deletion record already exists for a
	// repository name, or when repository creation collides with a pending deletion (R11, R16).
	ErrDeletionInProgress = errors.New("repository deletion in progress")

	// ErrInvalidRepositoryDeletionRecord is returned when a KV entry under the deletion partition
	// does not decode to a RepositoryDeletionData.
	ErrInvalidRepositoryDeletionRecord = errors.New("invalid repository deletion record")

	// ErrDeletionNotFailed is returned when a retry is asked for a record that is not FAILED.
	ErrDeletionNotFailed = errors.New("repository deletion is not failed")
)

//nolint:gochecknoinits
func init() {
	kv.MustRegisterType(RepositoryDeletionsPartition, repositoryDeletionsPathPrefix, (&RepositoryDeletionData{}).ProtoReflect().Type())
}

// RepositoryDeletion is the API-facing and worker-facing form of a repository storage deletion
// record (R14).  Times are zero when the corresponding proto timestamp is unset.
type RepositoryDeletion struct {
	Repository        string
	StorageID         string
	StorageNamespace  string
	State             RepositoryDeletionState
	Step              RepositoryDeletionStep
	Marker            string
	ObjectsDeleted    int64
	RequestedBy       string
	RequestedAt       time.Time
	UpdatedAt         time.Time
	MetadataDeletedAt time.Time
	NextAttemptAt     time.Time
	Attempts          int32
	LastError         string
	Force             bool
	CascadeOf         string
}

// RepositoryDeletionKey returns the KV key of a repository's deletion record.
func RepositoryDeletionKey(repository string) []byte {
	return []byte(kv.FormatPath(repositoryDeletionsPathPrefix, repository))
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func timeFromTimestamp(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func (d *RepositoryDeletion) toProto() *RepositoryDeletionData {
	return &RepositoryDeletionData{
		Repository:        d.Repository,
		StorageId:         d.StorageID,
		StorageNamespace:  d.StorageNamespace,
		State:             d.State,
		Step:              d.Step,
		Marker:            d.Marker,
		ObjectsDeleted:    d.ObjectsDeleted,
		RequestedBy:       d.RequestedBy,
		RequestedAt:       timestampOrNil(d.RequestedAt),
		UpdatedAt:         timestampOrNil(d.UpdatedAt),
		MetadataDeletedAt: timestampOrNil(d.MetadataDeletedAt),
		NextAttemptAt:     timestampOrNil(d.NextAttemptAt),
		Attempts:          d.Attempts,
		LastError:         d.LastError,
		Force:             d.Force,
		CascadeOf:         d.CascadeOf,
	}
}

func repositoryDeletionFromProto(data *RepositoryDeletionData) *RepositoryDeletion {
	return &RepositoryDeletion{
		Repository:        data.GetRepository(),
		StorageID:         data.GetStorageId(),
		StorageNamespace:  data.GetStorageNamespace(),
		State:             data.GetState(),
		Step:              data.GetStep(),
		Marker:            data.GetMarker(),
		ObjectsDeleted:    data.GetObjectsDeleted(),
		RequestedBy:       data.GetRequestedBy(),
		RequestedAt:       timeFromTimestamp(data.GetRequestedAt()),
		UpdatedAt:         timeFromTimestamp(data.GetUpdatedAt()),
		MetadataDeletedAt: timeFromTimestamp(data.GetMetadataDeletedAt()),
		NextAttemptAt:     timeFromTimestamp(data.GetNextAttemptAt()),
		Attempts:          data.GetAttempts(),
		LastError:         data.GetLastError(),
		Force:             data.GetForce(),
		CascadeOf:         data.GetCascadeOf(),
	}
}

// CreateRepositoryDeletion writes a new record with a nil predicate, so the write fails with
// kv.ErrPredicateFailed if a record for the repository name already exists (design §3.2, R11).
func (c *Catalog) CreateRepositoryDeletion(ctx context.Context, d *RepositoryDeletion) error {
	now := time.Now().UTC()
	if d.RequestedAt.IsZero() {
		d.RequestedAt = now
	}
	d.UpdatedAt = now
	err := kv.SetMsgIf(ctx, c.KVStore, RepositoryDeletionsPartition, RepositoryDeletionKey(d.Repository), d.toProto(), nil)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return fmt.Errorf("repository %s: %w", d.Repository, ErrDeletionInProgress)
	}
	if err != nil {
		return fmt.Errorf("create repository deletion record for %s: %w", d.Repository, err)
	}
	return nil
}

func (c *Catalog) getRepositoryDeletionWithPredicate(ctx context.Context, repository string) (*RepositoryDeletion, kv.Predicate, error) {
	data := &RepositoryDeletionData{}
	predicate, err := kv.GetMsg(ctx, c.KVStore, RepositoryDeletionsPartition, RepositoryDeletionKey(repository), data)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return nil, nil, fmt.Errorf("repository %s: %w", repository, ErrRepositoryDeletionNotFound)
		}
		return nil, nil, fmt.Errorf("get repository deletion record for %s: %w", repository, err)
	}
	return repositoryDeletionFromProto(data), predicate, nil
}

// GetRepositoryDeletion returns the storage deletion record of a repository (R14).
func (c *Catalog) GetRepositoryDeletion(ctx context.Context, repository string) (*RepositoryDeletion, error) {
	d, _, err := c.getRepositoryDeletionWithPredicate(ctx, repository)
	return d, err
}

// ListRepositoryDeletions returns every storage deletion record, in repository name order (R14).
// Records are bounded by the number of repositories, so there is no pagination here; callers that
// expose a paginated listing slice the result.
func (c *Catalog) ListRepositoryDeletions(ctx context.Context) ([]*RepositoryDeletion, error) {
	it, err := kv.NewPrimaryIterator(ctx, c.KVStore, (&RepositoryDeletionData{}).ProtoReflect().Type(),
		RepositoryDeletionsPartition, []byte(repositoryDeletionsScanPrefix), kv.IteratorOptionsFrom([]byte(repositoryDeletionsScanPrefix)))
	if err != nil {
		return nil, fmt.Errorf("scan repository deletions: %w", err)
	}
	defer it.Close()

	var records []*RepositoryDeletion
	for it.Next() {
		data, ok := it.Entry().Value.(*RepositoryDeletionData)
		if !ok || data == nil {
			return nil, fmt.Errorf("scan repository deletions: %w", ErrInvalidRepositoryDeletionRecord)
		}
		records = append(records, repositoryDeletionFromProto(data))
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("scan repository deletions: %w", err)
	}
	return records, nil
}

// UpdateRepositoryDeletion writes d with the predicate from the caller's own read.  A lost race
// returns kv.ErrPredicateFailed: another owner advanced the record, so this writer must drop the
// record for this tick (design §3.2).
func (c *Catalog) UpdateRepositoryDeletion(ctx context.Context, d *RepositoryDeletion, predicate kv.Predicate) error {
	d.UpdatedAt = time.Now().UTC()
	err := kv.SetMsgIf(ctx, c.KVStore, RepositoryDeletionsPartition, RepositoryDeletionKey(d.Repository), d.toProto(), predicate)
	if err != nil {
		return fmt.Errorf("update repository deletion record for %s: %w", d.Repository, err)
	}
	return nil
}

// ResetRepositoryDeletionForRetry turns a FAILED record back into PENDING for a repeated delete
// request (R15, design §3.2).  The step and the marker are kept, so the deletion resumes where it
// stopped; attempts is reset, and the retry is due immediately.
func (c *Catalog) ResetRepositoryDeletionForRetry(ctx context.Context, d *RepositoryDeletion, predicate kv.Predicate) (*RepositoryDeletion, error) {
	if d.State != RepositoryDeletionState_FAILED {
		return nil, fmt.Errorf("repository %s: retry of a %s deletion record: %w", d.Repository, d.State, ErrDeletionNotFailed)
	}
	d.State = RepositoryDeletionState_PENDING
	d.Attempts = 0
	d.NextAttemptAt = time.Time{}
	if err := c.UpdateRepositoryDeletion(ctx, d, predicate); err != nil {
		return nil, err
	}
	return d, nil
}

// DeleteRepositoryDeletion deletes the record of a repository, used by the worker once a DONE
// record expires (R14: status is readable for 24 hours after the deletion ends).
func (c *Catalog) DeleteRepositoryDeletion(ctx context.Context, repository string) error {
	if err := c.KVStore.Delete(ctx, []byte(RepositoryDeletionsPartition), RepositoryDeletionKey(repository)); err != nil {
		return fmt.Errorf("delete repository deletion record for %s: %w", repository, err)
	}
	return nil
}
