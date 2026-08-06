// Package checkpoint implements central checkpoint storage: the host
// uploads serialized SaveGame blobs to MinIO (via presigned PUT) and the
// coordinator commits metadata to PostgreSQL only after verifying the
// object's hash and size.
//
// Object key: matches/{matchId}/epochs/{authorityEpoch}/snapshots/{revision}-{sha256}.sav
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

var (
	ErrMatchNotFound = errors.New("match not found")
	ErrNotHost       = errors.New("only the current host may commit checkpoints")
	ErrEpochStale    = errors.New("checkpoint epoch is stale")
	ErrHashMismatch  = errors.New("payload hash mismatch")
	ErrSizeMismatch  = errors.New("payload size mismatch")
	ErrConflict      = errors.New("checkpoint revision conflict")
)

// CommitRequest is the metadata the host submits after uploading the blob.
type CommitRequest struct {
	MatchId             string `json:"matchId"`
	AuthorityEpoch      uint64 `json:"authorityEpoch"`
	RosterRevision      uint64 `json:"rosterRevision"`
	SnapshotRevision    uint64 `json:"snapshotRevision"`
	PayloadHash         string `json:"payloadHash"`  // hex sha256 of blob
	PayloadSize         int64  `json:"payloadSize"`  // bytes
	SaveFormatVersion   int    `json:"saveFormatVersion"`
	SnapshotSchemaVersion int  `json:"snapshotSchemaVersion"`
}

// CommitResult describes the committed checkpoint.
type CommitResult struct {
	ObjectKey string    `json:"key"`
	CommittedAt time.Time `json:"committedAt"`
}

type Service struct {
	pool   *pgxpool.Pool
	minio  *minio.Client
	bucket string
}

func NewService(pool *pgxpool.Pool, mc *minio.Client, bucket string) *Service {
	return &Service{pool: pool, minio: mc, bucket: bucket}
}

// PresignedUploadURL returns a PUT URL the host uses to upload the blob.
func (s *Service) PresignedUploadURL(ctx context.Context, matchID string, authorityEpoch, snapshotRevision uint64, payloadHash string) (string, string, error) {
	objKey := objectKey(matchID, authorityEpoch, snapshotRevision, payloadHash)
	url, err := s.minio.PresignedPutObject(ctx, s.bucket, objKey, 15*time.Minute)
	if err != nil {
		return "", "", err
	}
	return url.String(), objKey, nil
}

// Commit verifies the uploaded blob (hash + size) against MinIO, then
// atomically records metadata in PostgreSQL. Same revision with a different
// hash fails closed.
func (s *Service) Commit(ctx context.Context, req CommitRequest, deviceID string) (CommitResult, error) {
	objKey := objectKey(req.MatchId, req.AuthorityEpoch, req.SnapshotRevision, req.PayloadHash)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return CommitResult{}, err
	}
	defer tx.Rollback(ctx)

	// match + current epoch + host check inside the transaction
	var currentEpoch uint64
	err = tx.QueryRow(ctx,
		`SELECT authority_epoch FROM matches WHERE match_id = $1`, req.MatchId).Scan(&currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return CommitResult{}, ErrMatchNotFound
	}
	if err != nil {
		return CommitResult{}, err
	}
	if req.AuthorityEpoch != currentEpoch {
		return CommitResult{}, ErrEpochStale
	}

	// the committing device must be the current lease holder
	var hostDevice string
	err = tx.QueryRow(ctx,
		`SELECT host_device_id FROM leases WHERE match_id = $1`, req.MatchId).Scan(&hostDevice)
	if errors.Is(err, pgx.ErrNoRows) {
		return CommitResult{}, ErrNotHost
	}
	if err != nil {
		return CommitResult{}, err
	}
	if hostDevice != deviceID {
		return CommitResult{}, ErrNotHost
	}

	// same revision + different hash must fail closed
	var existingHash string
	err = tx.QueryRow(ctx,
		`SELECT payload_hash FROM checkpoints WHERE match_id = $1 AND authority_epoch = $2 AND snapshot_revision = $3`,
		req.MatchId, req.AuthorityEpoch, req.SnapshotRevision).Scan(&existingHash)
	if err == nil {
		if existingHash != req.PayloadHash {
			return CommitResult{}, ErrConflict
		}
		// idempotent re-commit: same hash, already committed
		if err := tx.Commit(ctx); err != nil {
			return CommitResult{}, err
		}
		obj := objectKey(req.MatchId, req.AuthorityEpoch, req.SnapshotRevision, existingHash)
		return CommitResult{ObjectKey: obj, CommittedAt: time.Now()}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CommitResult{}, err
	}

	// verify blob in MinIO: size + sha256
	if err := s.verifyBlob(ctx, objKey, req.PayloadSize, req.PayloadHash); err != nil {
		return CommitResult{}, err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO checkpoints
			(match_id, authority_epoch, roster_revision, snapshot_revision,
			 object_key, payload_hash, payload_size,
			 save_format_version, snapshot_schema_version, committed_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		req.MatchId, req.AuthorityEpoch, req.RosterRevision, req.SnapshotRevision,
		objKey, req.PayloadHash, req.PayloadSize,
		req.SaveFormatVersion, req.SnapshotSchemaVersion, deviceID)
	if err != nil {
		return CommitResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return CommitResult{}, err
	}
	return CommitResult{ObjectKey: objKey, CommittedAt: time.Now()}, nil
}

// Latest returns the most recent committed checkpoint metadata for a match.
func (s *Service) Latest(ctx context.Context, matchID string) (*CommitRequest, CommitResult, error) {
	var req CommitRequest
	var res CommitResult
	err := s.pool.QueryRow(ctx, `
		SELECT match_id, authority_epoch, roster_revision, snapshot_revision,
		       payload_hash, payload_size, save_format_version, snapshot_schema_version,
		       object_key, committed_at
		FROM checkpoints WHERE match_id = $1
		ORDER BY authority_epoch DESC, snapshot_revision DESC LIMIT 1`, matchID).
		Scan(&req.MatchId, &req.AuthorityEpoch, &req.RosterRevision, &req.SnapshotRevision,
			&req.PayloadHash, &req.PayloadSize, &req.SaveFormatVersion, &req.SnapshotSchemaVersion,
			&res.ObjectKey, &res.CommittedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, CommitResult{}, nil
	}
	return &req, res, err
}

// verifyBlob streams the object from MinIO and checks size and sha256.
func (s *Service) verifyBlob(ctx context.Context, objKey string, wantSize int64, wantHash string) error {
	obj, err := s.minio.GetObject(ctx, s.bucket, objKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()

	h := sha256.New()
	n, err := io.Copy(h, obj)
	if err != nil {
		return fmt.Errorf("stream object: %w", err)
	}
	if n != wantSize {
		return ErrSizeMismatch
	}
	if hex.EncodeToString(h.Sum(nil)) != wantHash {
		return ErrHashMismatch
	}
	return nil
}

func objectKey(matchID string, epoch, revision uint64, hash string) string {
	return fmt.Sprintf("matches/%s/epochs/%d/snapshots/%d-%s.sav", matchID, epoch, revision, hash)
}

// ensureBucket creates the checkpoint bucket if missing.
func (s *Service) EnsureBucket(ctx context.Context) error {
	exists, err := s.minio.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return s.minio.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
	}
	return nil
}
