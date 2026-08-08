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

	// commit 成功后执行回收策略(5 上限 / 成员离开即删)
	_ = s.EnforceRetention(ctx, req.MatchId)

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

// 回收策略常量
const (
	// 每个 match 保留的最近 checkpoint 数量上限
	MaxCheckpointsPerMatch = 5
	// 未确认全部成员离开时的强制删除 TTL(24h)
	CheckpointForceTTL = 24 * time.Hour
)

// EnforceRetention 在 commit 后执行回收:
//  1. 该 match 的 checkpoints 超过 MaxCheckpointsPerMatch → 删最旧
//  2. 该 match 所有成员已离开(match_peers 空)→ 删全部
func (s *Service) EnforceRetention(ctx context.Context, matchID string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 规则 2:成员全部离开 → 删全部
	var peerCount int
	err = tx.QueryRow(ctx,
		`SELECT count(*) FROM match_peers WHERE match_id = $1`, matchID).Scan(&peerCount)
	if err != nil {
		return err
	}
	if peerCount == 0 {
		rows, err := tx.Query(ctx,
			`SELECT object_key FROM checkpoints WHERE match_id = $1`, matchID)
		if err != nil {
			return err
		}
		var keys []string
		for rows.Next() {
			var k string
			if err := rows.Scan(&k); err != nil {
				rows.Close()
				return err
			}
			keys = append(keys, k)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx,
			`DELETE FROM checkpoints WHERE match_id = $1`, matchID); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		// 事务外删 MinIO object
		for _, k := range keys {
			_ = s.minio.RemoveObject(ctx, s.bucket, k, minio.RemoveObjectOptions{})
		}
		return nil
	}

	// 规则 1:超过数量上限 → 删最旧(只删元数据行,文件等后台 TTL 清)
	var overCount int
	err = tx.QueryRow(ctx,
		`SELECT count(*) - $2 FROM checkpoints WHERE match_id = $1`,
		matchID, MaxCheckpointsPerMatch).Scan(&overCount)
	if err != nil {
		return err
	}
	if overCount > 0 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM checkpoints
			WHERE match_id = $1
			AND (authority_epoch, snapshot_revision) IN (
				SELECT authority_epoch, snapshot_revision
				FROM checkpoints
				WHERE match_id = $1
				ORDER BY authority_epoch DESC, snapshot_revision DESC
				OFFSET $2
			)`, matchID, MaxCheckpointsPerMatch); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// RunRetentionLoop 后台定时清理过期 checkpoint(24h 兜底)。
// 每 1 小时执行一次;过期 = expires_at < now()。
func (s *Service) RunRetentionLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rows, err := s.pool.Query(ctx,
				`SELECT object_key FROM checkpoints WHERE expires_at < now()`)
			if err != nil {
				continue
			}
			var keys []string
			for rows.Next() {
				var k string
				if err := rows.Scan(&k); err != nil {
					rows.Close()
					break
				}
				keys = append(keys, k)
			}
			rows.Close()
			if _, err := s.pool.Exec(ctx,
				`DELETE FROM checkpoints WHERE expires_at < now()`); err != nil {
				continue
			}
			for _, k := range keys {
				_ = s.minio.RemoveObject(ctx, s.bucket, k, minio.RemoveObjectOptions{})
			}
		}
	}
}
