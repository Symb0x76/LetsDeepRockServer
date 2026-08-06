package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func testStore(t *testing.T) (*pgxpool.Pool, *minio.Client, string) {
	t.Helper()
	url := os.Getenv("COORDINATOR_TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://letsdeeprock:letsdeeprock123@127.0.0.1:5432/letsdeeprock"
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	mc, err := minio.New("127.0.0.1:9000", &minio.Options{
		Creds:  credentials.NewStaticV4("letsdeeprock", "letsdeeprock123", ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	bucket := "checkpoint-test"
	exists, _ := mc.BucketExists(context.Background(), bucket)
	if !exists {
		if err := mc.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("make bucket: %v", err)
		}
	}
	t.Cleanup(func() {
		// best-effort cleanup
		objs := mc.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Recursive: true})
		for o := range objs {
			_ = mc.RemoveObject(context.Background(), bucket, o.Key, minio.RemoveObjectOptions{})
		}
	})
	return pool, mc, bucket
}

// seedMatch creates a match at epoch 1 with hostDevice as the lease holder.
func seedMatch(t *testing.T, pool *pgxpool.Pool, deviceID string) string {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
		INSERT INTO devices (device_id, public_key) VALUES ($1, $2)
		ON CONFLICT (device_id) DO NOTHING`,
		deviceID, []byte("host-pub-"+t.Name()+"-00000000000000000000000000")); err != nil {
		t.Fatal(err)
	}
	var matchID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO matches (match_id, join_code, join_code_exp, host_device_id, authority_epoch, roster_revision, created_by)
		VALUES (gen_random_uuid()::text, gen_random_uuid()::text, now() + interval '1 hour', $1, 1, 0, $1)
		RETURNING match_id`, deviceID).Scan(&matchID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO match_peers (match_id, peer_id, device_id, role) VALUES ($1, $2, $2, 'Host')`,
		matchID, deviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO leases (match_id, host_device_id, host_peer_id, authority_epoch, expires_at)
		VALUES ($1, $2, $2, 1, now() + interval '1 hour')`, matchID, deviceID); err != nil {
		t.Fatal(err)
	}
	return matchID
}

func TestCommitVerifyAndLatest(t *testing.T) {
	pool, mc, bucket := testStore(t)
	svc := NewService(pool, mc, bucket)
	ctx := context.Background()

	host := "ckpt-host-" + t.Name()
	matchID := seedMatch(t, pool, host)

	blob := []byte("checkpoint-savegame-binary-data")
	sum := sha256.Sum256(blob)
	hash := hex.EncodeToString(sum[:])

	// upload via presigned URL
	url, key, err := svc.PresignedUploadURL(ctx, matchID, 1, 1, hash)
	if err != nil {
		t.Fatalf("presign: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(blob))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("upload via presigned url: %v %v", resp, err)
	}
	resp.Body.Close()

	// commit
	res, err := svc.Commit(ctx, CommitRequest{
		MatchId:               matchID,
		AuthorityEpoch:        1,
		RosterRevision:        0,
		SnapshotRevision:      1,
		PayloadHash:           hash,
		PayloadSize:           int64(len(blob)),
		SaveFormatVersion:     1,
		SnapshotSchemaVersion: 1,
	}, host)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if res.ObjectKey != key {
		t.Fatalf("object key mismatch: %s vs %s", res.ObjectKey, key)
	}

	// latest
	meta, _, err := svc.Latest(ctx, matchID)
	if err != nil || meta == nil {
		t.Fatalf("latest: %v %v", meta, err)
	}
	if meta.SnapshotRevision != 1 {
		t.Fatalf("latest revision should be 1, got %d", meta.SnapshotRevision)
	}

	// idempotent re-commit
	_, err = svc.Commit(ctx, CommitRequest{
		MatchId: matchID, AuthorityEpoch: 1, RosterRevision: 0, SnapshotRevision: 1,
		PayloadHash: hash, PayloadSize: int64(len(blob)),
		SaveFormatVersion: 1, SnapshotSchemaVersion: 1,
	}, host)
	if err != nil {
		t.Fatalf("re-commit should be idempotent: %v", err)
	}
}

func TestCommitConflictSameRevisionDifferentHash(t *testing.T) {
	pool, mc, bucket := testStore(t)
	svc := NewService(pool, mc, bucket)
	ctx := context.Background()

	host := "ckpt-host-" + t.Name()
	matchID := seedMatch(t, pool, host)

	blob := []byte("checkpoint-v1")
	sum := sha256.Sum256(blob)
	hash := hex.EncodeToString(sum[:])

	url, _, err := svc.PresignedUploadURL(ctx, matchID, 1, 1, hash)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(blob))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("upload: %v", err)
	}
	resp.Body.Close()

	if _, err := svc.Commit(ctx, CommitRequest{
		MatchId: matchID, AuthorityEpoch: 1, SnapshotRevision: 1,
		PayloadHash: hash, PayloadSize: int64(len(blob)),
	}, host); err != nil {
		t.Fatalf("first commit: %v", err)
	}

	// different hash, same revision -> conflict
	if _, err := svc.Commit(ctx, CommitRequest{
		MatchId: matchID, AuthorityEpoch: 1, SnapshotRevision: 1,
		PayloadHash: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		PayloadSize: int64(len(blob)),
	}, host); err != ErrConflict {
		t.Fatalf("different hash same revision should conflict, got %v", err)
	}
}

func TestCommitRejectsNonHost(t *testing.T) {
	pool, mc, bucket := testStore(t)
	svc := NewService(pool, mc, bucket)
	ctx := context.Background()

	host := "ckpt-host-" + t.Name()
	other := "ckpt-other-" + t.Name()
	matchID := seedMatch(t, pool, host)

	// other device not in match
	if _, err := svc.Commit(ctx, CommitRequest{
		MatchId: matchID, AuthorityEpoch: 1, SnapshotRevision: 1,
		PayloadHash: "aa", PayloadSize: 2,
	}, other); err != ErrNotHost {
		t.Fatalf("non-host commit should fail, got %v", err)
	}

	// host at stale epoch
	if _, err := svc.Commit(ctx, CommitRequest{
		MatchId: matchID, AuthorityEpoch: 0, SnapshotRevision: 1,
		PayloadHash: "bb", PayloadSize: 2,
	}, host); err != ErrEpochStale {
		t.Fatalf("stale epoch commit should fail, got %v", err)
	}
}

func TestCommitHashMismatch(t *testing.T) {
	pool, mc, bucket := testStore(t)
	svc := NewService(pool, mc, bucket)
	ctx := context.Background()

	host := "ckpt-host-" + t.Name()
	matchID := seedMatch(t, pool, host)

	blob := []byte("data")
	sum := sha256.Sum256(blob)
	hash := hex.EncodeToString(sum[:])

	url, _, _ := svc.PresignedUploadURL(ctx, matchID, 1, 1, hash)
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(blob))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()

	// report wrong size -> verification fails
	_, err := svc.Commit(ctx, CommitRequest{
		MatchId: matchID, AuthorityEpoch: 1, SnapshotRevision: 1,
		PayloadHash: hash, PayloadSize: 9999,
	}, host)
	if err != ErrSizeMismatch {
		t.Fatalf("wrong size should fail with ErrSizeMismatch, got %v", err)
	}
}
