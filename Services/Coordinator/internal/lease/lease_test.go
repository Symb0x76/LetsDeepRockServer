package lease

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
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
	return pool
}

// freshMatch creates a match owned by hostDevice with clientDevice joined.
func freshMatch(t *testing.T, pool *pgxpool.Pool) (matchID, hostDevice, clientDevice string) {
	t.Helper()
	ctx := context.Background()
	hostDevice = "host-dev-" + t.Name()
	clientDevice = "client-dev-" + t.Name()

	if _, err := pool.Exec(ctx, `
		INSERT INTO devices (device_id, public_key) VALUES ($1, $2), ($3, $4)
		ON CONFLICT (device_id) DO NOTHING`,
		hostDevice, []byte("host-pub-"+t.Name()+"-00000000000000000000000000"),
		clientDevice, []byte("client-pub-"+t.Name()+"-0000000000000000000000000"));
	err != nil {
		t.Fatalf("seed devices: %v", err)
	}

	if err := pool.QueryRow(ctx, `
		INSERT INTO matches (match_id, join_code, join_code_exp, host_device_id, authority_epoch, roster_revision, created_by)
		VALUES (gen_random_uuid()::text, gen_random_uuid()::text, now() + interval '1 hour', $1, 0, 0, $1)
		RETURNING match_id`,
		hostDevice).Scan(&matchID); err != nil {
		t.Fatalf("create match: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO match_peers (match_id, peer_id, device_id, role) VALUES ($1, $2, $2, 'Host'), ($1, $3, $3, 'Client')`,
		matchID, hostDevice, clientDevice); err != nil {
		t.Fatalf("seed peers: %v", err)
	}
	return matchID, hostDevice, clientDevice
}

func TestClaimAndHeartbeat(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool, 30*time.Second)
	ctx := context.Background()

	matchID, host, client := freshMatch(t, pool)

	l, err := svc.Claim(ctx, matchID, host, "peer-"+host, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if l.AuthorityEpoch != 1 {
		t.Fatalf("epoch should be 1, got %d", l.AuthorityEpoch)
	}

	// non-host cannot heartbeat
	if _, err := svc.Heartbeat(ctx, matchID, client, 1); err != ErrNotHost {
		t.Fatalf("client heartbeat should fail with ErrNotHost, got %v", err)
	}

	// host heartbeat renews
	renewed, err := svc.Heartbeat(ctx, matchID, host, 1)
	if err != nil {
		t.Fatalf("host heartbeat: %v", err)
	}
	if !renewed.ExpiresAt.After(l.ExpiresAt) {
		t.Fatal("heartbeat should extend expiry")
	}
}

func TestConflictSameEpoch(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool, 30*time.Second)
	ctx := context.Background()

	matchID, host, client := freshMatch(t, pool)

	if _, err := svc.Claim(ctx, matchID, host, "peer-"+host, 1); err != nil {
		t.Fatalf("host claim: %v", err)
	}
	// client tries to claim while host lease is active
	if _, err := svc.Claim(ctx, matchID, client, "peer-"+client, 1); err == nil {
		t.Fatal("second claim in same epoch should fail")
	}
}

func TestClaimAfterExpirySuccessorPolicy(t *testing.T) {
	pool := testPool(t)
	// negative TTL forces immediate expiry
	svc := NewService(pool, -time.Second)
	ctx := context.Background()

	matchID, host, client := freshMatch(t, pool)

	l, err := svc.Claim(ctx, matchID, host, "peer-"+host, 1)
	if err != nil {
		t.Fatalf("host claim: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// successor (client) claims after expiry -> epoch 2
	l2, err := svc.Claim(ctx, matchID, client, "peer-"+client, 2)
	if err != nil {
		t.Fatalf("successor claim: %v", err)
	}
	if l2.AuthorityEpoch != 2 {
		t.Fatalf("epoch should be 2, got %d", l2.AuthorityEpoch)
	}

	// old host cannot claim stale epoch 1
	if _, err := svc.Claim(ctx, matchID, host, "peer-"+host, 1); err == nil {
		t.Fatal("old host claiming stale epoch should fail")
	}

	_ = l
}

func TestStaleHeartbeatRejected(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool, 30*time.Second)
	ctx := context.Background()

	matchID, host, _ := freshMatch(t, pool)
	if _, err := svc.Claim(ctx, matchID, host, "peer-"+host, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Heartbeat(ctx, matchID, host, 99); err != ErrEpochMismatch {
		t.Fatalf("wrong epoch heartbeat should fail with ErrEpochMismatch, got %v", err)
	}
}

func TestReleaseByHostOnly(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool, 30*time.Second)
	ctx := context.Background()

	matchID, host, client := freshMatch(t, pool)
	if _, err := svc.Claim(ctx, matchID, host, "peer-"+host, 1); err != nil {
		t.Fatal(err)
	}

	if err := svc.Release(ctx, matchID, client, 1); err != ErrNotHost {
		t.Fatalf("client release should fail, got %v", err)
	}
	if err := svc.Release(ctx, matchID, host, 1); err != nil {
		t.Fatalf("host release: %v", err)
	}
	if _, err := svc.Current(ctx, matchID); err != ErrNoLease {
		t.Fatalf("after release no lease should exist, got %v", err)
	}
}
