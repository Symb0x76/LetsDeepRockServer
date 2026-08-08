package candidate

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsdeeprock/coordinator/internal/protocol"
	"github.com/letsdeeprock/coordinator/internal/store"
)

// testPool connects and applies migrations so tests are self-contained.
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

	st := &store.Store{Pool: pool}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func freshMatchWithEpoch(t *testing.T, pool *pgxpool.Pool, epoch uint64) (matchID, host, client string) {
	t.Helper()
	ctx := context.Background()
	host = "host-" + t.Name()
	client = "client-" + t.Name()

	if _, err := pool.Exec(ctx, `
		INSERT INTO devices (device_id, public_key) VALUES ($1, $2), ($3, $4)
		ON CONFLICT (device_id) DO NOTHING`,
		host, []byte("host-pub-"+t.Name()+"-00000000000000000000000000"),
		client, []byte("client-pub-"+t.Name()+"-0000000000000000000000000"));
	err != nil {
		t.Fatalf("seed devices: %v", err)
	}

	if err := pool.QueryRow(ctx, `
		INSERT INTO matches (match_id, join_code, join_code_exp, host_device_id, authority_epoch, roster_revision, created_by)
		VALUES (gen_random_uuid()::text, gen_random_uuid()::text, now() + interval '1 hour', $1, $2, 0, $1)
		RETURNING match_id`,
		host, epoch).Scan(&matchID); err != nil {
		t.Fatalf("create match: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO match_peers (match_id, peer_id, device_id, role) VALUES ($1, $2, $2, 'Host'), ($1, $3, $3, 'Client')`,
		matchID, host, client); err != nil {
		t.Fatalf("seed peers: %v", err)
	}
	return matchID, host, client
}

func cand(matchID, peer string, epoch uint64) protocol.EndpointCandidate {
	return protocol.EndpointCandidate{
		MatchId:        matchID,
		AuthorityEpoch: epoch,
		PeerId:         peer,
		Transport:      protocol.TransportUDP,
		CandidateType:  protocol.CandidateTypeHost,
		Address:        "192.168.1.10",
		Port:           7777,
		Priority:       100,
		Foundation:     "f1",
		ExpiresAt:      time.Now().Add(10 * time.Minute),
	}
}

func TestStoreAndList(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool)
	ctx := context.Background()

	matchID, host, client := freshMatchWithEpoch(t, pool, 1)

	if err := svc.StoreCandidate(ctx, cand(matchID, host, 1)); err != nil {
		t.Fatalf("store host: %v", err)
	}
	if err := svc.StoreCandidate(ctx, cand(matchID, client, 1)); err != nil {
		t.Fatalf("store client: %v", err)
	}

	list, err := svc.ListCandidates(ctx, matchID, host)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(list))
	}
}

func TestStaleEpochRejected(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool)
	ctx := context.Background()

	matchID, host, _ := freshMatchWithEpoch(t, pool, 2)

	err := svc.StoreCandidate(ctx, cand(matchID, host, 1))
	if err != ErrStaleEpoch {
		t.Fatalf("stale epoch should fail with ErrStaleEpoch, got %v", err)
	}
}

func TestFutureEpochRejected(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool)
	ctx := context.Background()

	matchID, host, _ := freshMatchWithEpoch(t, pool, 2)

	err := svc.StoreCandidate(ctx, cand(matchID, host, 3))
	if err != ErrInvalidEpoch {
		t.Fatalf("future epoch should fail with ErrInvalidEpoch, got %v", err)
	}
}

func TestExpiredCandidateFiltered(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool)
	ctx := context.Background()

	matchID, host, _ := freshMatchWithEpoch(t, pool, 1)

	expired := cand(matchID, host, 1)
	expired.ExpiresAt = time.Now().Add(-time.Minute)
	if err := svc.StoreCandidate(ctx, expired); err != nil {
		t.Fatalf("store expired: %v", err)
	}

	list, err := svc.ListCandidates(ctx, matchID, host)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("expired candidate should be filtered, got %d", len(list))
	}
}

func TestNotMemberRejected(t *testing.T) {
	pool := testPool(t)
	svc := NewService(pool)
	ctx := context.Background()

	matchID, _, _ := freshMatchWithEpoch(t, pool, 1)

	outsider := cand(matchID, "unknown-dev", 1)
	err := svc.StoreCandidate(ctx, outsider)
	if err != ErrNotMember {
		t.Fatalf("outsider should fail with ErrNotMember, got %v", err)
	}

	if _, err := svc.ListCandidates(ctx, matchID, "unknown-dev"); err != ErrNotMember {
		t.Fatalf("outsider list should fail, got %v", err)
	}
}
