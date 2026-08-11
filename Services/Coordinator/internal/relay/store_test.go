package relay

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsdeeprock/coordinator/internal/store"
)

// testPool connects to the local test database and applies migrations so
// tests are self-contained (same pattern as internal/lease).
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := "postgres://letsdeeprock:letsdeeprock123@127.0.0.1:5432/letsdeeprock"
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	t.Cleanup(pool.Close)
	st := &store.Store{Pool: pool}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

// cleanupAllocations removes rows created by previous runs so assertions on
// list counts stay hermetic.
func cleanupAllocations(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `DELETE FROM relay_allocations`); err != nil {
		t.Fatalf("cleanup allocations: %v", err)
	}
}

func TestStoreUpsertAndList(t *testing.T) {
	pool := testPool(t)
	cleanupAllocations(t, pool)
	ctx := context.Background()
	s := NewStore(pool)

	a := Allocation{
		AllocationID: "alloc-1",
		MatchID:      "match-1",
		PeerID:       "peer-1",
		Token:        "tok",
		RelayAddr:    "203.0.113.7",
		RelayPort:    49152,
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := s.Upsert(ctx, a); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := s.GetByAllocationID(ctx, "alloc-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.RelayPort != 49152 || got.MatchID != "match-1" {
		t.Fatalf("mismatch: %+v", got)
	}

	list, err := s.ListByMatch(ctx, "match-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("want 1 allocation, got %d", len(list))
	}

	if err := s.Delete(ctx, "alloc-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetByAllocationID(ctx, "alloc-1"); err != ErrAllocationNotFound {
		t.Fatalf("want ErrAllocationNotFound after delete, got %v", err)
	}
}

func TestStoreDeleteExpired(t *testing.T) {
	pool := testPool(t)
	cleanupAllocations(t, pool)
	ctx := context.Background()
	s := NewStore(pool)

	expired := Allocation{
		AllocationID: "alloc-expired",
		MatchID:      "match-1",
		PeerID:       "peer-1",
		Token:        "tok",
		RelayAddr:    "203.0.113.7",
		RelayPort:    49153,
		ExpiresAt:    time.Now().Add(-time.Minute),
	}
	live := Allocation{
		AllocationID: "alloc-live",
		MatchID:      "match-1",
		PeerID:       "peer-1",
		Token:        "tok",
		RelayAddr:    "203.0.113.7",
		RelayPort:    49154,
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := s.Upsert(ctx, expired); err != nil {
		t.Fatalf("upsert expired: %v", err)
	}
	if err := s.Upsert(ctx, live); err != nil {
		t.Fatalf("upsert live: %v", err)
	}

	n, err := s.DeleteExpired(ctx)
	if err != nil {
		t.Fatalf("delete expired: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 deleted, got %d", n)
	}
	list, err := s.ListByMatch(ctx, "match-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].AllocationID != "alloc-live" {
		t.Fatalf("want only live allocation, got %+v", list)
	}
}
