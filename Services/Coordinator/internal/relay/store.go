package relay

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrAllocationNotFound = errors.New("allocation not found")

// Allocation is one TURN relay allocation recorded for a match peer.
type Allocation struct {
	AllocationID string    `json:"allocationId"`
	MatchID      string    `json:"matchId"`
	PeerID       string    `json:"peerId"`
	Token        string    `json:"token"`
	RelayAddr    string    `json:"relayAddress"`
	RelayPort    int       `json:"relayPort"`
	ExpiresAt    time.Time `json:"expiresAt"`
	CreatedAt    time.Time `json:"createdAt"`
}

// Store persists relay allocations in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Upsert records or refreshes an allocation. The primary key is the
// allocation id (derived from the TURN five-tuple), so a client that
// re-allocates from the same binding refreshes the same row.
func (s *Store) Upsert(ctx context.Context, a Allocation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO relay_allocations
			(allocation_id, match_id, peer_id, token, relay_address, relay_port, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (allocation_id) DO UPDATE SET
			match_id = EXCLUDED.match_id,
			peer_id = EXCLUDED.peer_id,
			token = EXCLUDED.token,
			relay_address = EXCLUDED.relay_address,
			relay_port = EXCLUDED.relay_port,
			expires_at = EXCLUDED.expires_at`,
		a.AllocationID, a.MatchID, a.PeerID, a.Token,
		a.RelayAddr, a.RelayPort, a.ExpiresAt)
	return err
}

// GetByAllocationID returns one allocation by id.
func (s *Store) GetByAllocationID(ctx context.Context, allocationID string) (Allocation, error) {
	var a Allocation
	err := s.pool.QueryRow(ctx, `
		SELECT allocation_id, match_id, peer_id, token,
		       relay_address, relay_port, expires_at, created_at
		FROM relay_allocations WHERE allocation_id = $1`, allocationID).
		Scan(&a.AllocationID, &a.MatchID, &a.PeerID, &a.Token,
			&a.RelayAddr, &a.RelayPort, &a.ExpiresAt, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Allocation{}, ErrAllocationNotFound
	}
	return a, err
}

// ListByMatch returns unexpired allocations for a match.
func (s *Store) ListByMatch(ctx context.Context, matchID string) ([]Allocation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT allocation_id, match_id, peer_id, token,
		       relay_address, relay_port, expires_at, created_at
		FROM relay_allocations
		WHERE match_id = $1 AND expires_at > now()
		ORDER BY created_at`, matchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Allocation
	for rows.Next() {
		var a Allocation
		if err := rows.Scan(&a.AllocationID, &a.MatchID, &a.PeerID, &a.Token,
			&a.RelayAddr, &a.RelayPort, &a.ExpiresAt, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Delete removes an allocation (revoke).
func (s *Store) Delete(ctx context.Context, allocationID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM relay_allocations WHERE allocation_id = $1`, allocationID)
	return err
}

// DeleteExpired removes allocations whose credentials have lapsed, keeping
// the table small. Returns the number of deleted rows.
func (s *Store) DeleteExpired(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM relay_allocations WHERE expires_at <= now()`)
	return tag.RowsAffected(), err
}
