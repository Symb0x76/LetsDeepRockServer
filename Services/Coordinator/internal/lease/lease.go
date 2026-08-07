// Package lease implements the Host Lease and AuthorityEpoch fencing.
//
// Invariants (enforced at the SQL layer inside transactions):
//   - a match has at most one active lease
//   - AuthorityEpoch is monotonic per match
//   - a claim must present the old epoch and a valid successor identity
//   - an expired lease only allows claims per the successor policy
//   - committed claims cannot be overwritten by the old host
package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNoLease           = errors.New("no active lease")
	ErrEpochMismatch     = errors.New("epoch does not match current lease")
	ErrStaleEpoch        = errors.New("stale epoch: cannot overwrite newer term")
	ErrNotHost           = errors.New("device is not the current host")
	ErrLeaseNotExpired   = errors.New("lease still valid")
	ErrConflict          = errors.New("lease conflict")
	ErrCandidateNotPeer  = errors.New("claimant is not a member of the match")
)

type Lease struct {
	MatchID        string    `json:"matchId"`
	HostDeviceID   string    `json:"hostDeviceId"`
	HostPeerID     string    `json:"hostPeerId"`
	AuthorityEpoch uint64    `json:"authorityEpoch"`
	ExpiresAt      time.Time `json:"expiresAt"`
}

type Service struct {
	pool     *pgxpool.Pool
	leaseTTL time.Duration
}

func NewService(pool *pgxpool.Pool, leaseTTL time.Duration) *Service {
	return &Service{pool: pool, leaseTTL: leaseTTL}
}

// Claim establishes a new lease for a match. The claimant must be a member
// (Host or Client, per successor policy) and must present the expected
// current epoch. If a lease exists and has not expired, the claim is
// rejected (no two hosts in the same epoch). After expiry, the successor
// may claim with the next epoch.
func (s *Service) Claim(ctx context.Context, matchID, deviceID, peerID string, expectedEpoch uint64) (Lease, error) {
	var l Lease
	now := time.Now()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback(ctx)

	// lock the match row: serializes all lease transitions per match
	var currentEpoch uint64
	err = tx.QueryRow(ctx,
		`SELECT authority_epoch FROM matches WHERE match_id = $1 FOR UPDATE`,
		matchID).Scan(&currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, ErrNoLease
	}
	if err != nil {
		return Lease{}, err
	}

	// membership check
	var role string
	err = tx.QueryRow(ctx,
		`SELECT role FROM match_peers WHERE match_id = $1 AND device_id = $2`,
		matchID, deviceID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, ErrCandidateNotPeer
	}
	if err != nil {
		return Lease{}, err
	}

	// read current lease state (if any)
	var curLease Lease
	haveLease := true
	err = tx.QueryRow(ctx, `
		SELECT match_id, host_device_id, host_peer_id, authority_epoch, expires_at
		FROM leases WHERE match_id = $1`, matchID).
		Scan(&curLease.MatchID, &curLease.HostDeviceID, &curLease.HostPeerID, &curLease.AuthorityEpoch, &curLease.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		haveLease = false
	} else if err != nil {
		return Lease{}, err
	}

	if haveLease && curLease.ExpiresAt.After(now) {
		// active lease: only the current host may renew (handled elsewhere),
		// a new claim is rejected
		return Lease{}, fmt.Errorf("%w: current host %s epoch %d until %s",
			ErrConflict, curLease.HostDeviceID, curLease.AuthorityEpoch, curLease.ExpiresAt.Format(time.RFC3339))
	}

	// successor policy: new epoch must be exactly current+1, and must be
	// higher than any prior epoch on record
	nextEpoch := currentEpoch + 1
	if haveLease && curLease.AuthorityEpoch >= nextEpoch {
		return Lease{}, ErrStaleEpoch
	}
	if expectedEpoch != 0 && expectedEpoch != nextEpoch {
		return Lease{}, ErrEpochMismatch
	}

	l = Lease{
		MatchID:        matchID,
		HostDeviceID:   deviceID,
		HostPeerID:     peerID,
		AuthorityEpoch: nextEpoch,
		ExpiresAt:      now.Add(s.leaseTTL),
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO leases (match_id, host_device_id, host_peer_id, authority_epoch, expires_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (match_id) DO UPDATE SET
			host_device_id = EXCLUDED.host_device_id,
			host_peer_id = EXCLUDED.host_peer_id,
			authority_epoch = EXCLUDED.authority_epoch,
			expires_at = EXCLUDED.expires_at
		WHERE leases.expires_at <= now() OR leases.authority_epoch < EXCLUDED.authority_epoch`,
		l.MatchID, l.HostDeviceID, l.HostPeerID, l.AuthorityEpoch, l.ExpiresAt)
	if err != nil {
		return Lease{}, err
	}

	// bump match epoch
	_, err = tx.Exec(ctx,
		`UPDATE matches SET authority_epoch = $2 WHERE match_id = $1`,
		matchID, nextEpoch)
	if err != nil {
		return Lease{}, err
	}

	// record role assignment at this epoch
	_, err = tx.Exec(ctx, `
		INSERT INTO role_assignments (match_id, authority_epoch, peer_id, role)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`,
		matchID, nextEpoch, peerID, role)
	if err != nil {
		return Lease{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// Heartbeat renews the lease for the current host. Only the current lease
// holder at the current epoch may renew; a stale device is rejected.
func (s *Service) Heartbeat(ctx context.Context, matchID, deviceID string, epoch uint64) (Lease, error) {
	var l Lease
	now := time.Now()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Lease{}, err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx, `
		SELECT match_id, host_device_id, host_peer_id, authority_epoch, expires_at
		FROM leases WHERE match_id = $1 FOR UPDATE`, matchID).
		Scan(&l.MatchID, &l.HostDeviceID, &l.HostPeerID, &l.AuthorityEpoch, &l.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, ErrNoLease
	}
	if err != nil {
		return Lease{}, err
	}

	if l.HostDeviceID != deviceID {
		return Lease{}, ErrNotHost
	}
	if epoch != 0 && epoch != l.AuthorityEpoch {
		return Lease{}, ErrEpochMismatch
	}
	if l.ExpiresAt.Before(now) {
		return Lease{}, ErrLeaseNotExpired // expired: must re-claim
	}

	newExp := now.Add(s.leaseTTL)
	_, err = tx.Exec(ctx,
		`UPDATE leases SET expires_at = $3 WHERE match_id = $1 AND authority_epoch = $2`,
		matchID, l.AuthorityEpoch, newExp)
	if err != nil {
		return Lease{}, err
	}
	l.ExpiresAt = newExp

	if err := tx.Commit(ctx); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// Release ends the lease held by the given device at the current epoch.
func (s *Service) Release(ctx context.Context, matchID, deviceID string, epoch uint64) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var hostDevice string
	var curEpoch uint64
	err = tx.QueryRow(ctx,
		`SELECT host_device_id, authority_epoch FROM leases WHERE match_id = $1 FOR UPDATE`,
		matchID).Scan(&hostDevice, &curEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoLease
	}
	if err != nil {
		return err
	}
	if hostDevice != deviceID {
		return ErrNotHost
	}
	if epoch != 0 && epoch != curEpoch {
		return ErrEpochMismatch
	}

	_, err = tx.Exec(ctx, `DELETE FROM leases WHERE match_id = $1`, matchID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Current returns the active lease for a match.
func (s *Service) Current(ctx context.Context, matchID string) (Lease, error) {
	var l Lease
	err := s.pool.QueryRow(ctx, `
		SELECT match_id, host_device_id, host_peer_id, authority_epoch, expires_at
		FROM leases WHERE match_id = $1`, matchID).
		Scan(&l.MatchID, &l.HostDeviceID, &l.HostPeerID, &l.AuthorityEpoch, &l.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, ErrNoLease
	}
	return l, err
}
