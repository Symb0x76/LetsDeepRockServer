// Package candidate implements endpoint candidate exchange and
// connectivity result reporting for matches.
//
// Candidates are stamped with MatchId / AuthorityEpoch / RosterRevision /
// PeerId; writes from stale epochs are rejected so an old host can never
// publish an endpoint over a newer term. Relayed candidates reference a
// relay allocation that must belong to the same match and peer.
package candidate

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsdeeprock/coordinator/internal/protocol"
)

var (
	ErrMatchNotFound    = errors.New("match not found")
	ErrNotMember        = errors.New("not a member of match")
	ErrStaleEpoch       = errors.New("candidate epoch is stale")
	ErrInvalidEpoch     = errors.New("candidate epoch does not match current lease")
	ErrRelayAllocation  = errors.New("relay allocation does not belong to this match/peer")
)

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// StoreCandidate persists a candidate after validating membership and
// epoch freshness against the current lease.
func (s *Service) StoreCandidate(ctx context.Context, c protocol.EndpointCandidate) error {
	// validate match + epoch freshness inside one transaction
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var currentEpoch uint64
	err = tx.QueryRow(ctx,
		`SELECT authority_epoch FROM matches WHERE match_id = $1`, c.MatchId).Scan(&currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrMatchNotFound
	}
	if err != nil {
		return err
	}

	// candidate must be written against the current term
	if c.AuthorityEpoch != currentEpoch {
		if c.AuthorityEpoch < currentEpoch {
			return ErrStaleEpoch
		}
		return ErrInvalidEpoch
	}

	// member check
	var role string
	err = tx.QueryRow(ctx,
		`SELECT role FROM match_peers WHERE match_id = $1 AND device_id = $2`,
		c.MatchId, c.PeerId).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotMember
	}
	if err != nil {
		return err
	}

	// a relayed candidate must reference a relay allocation owned by this
	// match and peer; otherwise any member could forge a relayed address
	if c.CandidateType == protocol.CandidateTypeRelayed {
		if c.RelayAllocationId == "" {
			return ErrRelayAllocation
		}
		var allocMatch, allocPeer string
		err = tx.QueryRow(ctx,
			`SELECT match_id, peer_id FROM relay_allocations WHERE allocation_id = $1`,
			c.RelayAllocationId).Scan(&allocMatch, &allocPeer)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrRelayAllocation
		}
		if err != nil {
			return err
		}
		if allocMatch != c.MatchId || allocPeer != c.PeerId {
			return ErrRelayAllocation
		}
	}

	expires := c.ExpiresAt
	if expires.IsZero() {
		expires = time.Now().Add(2 * time.Minute)
	}
	if c.CandidateId == "" {
		c.CandidateId = uuid.NewString()
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO endpoint_candidates
			(match_id, peer_id, candidate_id, authority_epoch, roster_revision,
			 transport, candidate_type, address, port, priority, foundation,
			 relay_allocation_id, expires_at, signature)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (match_id, peer_id, candidate_id) DO UPDATE SET
			transport = EXCLUDED.transport,
			candidate_type = EXCLUDED.candidate_type,
			address = EXCLUDED.address,
			port = EXCLUDED.port,
			priority = EXCLUDED.priority,
			foundation = EXCLUDED.foundation,
			expires_at = EXCLUDED.expires_at`,
		c.MatchId, c.PeerId, c.CandidateId, c.AuthorityEpoch, c.RosterRevision,
		string(c.Transport), string(c.CandidateType), c.Address, c.Port, c.Priority,
		c.Foundation, c.RelayAllocationId, expires, c.Signature)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListCandidates returns non-expired candidates for a match, filtered to
// the requesting device's match membership.
func (s *Service) ListCandidates(ctx context.Context, matchID, deviceID string) ([]protocol.EndpointCandidate, error) {
	// membership check
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM match_peers WHERE match_id = $1 AND device_id = $2`,
		matchID, deviceID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotMember
	}
	if err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT match_id, peer_id, candidate_id, authority_epoch, roster_revision,
		       transport, candidate_type, address, port, priority, foundation,
		       relay_allocation_id, expires_at, signature
		FROM endpoint_candidates
		WHERE match_id = $1 AND expires_at > now()
		ORDER BY authority_epoch DESC, priority DESC`, matchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 空结果返回空数组而非 nil:encoding/json 会把 nil slice 序列化为
	// "candidates":null,UE 端 TryGetArrayField 对 null 返回 false 导致
	// 客户端把正常空列表误判为响应解析失败。
	out := make([]protocol.EndpointCandidate, 0)
	for rows.Next() {
		var c protocol.EndpointCandidate
		var transport, candType string
		err := rows.Scan(&c.MatchId, &c.PeerId, &c.CandidateId, &c.AuthorityEpoch,
			&c.RosterRevision, &transport, &candType, &c.Address, &c.Port,
			&c.Priority, &c.Foundation, &c.RelayAllocationId, &c.ExpiresAt, &c.Signature)
		if err != nil {
			return nil, err
		}
		c.Transport = protocol.Transport(transport)
		c.CandidateType = protocol.CandidateType(candType)
		out = append(out, c)
	}
	return out, rows.Err()
}
