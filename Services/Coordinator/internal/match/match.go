// Package match implements the match lifecycle: create, join, leave,
// query, role assignment, and the WebSocket event channel.
package match

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const joinCodeTTL = 15 * time.Minute

var (
	ErrNotFound     = errors.New("match not found")
	ErrJoinCodeExpired = errors.New("join code expired or invalid")
	ErrAlreadyJoined = errors.New("already in match")
	ErrNotMember    = errors.New("not a member of match")
)

type Role string

const (
	RoleHost     Role = "Host"
	RoleClient   Role = "Client"
	RoleObserver Role = "Observer"
)

type Match struct {
	MatchID          string    `json:"matchId"`
	JoinCode         string    `json:"joinCode"`
	JoinCodeExp      time.Time `json:"joinCodeExp"`
	HostDeviceID     string    `json:"hostDeviceId,omitempty"`
	AuthorityEpoch   uint64    `json:"authorityEpoch"`
	RosterRevision   uint64    `json:"rosterRevision"`
	CreatedAt        time.Time `json:"createdAt"`
	CreatedBy        string    `json:"createdBy"`
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

// CreateMatch mints a match owned by the creator (who becomes the initial
// Host) with a random join code.
func (s *Service) CreateMatch(ctx context.Context, creatorDeviceID string) (Match, error) {
	matchID := uuid.NewString()
	joinCode, err := randomJoinCode()
	if err != nil {
		return Match{}, err
	}
	now := time.Now()
	m := Match{
		MatchID:        matchID,
		JoinCode:       joinCode,
		JoinCodeExp:    now.Add(joinCodeTTL),
		HostDeviceID:   creatorDeviceID,
		AuthorityEpoch: 0,
		RosterRevision: 0,
		CreatedAt:      now,
		CreatedBy:      creatorDeviceID,
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Match{}, err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO matches (match_id, join_code, join_code_exp, host_device_id, authority_epoch, roster_revision, created_by)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		m.MatchID, m.JoinCode, m.JoinCodeExp, m.HostDeviceID, m.AuthorityEpoch, m.RosterRevision, m.CreatedBy)
	if err != nil {
		return Match{}, err
	}

	// creator joins as Host
	_, err = tx.Exec(ctx, `
		INSERT INTO match_peers (match_id, peer_id, device_id, role)
		VALUES ($1, $2, $3, $4)`,
		m.MatchID, creatorDeviceID, creatorDeviceID, RoleHost)
	if err != nil {
		return Match{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO role_assignments (match_id, authority_epoch, peer_id, role)
		VALUES ($1, $2, $3, $4)`,
		m.MatchID, m.AuthorityEpoch, creatorDeviceID, RoleHost)
	if err != nil {
		return Match{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Match{}, err
	}
	return m, nil
}

// JoinMatch adds the device to a match by join code. Returns the match and
// the assigned role (first joiner after host is a Client).
func (s *Service) JoinMatch(ctx context.Context, joinCode, deviceID string) (Match, Role, error) {
	var m Match
	var role Role

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Match{}, "", err
	}
	defer tx.Rollback(ctx)

	// lock the match row to serialize roster changes
	err = tx.QueryRow(ctx, `
		SELECT match_id, join_code, join_code_exp, host_device_id, authority_epoch, roster_revision, created_at, created_by
		FROM matches WHERE join_code = $1
		FOR UPDATE`, joinCode).
		Scan(&m.MatchID, &m.JoinCode, &m.JoinCodeExp, &m.HostDeviceID, &m.AuthorityEpoch, &m.RosterRevision, &m.CreatedAt, &m.CreatedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return Match{}, "", ErrJoinCodeExpired
	}
	if err != nil {
		return Match{}, "", err
	}
	if time.Now().After(m.JoinCodeExp) {
		return Match{}, "", ErrJoinCodeExpired
	}

	// already a member?
	var existing string
	err = tx.QueryRow(ctx,
		`SELECT role FROM match_peers WHERE match_id = $1 AND device_id = $2`,
		m.MatchID, deviceID).Scan(&existing)
	if err == nil {
		role = Role(existing)
		if role == "" {
			role = RoleClient
		}
		if err := tx.Commit(ctx); err != nil {
			return Match{}, "", err
		}
		return m, role, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Match{}, "", err
	}

	// role: the creator holds Host; every other joiner is a Client
	role = RoleClient
	_, err = tx.Exec(ctx, `
		INSERT INTO match_peers (match_id, peer_id, device_id, role)
		VALUES ($1, $2, $3, $4)`,
		m.MatchID, deviceID, deviceID, role)
	if err != nil {
		return Match{}, "", err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO role_assignments (match_id, authority_epoch, peer_id, role)
		VALUES ($1, $2, $3, $4)`,
		m.MatchID, m.AuthorityEpoch, deviceID, role)
	if err != nil {
		return Match{}, "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return Match{}, "", err
	}
	return m, role, nil
}

// LeaveMatch removes the device from the match roster.
func (s *Service) LeaveMatch(ctx context.Context, matchID, deviceID string) error {
	ct, err := s.pool.Exec(ctx,
		`DELETE FROM match_peers WHERE match_id = $1 AND device_id = $2`,
		matchID, deviceID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotMember
	}
	return nil
}

// Get returns match metadata for a member.
func (s *Service) Get(ctx context.Context, matchID string) (Match, error) {
	var m Match
	err := s.pool.QueryRow(ctx, `
		SELECT match_id, join_code, join_code_exp, host_device_id, authority_epoch, roster_revision, created_at, created_by
		FROM matches WHERE match_id = $1`, matchID).
		Scan(&m.MatchID, &m.JoinCode, &m.JoinCodeExp, &m.HostDeviceID, &m.AuthorityEpoch, &m.RosterRevision, &m.CreatedAt, &m.CreatedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return Match{}, ErrNotFound
	}
	return m, err
}

func randomJoinCode() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
