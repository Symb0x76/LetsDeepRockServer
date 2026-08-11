package relay

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"
)

// Service ties TURN credentials, allocation events and persistence together:
// it issues short-lived REST credentials, parses the username embedded in
// allocation events and records rows used for relayed candidates.
type Service struct {
	store        *Store
	secret       string // shared HMAC secret, must match the STUN server's
	ttl          time.Duration
	relayAddr    string // advertised TURN server address, e.g. "203.0.113.7"
}

func NewService(store *Store, sharedSecret, relayAddr string, ttl time.Duration) *Service {
	if ttl == 0 {
		ttl = time.Hour
	}
	return &Service{store: store, secret: sharedSecret, relayAddr: relayAddr, ttl: ttl}
}

// CredentialsFor issues a short-lived TURN credential for a match peer.
// The coordinator must hand these to the peer over the authenticated
// control channel; the STUN server validates them statelessly.
func (s *Service) CredentialsFor(matchID, peerID string) Credential {
	return Credentials(s.secret, s.relayAddr, matchID, peerID, s.ttl)
}

// RecordAllocation persists one allocation observed by the TURN server.
// The callback runs on the TURN event loop; failures are logged, not fatal.
func (s *Service) RecordAllocation(ctx context.Context, username string, clientAddr, relayAddr net.Addr) {
	matchID, peerID, err := ParseUsername(username)
	if err != nil {
		slog.Warn("allocation event with invalid username ignored", "username", username, "err", err)
		return
	}
	relayPort, err := portOf(relayAddr)
	if err != nil {
		slog.Warn("allocation event with invalid relay address ignored", "relay", relayAddr.String())
		return
	}
	a := Allocation{
		AllocationID: AllocationID(clientAddr.String(), relayAddr.String(), "udp"),
		MatchID:      matchID,
		PeerID:       peerID,
		Token:        username,
		RelayAddr:    s.relayAddr,
		RelayPort:    relayPort,
		ExpiresAt:    time.Now().Add(s.ttl),
	}
	if err := s.store.Upsert(ctx, a); err != nil {
		slog.Error("record relay allocation failed", "err", err, "match", matchID)
	}
}

// ListAllocations returns unexpired allocations for a match.
func (s *Service) ListAllocations(ctx context.Context, matchID string) ([]Allocation, error) {
	return s.store.ListByMatch(ctx, matchID)
}

// Revoke removes an allocation by id.
func (s *Service) Revoke(ctx context.Context, allocationID string) error {
	return s.store.Delete(ctx, allocationID)
}

// RunPruneLoop periodically removes expired allocation rows.
func (s *Service) RunPruneLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := s.store.DeleteExpired(ctx)
			if err != nil {
				slog.Error("prune relay allocations failed", "err", err)
				continue
			}
			if n > 0 {
				slog.Info("pruned expired relay allocations", "count", n)
			}
		}
	}
}

func portOf(addr net.Addr) (int, error) {
	s := addr.String()
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return 0, &net.AddrError{Err: "missing port", Addr: s}
	}
	return strconv.Atoi(s[i+1:])
}
