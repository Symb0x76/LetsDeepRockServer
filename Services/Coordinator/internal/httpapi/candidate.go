package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/letsdeeprock/coordinator/internal/candidate"
	"github.com/letsdeeprock/coordinator/internal/lease"
	"github.com/letsdeeprock/coordinator/internal/match"
	"github.com/letsdeeprock/coordinator/internal/protocol"
)

// CandidateHandler serves endpoint candidate exchange and connectivity
// result reporting.
type CandidateHandler struct {
	Service *candidate.Service
	Leases  *lease.Service
	Hub     *match.Hub
}

type connectivityCheckResult struct {
	PeerID      string                `json:"peerId"`
	AuthorityEpoch uint64             `json:"authorityEpoch"`
	State       protocol.ConnectivityState `json:"state"`
	Transport   protocol.Transport    `json:"transport"`
}

// POST /v1/matches/{matchId}/peers/{peerId}/candidates
func (h *CandidateHandler) AddCandidate(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")
	targetPeer := r.PathValue("peerId")

	var c protocol.EndpointCandidate
	if err := parseJSON(r, &c); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	c.MatchId = matchID
	c.PeerId = deviceID

	// a device may only write candidates for itself
	if targetPeer != deviceID {
		writeErr(w, http.StatusForbidden, "cannot write candidates for another peer")
		return
	}

	if err := h.Service.StoreCandidate(r.Context(), c); err != nil {
		switch {
		case err == candidate.ErrMatchNotFound:
			writeErr(w, http.StatusNotFound, "match not found")
		case err == candidate.ErrNotMember:
			writeErr(w, http.StatusForbidden, "not a member")
		case err == candidate.ErrStaleEpoch || err == candidate.ErrInvalidEpoch:
			writeErr(w, http.StatusConflict, err.Error())
		case err == candidate.ErrRelayAllocation:
			writeErr(w, http.StatusForbidden, err.Error())
		default:
			slog.Error("store candidate failed", "err", err)
			writeErr(w, http.StatusInternalServerError, "store failed")
		}
		return
	}
	h.Hub.Publish(matchID, protocol.Event{
		Type:    protocol.EventCandidateUpdated,
		MatchId: matchID,
		Epoch:   c.AuthorityEpoch,
		PeerId:  deviceID,
	})
	writeJSON(w, http.StatusCreated, map[string]string{"status": "stored"})
}

// GET /v1/matches/{matchId}/peers/{peerId}/candidates
func (h *CandidateHandler) ListCandidates(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")

	list, err := h.Service.ListCandidates(r.Context(), matchID, deviceID)
	if err != nil {
		switch {
		case err == candidate.ErrNotMember:
			writeErr(w, http.StatusForbidden, "not a member")
		default:
			slog.Error("list candidates failed", "err", err)
			writeErr(w, http.StatusInternalServerError, "list failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"candidates": list})
}

// POST /v1/matches/{matchId}/connectivity/check-result
func (h *CandidateHandler) CheckResult(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")

	var res connectivityCheckResult
	if err := parseJSON(r, &res); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	// record the peer's connectivity observation for other members
	// (audit / diagnostics; the nominated endpoint is authoritative from
	// the host, not from a single peer's check result)
	slog.Info("connectivity check result",
		"match", matchID, "peer", deviceID,
		"state", res.State, "epoch", res.AuthorityEpoch)
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

// POST /v1/matches/{matchId}/connectivity/nominate
// The host nominates the endpoint clients should use to reach it. Only the
// current lease holder may nominate.
func (h *CandidateHandler) Nominate(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")

	var req struct {
		AuthorityEpoch uint64 `json:"authorityEpoch"`
		Address        string `json:"address"`
		Port           int    `json:"port"`
	}
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}

	// verify the nominator holds the current lease
	cur, err := h.Leases.Current(r.Context(), matchID)
	if err != nil {
		if errors.Is(err, lease.ErrNoLease) {
			writeErr(w, http.StatusConflict, "no active lease")
			return
		}
		writeErr(w, http.StatusInternalServerError, "lease lookup failed")
		return
	}
	if cur.HostDeviceID != deviceID {
		writeErr(w, http.StatusForbidden, "only the current host may nominate")
		return
	}
	if req.AuthorityEpoch != 0 && req.AuthorityEpoch != cur.AuthorityEpoch {
		writeErr(w, http.StatusConflict, "epoch mismatch")
		return
	}

	h.Hub.Publish(matchID, protocol.Event{
		Type:    protocol.EventHostEndpointPublished,
		MatchId: matchID,
		Epoch:   cur.AuthorityEpoch,
		PeerId:  deviceID,
		Payload: map[string]any{
			"address": req.Address,
			"port":    req.Port,
		},
	})
	writeJSON(w, http.StatusOK, map[string]string{"status": "nominated"})
}
