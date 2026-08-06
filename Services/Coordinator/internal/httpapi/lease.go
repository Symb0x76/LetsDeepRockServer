package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/letsdeeprock/coordinator/internal/lease"
	"github.com/letsdeeprock/coordinator/internal/match"
	"github.com/letsdeeprock/coordinator/internal/protocol"
)

// LeaseHandler serves the host lease endpoints.
type LeaseHandler struct {
	Service *lease.Service
	Hub     *match.Hub
}

type leaseClaimRequest struct {
	PeerID        string `json:"peerId"`
	ExpectedEpoch uint64 `json:"expectedEpoch"`
}

// POST /v1/matches/{matchId}/lease/claim
func (h *LeaseHandler) Claim(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")
	var req leaseClaimRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	l, err := h.Service.Claim(r.Context(), matchID, deviceID, req.PeerID, req.ExpectedEpoch)
	if err != nil {
		slog.Debug("lease claim rejected", "match", matchID, "err", err)
		switch {
		case errors.Is(err, lease.ErrNoLease) || errors.Is(err, lease.ErrCandidateNotPeer):
			writeErr(w, http.StatusNotFound, err.Error())
		case errors.Is(err, lease.ErrConflict):
			writeErr(w, http.StatusConflict, err.Error())
		case errors.Is(err, lease.ErrEpochMismatch) || errors.Is(err, lease.ErrStaleEpoch):
			writeErr(w, http.StatusConflict, err.Error())
		default:
			writeErr(w, http.StatusInternalServerError, "claim failed")
		}
		return
	}
	h.Hub.Publish(matchID, protocol.Event{
		Type:    protocol.EventLeaseGranted,
		MatchId: matchID,
		Epoch:   l.AuthorityEpoch,
		PeerId:  deviceID,
	})
	writeJSON(w, http.StatusCreated, l)
}

// POST /v1/matches/{matchId}/lease/heartbeat
func (h *LeaseHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")
	var req leaseClaimRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	l, err := h.Service.Heartbeat(r.Context(), matchID, deviceID, req.ExpectedEpoch)
	if err != nil {
		switch {
		case errors.Is(err, lease.ErrNoLease):
			writeErr(w, http.StatusNotFound, "no active lease")
		case errors.Is(err, lease.ErrNotHost):
			writeErr(w, http.StatusForbidden, "not the current host")
		case errors.Is(err, lease.ErrEpochMismatch):
			writeErr(w, http.StatusConflict, "epoch mismatch")
		case errors.Is(err, lease.ErrLeaseNotExpired):
			writeErr(w, http.StatusConflict, "lease expired; re-claim required")
		default:
			writeErr(w, http.StatusInternalServerError, "heartbeat failed")
		}
		return
	}
	h.Hub.Publish(matchID, protocol.Event{
		Type:    protocol.EventLeaseRenewed,
		MatchId: matchID,
		Epoch:   l.AuthorityEpoch,
		PeerId:  deviceID,
	})
	writeJSON(w, http.StatusOK, l)
}

// POST /v1/matches/{matchId}/lease/release
func (h *LeaseHandler) Release(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")
	var req leaseClaimRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	if err := h.Service.Release(r.Context(), matchID, deviceID, req.ExpectedEpoch); err != nil {
		switch {
		case errors.Is(err, lease.ErrNoLease):
			writeErr(w, http.StatusNotFound, "no active lease")
		case errors.Is(err, lease.ErrNotHost):
			writeErr(w, http.StatusForbidden, "not the current host")
		default:
			writeErr(w, http.StatusInternalServerError, "release failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "released"})
}
