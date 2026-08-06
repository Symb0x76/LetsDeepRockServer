package httpapi

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/letsdeeprock/coordinator/internal/checkpoint"
	"github.com/letsdeeprock/coordinator/internal/match"
	"github.com/letsdeeprock/coordinator/internal/protocol"
)

// CheckpointHandler serves the checkpoint upload/commit flow.
type CheckpointHandler struct {
	Service *checkpoint.Service
	Hub     *match.Hub
}

// POST /v1/matches/{matchId}/checkpoints/begin
// Returns a presigned PUT URL for the snapshot blob.
func (h *CheckpointHandler) Begin(w http.ResponseWriter, r *http.Request) {
	matchID := r.PathValue("matchId")

	var req struct {
		AuthorityEpoch   uint64 `json:"authorityEpoch"`
		SnapshotRevision uint64 `json:"snapshotRevision"`
		PayloadHash      string `json:"payloadHash"`
	}
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	url, objectKey, err := h.Service.PresignedUploadURL(r.Context(), matchID, req.AuthorityEpoch, req.SnapshotRevision, req.PayloadHash)
	if err != nil {
		slog.Error("presign failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "presign failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"uploadUrl": url, "objectKey": objectKey})
}

// POST /v1/matches/{matchId}/checkpoints/commit
func (h *CheckpointHandler) Commit(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")

	var req checkpoint.CommitRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	req.MatchId = matchID

	res, err := h.Service.Commit(r.Context(), req, deviceID)
	if err != nil {
		switch {
		case errors.Is(err, checkpoint.ErrMatchNotFound):
			writeErr(w, http.StatusNotFound, "match not found")
		case errors.Is(err, checkpoint.ErrNotHost):
			writeErr(w, http.StatusForbidden, "only the current host may commit")
		case errors.Is(err, checkpoint.ErrEpochStale):
			writeErr(w, http.StatusConflict, "epoch stale")
		case errors.Is(err, checkpoint.ErrConflict):
			writeErr(w, http.StatusConflict, "revision conflict: hash differs")
		case errors.Is(err, checkpoint.ErrHashMismatch) || errors.Is(err, checkpoint.ErrSizeMismatch):
			writeErr(w, http.StatusUnprocessableEntity, "blob verification failed")
		case strings.Contains(err.Error(), "does not exist"):
			writeErr(w, http.StatusUnprocessableEntity, "blob not found: upload with the presigned URL first")
		default:
			slog.Error("checkpoint commit failed", "err", err)
			writeErr(w, http.StatusInternalServerError, "commit failed")
		}
		return
	}
	h.Hub.Publish(matchID, protocol.Event{
		Type:    protocol.EventCheckpointCommitted,
		MatchId: matchID,
		Epoch:   req.AuthorityEpoch,
		PeerId:  deviceID,
		Payload: map[string]any{
			"objectKey":        res.ObjectKey,
			"snapshotRevision": req.SnapshotRevision,
		},
	})
	writeJSON(w, http.StatusOK, res)
}

// GET /v1/matches/{matchId}/checkpoints/latest
func (h *CheckpointHandler) Latest(w http.ResponseWriter, r *http.Request) {
	matchID := r.PathValue("matchId")
	meta, res, err := h.Service.Latest(r.Context(), matchID)
	if err != nil {
		slog.Error("latest checkpoint failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	if meta == nil {
		writeJSON(w, http.StatusOK, map[string]any{"checkpoint": nil})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checkpoint": meta, "objectKey": res.ObjectKey, "committedAt": res.CommittedAt})
}
