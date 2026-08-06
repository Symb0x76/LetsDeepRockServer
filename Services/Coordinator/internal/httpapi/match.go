package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
	"github.com/letsdeeprock/coordinator/internal/match"
)

// MatchHandler serves the match lifecycle endpoints and the WebSocket
// event channel.
type MatchHandler struct {
	Service *match.Service
	Hub     *match.Hub
}

type createMatchRequest struct{}

// POST /v1/matches
func (h *MatchHandler) Create(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	m, err := h.Service.CreateMatch(r.Context(), deviceID)
	if err != nil {
		slog.Error("create match failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "create match failed")
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

type joinMatchRequest struct {
	JoinCode string `json:"joinCode"`
}

// POST /v1/matches/{matchId}/join
func (h *MatchHandler) Join(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	var req joinMatchRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	m, role, err := h.Service.JoinMatch(r.Context(), req.JoinCode, deviceID)
	if err != nil {
		switch {
		case err == match.ErrJoinCodeExpired:
			writeErr(w, http.StatusBadRequest, "invalid or expired join code")
		case err == match.ErrAlreadyJoined:
			writeErr(w, http.StatusConflict, "already joined")
		default:
			slog.Error("join match failed", "err", err)
			writeErr(w, http.StatusInternalServerError, "join failed")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"match": m,
		"role":  role,
	})
}

// POST /v1/matches/{matchId}/leave
func (h *MatchHandler) Leave(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")
	if err := h.Service.LeaveMatch(r.Context(), matchID, deviceID); err != nil {
		writeErr(w, http.StatusNotFound, "not a member")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "left"})
}

// GET /v1/matches/{matchId}
func (h *MatchHandler) Get(w http.ResponseWriter, r *http.Request) {
	matchID := r.PathValue("matchId")
	m, err := h.Service.Get(r.Context(), matchID)
	if err != nil {
		writeErr(w, http.StatusNotFound, "match not found")
		return
	}
	writeJSON(w, http.StatusOK, m)
}

// GET /v1/matches/{matchId}/events
// Upgrades to a WebSocket and streams match events to the device. The
// device must already be a member (or the host).
func (h *MatchHandler) Events(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")

	// membership check before upgrade
	if _, err := h.Service.Get(r.Context(), matchID); err != nil {
		writeErr(w, http.StatusNotFound, "match not found")
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusInternalError, "closed")

	done := h.Hub.Subscribe(matchID, deviceID, conn)
	defer h.Hub.Unsubscribe(matchID, conn)

	// keep reading so close frames and ping/pong are serviced
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-readDone:
	case <-r.Context().Done():
	}
}
