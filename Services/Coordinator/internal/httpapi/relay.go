package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsdeeprock/coordinator/internal/relay"
)

// RelayHandler serves per-match TURN relay credential issuance.
type RelayHandler struct {
	Pool      *pgxpool.Pool
	Secret    string
	Realm     string
	RelayAddr string
}

// POST /v1/matches/{matchId}/relay/credentials
//
// Issues short-lived TURN long-term credentials for the requesting device.
// The device must be a member of the match (checked against match_peers).
func (h *RelayHandler) Credentials(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")

	var one int
	err := h.Pool.QueryRow(r.Context(),
		`SELECT 1 FROM match_peers WHERE match_id = $1 AND device_id = $2`,
		matchID, deviceID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		writeErr(w, http.StatusForbidden, "not a member")
		return
	}
	if err != nil {
		slog.Error("relay credentials membership check failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "membership check failed")
		return
	}

	creds := relay.Issue(h.Secret, h.Realm, h.RelayAddr, deviceID)
	writeJSON(w, http.StatusOK, creds)
}
