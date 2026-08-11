package httpapi

import (
	"net/http"

	"github.com/letsdeeprock/coordinator/internal/relay"
)

// RelayHandler serves TURN credential issuance and allocation queries.
type RelayHandler struct {
	Service *relay.Service
}

// POST /v1/matches/{matchId}/relay/credentials
// Issues a short-lived TURN credential for the requesting peer. The peer
// uses it to allocate a relayed address on the STUN/TURN listener.
func (h *RelayHandler) Credentials(w http.ResponseWriter, r *http.Request) {
	deviceID := deviceIDFrom(r.Context())
	matchID := r.PathValue("matchId")

	cred := h.Service.CredentialsFor(matchID, deviceID)
	writeJSON(w, http.StatusOK, map[string]any{
		"username":  cred.Username,
		"password":  cred.Password,
		"relayAddr": cred.RelayAddr,
		"expiresAt": cred.ExpiresAt,
	})
}
