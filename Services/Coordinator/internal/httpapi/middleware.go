package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/letsdeeprock/coordinator/internal/auth"
)

type contextKey string

const (
	ctxKeyDeviceID contextKey = "device_id"
	ctxKeyPeerID   contextKey = "peer_id"
)

// bearerToken extracts a device access token from the Authorization header.
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return ""
	}
	return strings.TrimSpace(h[len("Bearer "):])
}

// RequireDevice wraps a handler with JWT bearer validation.
func RequireDevice(a *auth.Auth, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := bearerToken(r)
		if tok == "" {
			writeErr(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		deviceID, err := a.ClaimsFromToken(tok)
		if err != nil {
			writeErr(w, http.StatusUnauthorized, "invalid token")
			return
		}
		ctx := context.WithValue(r.Context(), ctxKeyDeviceID, deviceID)
		next(w, r.WithContext(ctx))
	}
}

func deviceIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyDeviceID).(string)
	return v
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// parseJSON decodes a request body into v.
func parseJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	return dec.Decode(v)
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
