package httpapi

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/letsdeeprock/coordinator/internal/auth"
)

// DeviceAuthHandler serves the device registration/challenge/session flow.
type DeviceAuthHandler struct {
	Auth *auth.Auth
}

type challengeRequest struct {
	PublicKey string `json:"publicKey"` // base64url Ed25519 public key
}

type signRequest struct {
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"` // base64url signature over the nonce
}

type registerRequest struct {
	PublicKey string `json:"publicKey"`
}

// POST /v1/devices/challenge
func (h *DeviceAuthHandler) Challenge(w http.ResponseWriter, r *http.Request) {
	var req challengeRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	key, err := decodePublicKey(req.PublicKey)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid publicKey")
		return
	}
	ch, err := h.Auth.NewChallenge(r.Context(), key)
	if err != nil {
		slog.Error("challenge failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "challenge failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"nonce":     ch.Nonce,
		"expiresAt": ch.ExpiresAt.Format(time.RFC3339),
	})
}

// POST /v1/devices/session
// Requires the device to be registered first; returns a short-lived access
// token bound to the registered device id.
func (h *DeviceAuthHandler) Session(w http.ResponseWriter, r *http.Request) {
	var req signRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	sig, err := base64.RawURLEncoding.DecodeString(req.Signature)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid signature encoding")
		return
	}
	key, err := h.Auth.VerifyChallenge(r.Context(), req.Nonce, sig)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "challenge verification failed")
		return
	}
	deviceID, err := h.Auth.DeviceIDForPublicKey(r.Context(), key)
	if err != nil {
		writeErr(w, http.StatusForbidden, "device not registered")
		return
	}
	tok, exp, err := h.Auth.Token(deviceID, key)
	if err != nil {
		slog.Error("token mint failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "token mint failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"token":     tok,
		"expiresAt": exp.Format(time.RFC3339),
	})
}

// POST /v1/devices/register
func (h *DeviceAuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := parseJSON(r, &req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid body")
		return
	}
	key, err := decodePublicKey(req.PublicKey)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid publicKey")
		return
	}
	deviceID, created, err := h.Auth.Register(r.Context(), key)
	if err != nil {
		writeErr(w, http.StatusConflict, "device already registered")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, map[string]string{"deviceId": deviceID})
}

func decodePublicKey(b64 string) (ed25519.PublicKey, error) {
	raw, err := base64.RawURLEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	if len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("public key must be 32 bytes")
	}
	return ed25519.PublicKey(raw), nil
}
