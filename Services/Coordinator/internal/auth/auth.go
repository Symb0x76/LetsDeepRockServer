// Package auth implements device authentication: Ed25519 challenge
// signing, device registration and short-lived JWT access tokens.
//
// Flow: client generates an Ed25519 keypair, requests a nonce, signs it,
// and receives a short-lived access token. Join codes are match-scoped
// and never serve as long-term identity.
package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	nonceTTL   = 5 * time.Minute
	tokenTTL   = 24 * time.Hour
	challengeSize = 32
)

var (
	ErrBadSignature = errors.New("signature verification failed")
	ErrNonceExpired = errors.New("nonce expired or not found")
	ErrDeviceExists = errors.New("device already registered")
	ErrUnknownDevice = errors.New("unknown device")
)

type Auth struct {
	pool      *pgxpool.Pool
	jwtSecret []byte
}

// Challenge is a server-issued nonce plus the data a client must sign.
type Challenge struct {
	Nonce   string    `json:"nonce"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func New(pool *pgxpool.Pool, jwtSecret string) *Auth {
	return &Auth{pool: pool, jwtSecret: []byte(jwtSecret)}
}

// NewChallenge mints a nonce tied to the requesting public key so a stolen
// challenge cannot be replayed for a different key.
func (a *Auth) NewChallenge(ctx context.Context, publicKey ed25519.PublicKey) (Challenge, error) {
	raw := make([]byte, challengeSize)
	if _, err := rand.Read(raw); err != nil {
		return Challenge{}, fmt.Errorf("nonce: %w", err)
	}
	nonce := hex.EncodeToString(raw)
	exp := time.Now().Add(nonceTTL)

	_, err := a.pool.Exec(ctx, `
		INSERT INTO device_challenges (nonce, public_key, expires_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (nonce) DO UPDATE SET public_key = EXCLUDED.public_key, expires_at = EXCLUDED.expires_at`,
		nonce, []byte(publicKey), exp)
	if err != nil {
		return Challenge{}, fmt.Errorf("store nonce: %w", err)
	}
	return Challenge{Nonce: nonce, ExpiresAt: exp}, nil
}

// VerifyChallenge checks the Ed25519 signature over the nonce, consumes the
// nonce (one-time use), and returns the authenticated public key.
func (a *Auth) VerifyChallenge(ctx context.Context, nonce string, signature []byte) (ed25519.PublicKey, error) {
	var storedKey []byte
	var expiresAt time.Time
	err := a.pool.QueryRow(ctx,
		`SELECT public_key, expires_at FROM device_challenges WHERE nonce = $1 AND consumed_at IS NULL`,
		nonce).Scan(&storedKey, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNonceExpired
	}
	if err != nil {
		return nil, err
	}
	if time.Now().After(expiresAt) {
		return nil, ErrNonceExpired
	}

	if len(storedKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("stored key wrong size: %d", len(storedKey))
	}

	msg := []byte(nonce)
	if !ed25519.Verify(ed25519.PublicKey(storedKey), msg, signature) {
		// consume the nonce on failure too: failed attempts must not retry
		_, _ = a.pool.Exec(ctx, `UPDATE device_challenges SET consumed_at = now() WHERE nonce = $1`, nonce)
		return nil, ErrBadSignature
	}

	_, _ = a.pool.Exec(ctx, `UPDATE device_challenges SET consumed_at = now() WHERE nonce = $1`, nonce)
	return ed25519.PublicKey(storedKey), nil
}

// Register creates a device identity. Idempotent: a public key that already
// exists returns the existing device id.
func (a *Auth) Register(ctx context.Context, publicKey ed25519.PublicKey) (string, bool, error) {
	var existing string
	err := a.pool.QueryRow(ctx,
		`SELECT device_id FROM devices WHERE public_key = $1`, publicKey).Scan(&existing)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}

	deviceID := uuid.NewString()
	_, err = a.pool.Exec(ctx,
		`INSERT INTO devices (device_id, public_key) VALUES ($1, $2)`,
		deviceID, publicKey)
	if err != nil {
		// lost the race: another concurrent register created it
		var dup string
		if err2 := a.pool.QueryRow(ctx,
			`SELECT device_id FROM devices WHERE public_key = $1`, publicKey).Scan(&dup); err2 == nil {
			return dup, false, nil
		}
		return "", false, fmt.Errorf("%w: %v", ErrDeviceExists, err)
	}
	return deviceID, true, nil
}

// DeviceIDForPublicKey resolves the registered device id for a verified key.
func (a *Auth) DeviceIDForPublicKey(ctx context.Context, publicKey ed25519.PublicKey) (string, error) {
	var deviceID string
	err := a.pool.QueryRow(ctx,
		`SELECT device_id FROM devices WHERE public_key = $1`, publicKey).Scan(&deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrUnknownDevice
	}
	return deviceID, err
}

// Token issues a short-lived JWT for the authenticated device.
func (a *Auth) Token(deviceID string, publicKey ed25519.PublicKey) (string, time.Time, error) {
	exp := time.Now().Add(tokenTTL)
	claims := jwt.MapClaims{
		"sub":  deviceID,
		"pk":   base64.RawURLEncoding.EncodeToString(publicKey),
		"exp":  exp.Unix(),
		"iat":  time.Now().Unix(),
		"type": "device-access",
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(a.jwtSecret)
	if err != nil {
		return "", time.Time{}, err
	}
	return signed, exp, nil
}

// ClaimsFromToken validates a bearer token and returns the device id.
func (a *Auth) ClaimsFromToken(tokenString string) (deviceID string, err error) {
	tok, err := jwt.Parse(tokenString, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return a.jwtSecret, nil
	}, jwt.WithValidMethods([]string{"HS256"}))
	if err != nil {
		return "", err
	}
	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok || !tok.Valid {
		return "", errors.New("invalid token")
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", errors.New("token missing subject")
	}
	return sub, nil
}
