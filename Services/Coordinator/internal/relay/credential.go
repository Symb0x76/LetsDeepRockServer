// Package relay implements TURN relay support for the Coordinator:
// short-lived REST credentials, allocation tracking and quota enforcement.
//
// Credentials follow the TURN REST API pattern (RFC 8489 §9.2 style, from
// draft-uberti-behave-turn-rest): the username is "timestamp:matchId:peerId"
// and the password is HMAC-SHA1(sharedSecret, username). The server verifies
// credentials statelessly; the timestamp makes them self-expiring.
package relay

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const UsernameSeparator = ":"

// Credential is a short-lived TURN credential for one match peer.
type Credential struct {
	Username  string    `json:"username"`
	Password  string    `json:"password"`
	RelayAddr string    `json:"relayAddr"` // TURN server address clients must reach
	ExpiresAt time.Time `json:"expiresAt"`
}

// Credentials issues a time-windowed TURN REST credential for a match peer.
// The username embeds the expiry timestamp so the TURN server can reject
// expired credentials without consulting the database. RelayAddr must be the
// public TURN address clients can actually reach.
func Credentials(sharedSecret, relayAddr, matchID, peerID string, ttl time.Duration) Credential {
	expires := time.Now().Add(ttl)
	username := strconv.FormatInt(expires.Unix(), 10) + UsernameSeparator + matchID + UsernameSeparator + peerID
	password, _ := LongTermPassword(sharedSecret, username)

	return Credential{
		Username:  username,
		Password:  password,
		RelayAddr: relayAddr,
		ExpiresAt: expires,
	}
}

// LongTermPassword derives the TURN REST password for a username:
// base64(HMAC-SHA1(sharedSecret, username)). The TURN server recomputes it
// from the shared secret to validate credentials statelessly.
func LongTermPassword(sharedSecret, username string) (string, error) {
	mac := hmac.New(sha1.New, []byte(sharedSecret))
	_, err := mac.Write([]byte(username))
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

// ParseUsername splits "timestamp:matchId:peerId" back apart. It returns an
// error for malformed or expired usernames.
func ParseUsername(username string) (matchID, peerID string, err error) {
	parts := strings.Split(username, UsernameSeparator)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("malformed turn username %q", username)
	}
	expiry, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", "", fmt.Errorf("malformed turn username timestamp: %w", err)
	}
	if expiry < time.Now().Unix() {
		return "", "", fmt.Errorf("expired turn credential")
	}
	return parts[1], parts[2], nil
}

// AllocationID derives a stable identifier for an allocation from its
// TURN five-tuple plus relay port, so a re-allocation with the same binding
// overwrites the previous record instead of duplicating it.
func AllocationID(clientAddr, relayAddr, protocol string) string {
	h := sha256.Sum256([]byte(clientAddr + "|" + relayAddr + "|" + protocol))
	return hex.EncodeToString(h[:8])
}
