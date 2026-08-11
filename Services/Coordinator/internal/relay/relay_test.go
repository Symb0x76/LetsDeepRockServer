package relay

import (
	"crypto/md5"
	"testing"
	"time"

	"github.com/pion/turn/v4"
)

// TestIssueDerivePasswordRoundTrip verifies that a credential issued by
// Issue is accepted by the server-side auth derivation (same username,
// same secret, same derived password).
func TestIssueDerivePasswordRoundTrip(t *testing.T) {
	secret := "test-secret"
	realm := "letsdeeprock"
	creds := Issue(secret, realm, ":3479", "device-123")

	expiry, deviceID, ok := parseUsername(creds.Username)
	if !ok {
		t.Fatalf("parseUsername(%q) rejected own credential", creds.Username)
	}
	if deviceID != "device-123" {
		t.Fatalf("deviceID = %q, want device-123", deviceID)
	}
	if time.Now().Unix() > expiry {
		t.Fatalf("credential already expired: %d", expiry)
	}

	derived := derivePassword(secret, creds.Username)
	if derived != creds.Password {
		t.Fatalf("derived password mismatch: %q != %q", derived, creds.Password)
	}
}

// TestAuthKeyMatchesUE verifies the server-side auth key computation
// matches what the UE client computes:
//   key = MD5(username ":" realm ":" password)
//   password = base64(HMAC-SHA1(secret, username))
func TestAuthKeyMatchesUE(t *testing.T) {
	secret := "test-secret"
	realm := "letsdeeprock"
	username := "1234567890:device-1"

	password := derivePassword(secret, username)

	// UE MakeAuthKey: MD5(username:realm:password)
	h := md5.New()
	h.Write([]byte(username + ":" + realm + ":" + password))
	wantKey := h.Sum(nil)

	// Server AuthHandler: turn.GenerateAuthKey(username, realm, password)
	gotKey := turn.GenerateAuthKey(username, realm, password)
	if string(gotKey) != string(wantKey) {
		t.Fatalf("auth key mismatch: got %x want %x", gotKey, wantKey)
	}
}

func TestParseUsernameRejectsMalformed(t *testing.T) {
	for _, u := range []string{"", ":", "abc:def", "123:", ":device", "123:device:extra", "notanumber:device"} {
		if _, _, ok := parseUsername(u); ok {
			t.Fatalf("parseUsername(%q) unexpectedly accepted", u)
		}
	}
}

func TestParseUsernameAcceptsValid(t *testing.T) {
	_, deviceID, ok := parseUsername("9999999999:dev-42")
	if !ok || deviceID != "dev-42" {
		t.Fatalf("parseUsername valid case failed: %v %q", ok, deviceID)
	}
}
