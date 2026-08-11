package relay

import (
	"strings"
	"testing"
	"time"
)

func TestCredentialsRoundTrip(t *testing.T) {
	secret := "test-shared-secret"
	cred := Credentials(secret, "203.0.113.7:3478", "match-1", "peer-1", time.Hour)

	if !strings.HasPrefix(cred.Username, "1") { // unix timestamp
		t.Fatalf("username should start with expiry timestamp, got %q", cred.Username)
	}
	if cred.RelayAddr != "203.0.113.7:3478" {
		t.Fatalf("relayAddr mismatch: %q", cred.RelayAddr)
	}
	if cred.Password == "" {
		t.Fatal("password should not be empty")
	}

	// the server-side parser must recover match/peer and accept the credential
	matchID, peerID, err := ParseUsername(cred.Username)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if matchID != "match-1" || peerID != "peer-1" {
		t.Fatalf("got %q/%q, want match-1/peer-1", matchID, peerID)
	}

	// the auth key must be reproducible from the shared secret
	_, err = LongTermPassword(secret, cred.Username)
	if err != nil {
		t.Fatalf("LongTermPassword: %v", err)
	}
}

func TestParseUsernameRejects(t *testing.T) {
	cases := []struct {
		name     string
		username string
	}{
		{"too few parts", "12345:match1"},
		{"too many parts", "12345:match1:peer1:extra"},
		{"non-numeric timestamp", "abc:match1:peer1"},
		{"expired timestamp", "1:match1:peer1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := ParseUsername(tc.username); err == nil {
				t.Fatalf("expected error for %q", tc.username)
			}
		})
	}
}

func TestAllocationIDStable(t *testing.T) {
	a := AllocationID("192.168.1.5:50000", "203.0.113.7:49152", "udp")
	b := AllocationID("192.168.1.5:50000", "203.0.113.7:49152", "udp")
	if a != b {
		t.Fatalf("allocation id must be deterministic, got %q vs %q", a, b)
	}
	c := AllocationID("192.168.1.5:50001", "203.0.113.7:49152", "udp")
	if a == c {
		t.Fatal("different client port must produce different allocation id")
	}
}
