package stun

import (
	"net"
	"testing"
)

func TestResolveRelayIP(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		want     string // empty means expect nil
		loopback bool   // accept 127.0.0.1 or ::1
	}{
		{"empty", "", "", false},
		{"bare ipv4", "203.0.113.7", "203.0.113.7", false},
		{"ipv4 with port", "203.0.113.7:3478", "203.0.113.7", false},
		{"bare ipv6", "2001:db8::1", "2001:db8::1", false},
		{"bracketed ipv6 with port", "[2001:db8::1]:3478", "2001:db8::1", false},
		{"localhost domain", "localhost:3478", "", true},
		{"bare localhost", "localhost", "", true},
		// unresolvable domain must yield nil so pion rejects allocations
		{"unresolvable", "no-such-host.invalid:3478", "", false},
		{"garbage", ":::", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveRelayIP(tc.in)
			if tc.want == "" && !tc.loopback {
				if got != nil {
					t.Fatalf("resolveRelayIP(%q) = %v, want nil", tc.in, got)
				}
				return
			}
			if got == nil {
				t.Fatalf("resolveRelayIP(%q) = nil, want %s", tc.in, tc.want)
			}
			if tc.loopback {
				if !got.IsLoopback() {
					t.Fatalf("resolveRelayIP(%q) = %v, want a loopback address", tc.in, got)
				}
				return
			}
			if !got.Equal(net.ParseIP(tc.want)) {
				t.Fatalf("resolveRelayIP(%q) = %v, want %s", tc.in, got, tc.want)
			}
		})
	}
}
