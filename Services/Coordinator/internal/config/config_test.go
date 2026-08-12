package config

import "testing"

func TestNormalizeRelayAddr(t *testing.T) {
	cases := map[string]string{
		"114.224.20.98":      "114.224.20.98:3478",
		"114.224.20.98:3478": "114.224.20.98:3478",
		"symb0x76.top":       "symb0x76.top:3478",
		"symb0x76.top:3478":  "symb0x76.top:3478",
		"turn:symb0x76.top":  "symb0x76.top:3478",
		"":                   "",
		"  ":                 "",
	}
	for in, want := range cases {
		if got := normalizeRelayAddr(in); got != want {
			t.Errorf("normalizeRelayAddr(%q) = %q, want %q", in, got, want)
		}
	}
}
