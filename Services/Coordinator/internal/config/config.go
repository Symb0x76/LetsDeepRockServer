package config

import (
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr       string
	STUNAddr       string
	DatabaseURL    string
	MinIOEndpoint  string
	MinIOBucket    string
	MinIOAccessKey string
	MinIOSecretKey string
	JWTSecret      string
	TLSCertFile    string
	TLSKeyFile     string
	LeaseTTL       time.Duration

	// TURN relay support. When RelaySecret is empty the STUN listener runs
	// STUN-only (no allocations). RelayAddr is the public address advertised
	// in allocation responses.
	RelaySecret        string
	RelayAddr          string
	RelayMinPort       uint16
	RelayMaxPort       uint16
	RelayTTL           time.Duration
	RelayMaxAllocsUser int
}

func FromEnv() Config {
	return Config{
		HTTPAddr:       getenv("COORDINATOR_HTTP_ADDR", ":8443"),
		STUNAddr:       getenv("COORDINATOR_STUN_ADDR", ":3478"),
		DatabaseURL:    getenv("DATABASE_URL", "postgres://letsdeeprock:letsdeeprock123@127.0.0.1:5432/letsdeeprock"),
		MinIOEndpoint:  getenv("MINIO_ENDPOINT", "127.0.0.1:9000"),
		MinIOBucket:    getenv("MINIO_BUCKET", "checkpoints"),
		MinIOAccessKey: getenv("MINIO_ACCESS_KEY", "letsdeeprock"),
		MinIOSecretKey: getenv("MINIO_SECRET_KEY", "letsdeeprock123"),
		JWTSecret:      getenv("JWT_SECRET", "dev-only-secret-change-me"),
		TLSCertFile:    getenv("TLS_CERT_FILE", ""),
		TLSKeyFile:     getenv("TLS_KEY_FILE", ""),
		LeaseTTL:       30 * time.Second,

		RelaySecret:        getenv("RELAY_SECRET", ""),
		RelayAddr:          normalizeRelayAddr(getenv("RELAY_ADDR", "")),
		RelayMinPort:       uint16(getenvInt("RELAY_MIN_PORT", 49152)),
		RelayMaxPort:       uint16(getenvInt("RELAY_MAX_PORT", 65535)),
		RelayTTL:           time.Duration(getenvInt("RELAY_TTL_SECONDS", 3600)) * time.Second,
		RelayMaxAllocsUser: getenvInt("RELAY_MAX_ALLOCS_PER_USER", 8),
	}
}

// normalizeRelayAddr canonicalizes the TURN server address into host[:port].
// RELAY_ADDR historically held a bare IP (pion needs an IP, clients dial the
// server); both the stun and relay packages now consume host:port, so a bare
// IP/domain is completed with the default STUN port 3478.
func normalizeRelayAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	// Strip a scheme prefix if a deployer wrote turn:host:port.
	if strings.HasPrefix(strings.ToLower(addr), "turn:") {
		addr = addr[len("turn:"):]
	}
	// Bare IPv6 in brackets has no port; anything with a last colon already
	// carries a port (host:port or [v6]:port). A bare unbracketed IPv6 or a
	// bare host has none — append the default.
	if !strings.Contains(addr, ":") {
		return net.JoinHostPort(addr, "3478")
	}
	return addr
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
