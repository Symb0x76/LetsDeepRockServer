package config

import (
	"os"
	"time"
)

type Config struct {
	HTTPAddr      string
	STUNAddr      string
	DatabaseURL   string
	MinIOEndpoint string
	MinIOBucket   string
	MinIOAccessKey string
	MinIOSecretKey string
	JWTSecret     string
	TLSCertFile   string
	TLSKeyFile    string
	LeaseTTL      time.Duration
}

func FromEnv() Config {
	return Config{
		HTTPAddr:      getenv("COORDINATOR_HTTP_ADDR", ":8443"),
		STUNAddr:      getenv("COORDINATOR_STUN_ADDR", ":3478"),
		DatabaseURL:   getenv("DATABASE_URL", "postgres://letsdeeprock:letsdeeprock123@127.0.0.1:5432/letsdeeprock"),
		MinIOEndpoint: getenv("MINIO_ENDPOINT", "127.0.0.1:9000"),
		MinIOBucket:   getenv("MINIO_BUCKET", "checkpoints"),
		MinIOAccessKey: getenv("MINIO_ACCESS_KEY", "letsdeeprock"),
		MinIOSecretKey: getenv("MINIO_SECRET_KEY", "letsdeeprock123"),
		JWTSecret:     getenv("JWT_SECRET", "dev-only-secret-change-me"),
		TLSCertFile:   getenv("TLS_CERT_FILE", ""),
		TLSKeyFile:    getenv("TLS_KEY_FILE", ""),
		LeaseTTL:      30 * time.Second,
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
