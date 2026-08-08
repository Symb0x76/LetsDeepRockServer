package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsdeeprock/coordinator/internal/store"
)

// testStore connects to the local dev database and applies schema
// migrations so tests are self-contained on a fresh database.
// Set COORDINATOR_TEST_DATABASE_URL to override.
func testStore(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("COORDINATOR_TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://letsdeeprock:letsdeeprock123@127.0.0.1:5432/letsdeeprock"
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	st := &store.Store{Pool: pool}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func TestChallengeVerifyRoundtrip(t *testing.T) {
	pool := testStore(t)
	a := New(pool, "test-secret")

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	ch, err := a.NewChallenge(context.Background(), pub)
	if err != nil {
		t.Fatalf("new challenge: %v", err)
	}

	got, err := a.VerifyChallenge(context.Background(), ch.Nonce, ed25519.Sign(priv, []byte(ch.Nonce)))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !bytes.Equal(got, pub) {
		t.Fatalf("public key mismatch")
	}
}

func TestChallengeReplayRejected(t *testing.T) {
	pool := testStore(t)
	a := New(pool, "test-secret")

	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	ch, _ := a.NewChallenge(context.Background(), pub)
	sig := ed25519.Sign(priv, []byte(ch.Nonce))

	if _, err := a.VerifyChallenge(context.Background(), ch.Nonce, sig); err != nil {
		t.Fatalf("first verify: %v", err)
	}
	if _, err := a.VerifyChallenge(context.Background(), ch.Nonce, sig); err != ErrNonceExpired {
		t.Fatalf("replay should fail with ErrNonceExpired, got %v", err)
	}
}

func TestChallengeBadSignatureConsumesNonce(t *testing.T) {
	pool := testStore(t)
	a := New(pool, "test-secret")

	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	ch, _ := a.NewChallenge(context.Background(), pub)

	badSig := ed25519.Sign(priv, []byte("wrong-message"))
	if _, err := a.VerifyChallenge(context.Background(), ch.Nonce, badSig); err != ErrBadSignature {
		t.Fatalf("bad signature should fail with ErrBadSignature, got %v", err)
	}

	// even with the right signature, the nonce is consumed
	goodSig := ed25519.Sign(priv, []byte(ch.Nonce))
	if _, err := a.VerifyChallenge(context.Background(), ch.Nonce, goodSig); err != ErrNonceExpired {
		t.Fatalf("consumed nonce should fail, got %v", err)
	}
}

func TestRegisterIdempotent(t *testing.T) {
	pool := testStore(t)
	a := New(pool, "test-secret")

	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	id1, created1, err := a.Register(context.Background(), pub)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if !created1 {
		t.Fatal("first register should create")
	}

	id2, created2, err := a.Register(context.Background(), pub)
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if created2 {
		t.Fatal("second register should not create")
	}
	if id1 != id2 {
		t.Fatalf("same key should resolve same id, got %s vs %s", id1, id2)
	}
}

func TestTokenClaims(t *testing.T) {
	a := New(testStore(t), "test-secret")

	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	tok, _, err := a.Token("device-1", pub)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := a.ClaimsFromToken(tok)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sub != "device-1" {
		t.Fatalf("sub mismatch: %s", sub)
	}

	if _, err := a.ClaimsFromToken("garbage"); err == nil {
		t.Fatal("garbage token should fail")
	}
}
