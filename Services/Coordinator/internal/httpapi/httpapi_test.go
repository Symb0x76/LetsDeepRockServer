package httpapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/letsdeeprock/coordinator/internal/auth"
	"github.com/letsdeeprock/coordinator/internal/candidate"
	"github.com/letsdeeprock/coordinator/internal/lease"
	"github.com/letsdeeprock/coordinator/internal/match"
	"github.com/letsdeeprock/coordinator/internal/store"
)

// testPool connects and applies migrations so tests are self-contained.
func testPool(t *testing.T) *pgxpool.Pool {
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

// testServer builds a full coordinator HTTP handler wired to the real DB.
func testServer(t *testing.T) *httptest.Server {
	t.Helper()
	pool := testPool(t)
	authSvc := auth.New(pool, "test-secret")
	hub := match.NewHub()
	matchSvc := match.NewService(pool)
	leaseSvc := lease.NewService(pool, 30*time.Second)
	candSvc := candidate.NewService(pool)

	dev := &DeviceAuthHandler{Auth: authSvc}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/devices/register", dev.Register)
	mux.HandleFunc("POST /v1/devices/challenge", dev.Challenge)
	mux.HandleFunc("POST /v1/devices/session", dev.Session)

	mh := &MatchHandler{Service: matchSvc, Hub: hub}
	mux.HandleFunc("POST /v1/matches", RequireDevice(authSvc, mh.Create))
	mux.HandleFunc("POST /v1/matches/{matchId}/join", RequireDevice(authSvc, mh.Join))
	mux.HandleFunc("GET /v1/matches/{matchId}/events", RequireDevice(authSvc, mh.Events))

	lh := &LeaseHandler{Service: leaseSvc, Hub: hub}
	mux.HandleFunc("POST /v1/matches/{matchId}/lease/claim", RequireDevice(authSvc, lh.Claim))

	ch := &CandidateHandler{Service: candSvc, Leases: leaseSvc, Hub: hub}
	mux.HandleFunc("POST /v1/matches/{matchId}/peers/{peerId}/candidates", RequireDevice(authSvc, ch.AddCandidate))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// registerAndToken does the full challenge->register->session dance and
// returns the access token plus the device id from the token's sub claim.
func registerAndToken(t *testing.T, srv *httptest.Server) (token, deviceID string) {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	pk := base64.RawURLEncoding.EncodeToString(pub)

	postJSON(t, srv.URL+"/v1/devices/register", `{"publicKey":"`+pk+`"}`)

	var ch struct{ Nonce string `json:"nonce"` }
	postJSONInto(t, srv.URL+"/v1/devices/challenge", `{"publicKey":"`+pk+`"}`, &ch)

	sig := base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte(ch.Nonce)))
	var sess struct{ Token string `json:"token"` }
	postJSONInto(t, srv.URL+"/v1/devices/session", `{"nonce":"`+ch.Nonce+`","signature":"`+sig+`"}`, &sess)

	parts := strings.Split(sess.Token, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	return sess.Token, claims["sub"].(string)
}

func postJSON(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// authedPost sends a POST with a bearer token.
func authedPost(t *testing.T, url, token, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("authed post %s: %v", url, err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func postJSONInto(t *testing.T, url, body string, v any) {
	t.Helper()
	resp := postJSON(t, url, body)
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func authedPostInto(t *testing.T, url, token, body string, v any) {
	t.Helper()
	resp := authedPost(t, url, token, body)
	if v != nil {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatalf("decode %s: %v", url, err)
		}
	}
}

func TestWebSocketLeaseGrantedEvent(t *testing.T) {
	srv := testServer(t)
	hostToken, hostDev := registerAndToken(t, srv)

	var m struct {
		MatchID string `json:"matchId"`
	}
	authedPostInto(t, srv.URL+"/v1/matches", hostToken, `{}`, &m)

	// subscribe to the match event stream
	wsURL := "ws" + srv.URL[len("http"):] + "/v1/matches/" + m.MatchID + "/events"
	header := http.Header{"Authorization": {"Bearer " + hostToken}}
	conn, _, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// claim lease -> should emit LeaseGranted on the socket
	req, _ := http.NewRequest("POST", srv.URL+"/v1/matches/"+m.MatchID+"/lease/claim",
		strings.NewReader(`{"peerId":"`+hostDev+`","expectedEpoch":1}`))
	req.Header.Set("Authorization", "Bearer "+hostToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("claim failed: %d", resp.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read ws event: %v", err)
	}
	var ev struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Type != "LeaseGranted" {
		t.Fatalf("expected LeaseGranted, got %s", ev.Type)
	}
}

func TestUnauthenticatedEventsRejected(t *testing.T) {
	srv := testServer(t)

	wsURL := "ws" + srv.URL[len("http"):] + "/v1/matches/nonexistent/events"
	_, _, err := websocket.Dial(context.Background(), wsURL, nil)
	if err == nil {
		t.Fatal("unauthenticated websocket should be rejected")
	}
}

func TestOldEpochCandidateRejected(t *testing.T) {
	srv := testServer(t)
	hostToken, hostDev := registerAndToken(t, srv)

	var m struct {
		MatchID string `json:"matchId"`
	}
	authedPostInto(t, srv.URL+"/v1/matches", hostToken, `{}`, &m)

	// advance match to epoch 1
	req, _ := http.NewRequest("POST", srv.URL+"/v1/matches/"+m.MatchID+"/lease/claim",
		strings.NewReader(`{"peerId":"`+hostDev+`","expectedEpoch":1}`))
	req.Header.Set("Authorization", "Bearer "+hostToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// stale epoch candidate must be rejected with 409
	body := `{"authorityEpoch":0,"rosterRevision":0,"transport":"UDP","candidateType":"Host",` +
		`"address":"10.0.0.1","port":1,"priority":1,"foundation":"stale"}`
	req2, _ := http.NewRequest("POST", srv.URL+"/v1/matches/"+m.MatchID+"/peers/"+hostDev+"/candidates",
		strings.NewReader(body))
	req2.Header.Set("Authorization", "Bearer "+hostToken)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("stale epoch candidate should be 409, got %d", resp2.StatusCode)
	}
}

func TestDuplicateRegisterIdempotent(t *testing.T) {
	srv := testServer(t)
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	pk := base64.RawURLEncoding.EncodeToString(pub)

	body := `{"publicKey":"` + pk + `"}`
	r1 := postJSON(t, srv.URL+"/v1/devices/register", body)
	r2 := postJSON(t, srv.URL+"/v1/devices/register", body)

	if r1.StatusCode != http.StatusCreated && r1.StatusCode != http.StatusOK {
		t.Fatalf("first register: %d", r1.StatusCode)
	}
	if r2.StatusCode != http.StatusOK {
		t.Fatalf("duplicate register should be idempotent 200, got %d", r2.StatusCode)
	}
}

func TestWebSocketReconnectStillReceivesEvents(t *testing.T) {
	srv := testServer(t)
	hostToken, hostDev := registerAndToken(t, srv)

	var m struct {
		MatchID string `json:"matchId"`
	}
	authedPostInto(t, srv.URL+"/v1/matches", hostToken, `{}`, &m)

	wsURL := "ws" + srv.URL[len("http"):] + "/v1/matches/" + m.MatchID + "/events"
	header := http.Header{"Authorization": {"Bearer " + hostToken}}

	// first connection, then drop it
	conn1, _, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial ws 1: %v", err)
	}
	conn1.Close(websocket.StatusNormalClosure, "client disconnect")

	// reconnect
	conn2, _, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("dial ws 2: %v", err)
	}
	defer conn2.Close(websocket.StatusNormalClosure, "")

	// claim -> event should arrive on the new connection
	req, _ := http.NewRequest("POST", srv.URL+"/v1/matches/"+m.MatchID+"/lease/claim",
		strings.NewReader(`{"peerId":"`+hostDev+`","expectedEpoch":1}`))
	req.Header.Set("Authorization", "Bearer "+hostToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := conn2.Read(ctx)
	if err != nil {
		t.Fatalf("read after reconnect: %v", err)
	}
	var ev struct {
		Type string `json:"type"`
	}
	json.Unmarshal(data, &ev)
	if ev.Type != "LeaseGranted" {
		t.Fatalf("expected LeaseGranted after reconnect, got %s", ev.Type)
	}
}
