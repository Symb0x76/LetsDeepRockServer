package stun

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/letsdeeprock/coordinator/internal/relay"
	"github.com/pion/logging"
	"github.com/pion/turn/v4"
)

// startTestServer boots a TURN-mode server on an ephemeral port and returns
// the server plus the address clients should dial.
func startTestServer(t *testing.T, ctx context.Context) (srv *Server, addr string, secret string, relayIP string, allocations *[]relay.Allocation) {
	t.Helper()

	// ephemeral UDP port for the STUN/TURN listener
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := pc.LocalAddr().(*net.UDPAddr).Port
	_ = pc.Close()

	secret = "test-relay-secret"
	relayIP = "127.0.0.1"
	var allocs []relay.Allocation
	var mu sync.Mutex

	srv = New(Config{
		Addr:              fmt.Sprintf("127.0.0.1:%d", port),
		SharedSecret:      secret,
		RelayAddr:         relayIP,
		RelayMinPort:      52000, // tests use a high range to avoid clashing with the real service
		RelayMaxPort:      52999,
		MaxAllocsPerUser:  1, // tight quota so the quota test is meaningful
		OnAllocation: func(username string, clientAddr, relayAddr net.Addr) {
			mu.Lock()
			defer mu.Unlock()
			matchID, peerID, err := relay.ParseUsername(username)
			if err != nil {
				return
			}
			rp := relayAddr.(*net.UDPAddr).Port
			allocs = append(allocs, relay.Allocation{
				AllocationID: relay.AllocationID(clientAddr.String(), relayAddr.String(), "udp"),
				MatchID:      matchID,
				PeerID:       peerID,
				Token:        username,
				RelayAddr:    relayIP,
				RelayPort:    rp,
				ExpiresAt:    time.Now().Add(time.Hour),
			})
		},
	})
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	return srv, addr, secret, relayIP, &allocs
}

// newTurnClient dials the TURN server with a relay credential and allocates
// a relayed socket, mirroring what the UE peer will do.
func newTurnClient(t *testing.T, addr, username, password string) (client *turn.Client, relayConn net.PacketConn) {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("client listen: %v", err)
	}
	client, err = turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           conn,
		Username:       username,
		Password:       password,
		Realm:          "letsdeeprock",
		LoggerFactory:  logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.Listen(); err != nil {
		t.Fatalf("client listen: %v", err)
	}
	t.Cleanup(client.Close)

	relayConn, err = client.Allocate()
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	t.Cleanup(func() { _ = relayConn.Close() })
	return client, relayConn
}

// TestTURNAllocateAndForward verifies the full relay path: credential ->
// allocate -> create permission -> bidirectional datagram forwarding.
func TestTURNAllocateAndForward(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, addr, secret, _, _ := startTestServer(t, ctx)

	cred := relay.Credentials(secret, addr, "match-1", "peer-1", time.Hour)
	client, relayConn := newTurnClient(t, addr, cred.Username, cred.Password)

	// the relayed address is on the server, not the client
	relayedAddr := relayConn.LocalAddr().(*net.UDPAddr)
	if relayedAddr.Port == 0 {
		t.Fatal("relay address has no port")
	}

	// peer socket that will exchange datagrams through the relay
	peerConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("peer listen: %v", err)
	}
	defer peerConn.Close()

	// punch a permission for the peer's address
	peerAddr := peerConn.LocalAddr()
	if err := client.CreatePermission(peerAddr); err != nil {
		t.Fatalf("create permission: %v", err)
	}

	// client -> relay -> peer
	_, err = relayConn.WriteTo([]byte("ping"), peerAddr)
	if err != nil {
		t.Fatalf("relay write: %v", err)
	}
	buf := make([]byte, 1600)
	_ = peerConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, from, err := peerConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("peer read: %v", err)
	}
	if string(buf[:n]) != "ping" {
		t.Fatalf("peer got %q, want ping", buf[:n])
	}
	t.Logf("client->relay->peer ok: %d bytes from %s", n, from)

	// peer -> relay -> client
	_, err = peerConn.WriteTo([]byte("pong"), relayedAddr)
	if err != nil {
		t.Fatalf("peer write: %v", err)
	}
	_ = relayConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, from, err = relayConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("relay read: %v", err)
	}
	if string(buf[:n]) != "pong" {
		t.Fatalf("relay got %q, want pong", buf[:n])
	}
	t.Logf("peer->relay->client ok: %d bytes from %s", n, from)
}

// TestTURNAllocationPersisted verifies the allocation event reaches the
// relay service and can be parsed back into match/peer.
func TestTURNAllocationPersisted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, addr, secret, _, allocations := startTestServer(t, ctx)

	cred := relay.Credentials(secret, addr, "match-1", "peer-1", time.Hour)
	_, relayConn := newTurnClient(t, addr, cred.Username, cred.Password)

	relayedAddr := relayConn.LocalAddr().(*net.UDPAddr)

	// wait for the OnAllocation callback to fire
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(*allocations) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(*allocations) == 0 {
		t.Fatal("allocation event never fired")
	}

	a := (*allocations)[0]
	if a.MatchID != "match-1" || a.PeerID != "peer-1" {
		t.Fatalf("allocation match/peer mismatch: %+v", a)
	}
	if a.RelayPort != relayedAddr.Port {
		t.Fatalf("relay port %d != allocated %d", a.RelayPort, relayedAddr.Port)
	}
}

// TestTURNRejectsBadCredentials verifies that an allocation with an unknown
// credential is refused.
func TestTURNRejectsBadCredentials(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, addr, _, _, _ := startTestServer(t, ctx)

	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           conn,
		Username:       "1234567890:match-1:peer-1",
		Password:       "wrong-password",
		Realm:          "letsdeeprock",
		LoggerFactory:  logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer client.Close()

	if _, err := client.Allocate(); err == nil {
		t.Fatal("allocation with wrong password should fail")
	}
}

// TestTURNQuotaReached verifies per-credential quota enforcement: a second
// allocation with the same credential but a different source address is
// refused. (The same five-tuple retry is rejected earlier by pion's
// duplicate-allocation check, which never reaches the quota handler.)
func TestTURNQuotaReached(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, addr, secret, _, _ := startTestServer(t, ctx)

	cred := relay.Credentials(secret, addr, "match-1", "peer-1", time.Hour)

	// first allocation from client A
	_, relayConn1 := newTurnClient(t, addr, cred.Username, cred.Password)
	_ = relayConn1

	// second allocation with the same credential from a different source
	// address must hit the per-username quota
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	client2, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: addr,
		TURNServerAddr: addr,
		Conn:           conn,
		Username:       cred.Username,
		Password:       cred.Password,
		Realm:          "letsdeeprock",
		LoggerFactory:  logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client2.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer client2.Close()

	if _, err := client2.Allocate(); err == nil {
		t.Fatal("second allocation with same credential should hit quota")
	}
}
