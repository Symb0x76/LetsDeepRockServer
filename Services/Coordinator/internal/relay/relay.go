// Package relay implements short-lived TURN credential issuance and the
// TURN server that accepts them.
//
// The UE client asks the coordinator for per-match relay credentials
// (username/password), then allocates a relayed candidate from the TURN
// server. Credentials use the TURN long-term mechanism (RFC 8489): a
// username like "<expiry-epoch>:<random>" and a shared secret derived from
// the coordinator's JWT secret. The TURN server validates the same secret
// on every Allocate.
package relay

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net"
	"time"

	"github.com/pion/turn/v4"
)

const (
	// CredentialLifetime is how long an issued credential stays valid.
	CredentialLifetime = 2 * time.Hour
)

// Credentials is a short-lived TURN credential set issued for a match.
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Realm    string `json:"realm"`
	// RelayAddr is the TURN server address the client should contact
	// (host:port), same value as Coordinator's TURNADDR advertised to clients.
	RelayAddr string `json:"relayAddr"`
}

// Issue builds a TURN long-term credential for matchID/deviceID.
// The secret must be the coordinator's JWT secret so the TURN server can
// validate it independently.
func Issue(secret, realm, relayAddr, deviceID string) Credentials {
	expiry := time.Now().Add(CredentialLifetime).Unix()
	username := fmt.Sprintf("%d:%s", expiry, deviceID)
	return Credentials{
		Username:  username,
		Password:  derivePassword(secret, username),
		Realm:     realm,
		RelayAddr: relayAddr,
	}
}

// Server wraps a pion/turn server that accepts the long-term credentials
// issued by Issue. The shared secret can be rotated by calling SetSecret.
type Server struct {
	secretMu chan string
	realm    string
}

// NewServer creates a TURN server state. Start must be called to bind the
// UDP listener.
func NewServer(initialSecret, realm string) *Server {
	s := &Server{
		secretMu: make(chan string, 1),
		realm:    realm,
	}
	s.SetSecret(initialSecret)
	return s
}

// Start binds the TURN UDP listener on addr and begins serving.
func (s *Server) Start(addr string) (*turn.Server, error) {
	udpListener, err := net.ListenPacket("udp4", addr)
	if err != nil {
		return nil, fmt.Errorf("relay: bind %s: %w", addr, err)
	}
	// The generator requires a listen address; TURN relay allocations
	// share the packet listener.
	relayGen := &turn.RelayAddressGeneratorStatic{
		RelayAddress: udpListener.LocalAddr().(*net.UDPAddr).IP,
		Address:      "0.0.0.0",
	}
	turnSrv, err := turn.NewServer(turn.ServerConfig{
		Realm: s.realm,
		AuthHandler: func(username, realm string, srcAddr net.Addr) ([]byte, bool) {
			// RFC 8489: key = MD5(username:realm:password). The password is
			// derived from the shared secret (see Issue), so the server can
			// recompute it without storing per-user state.
			expiry, _, ok := parseUsername(username)
			if !ok {
				return nil, false
			}
			if time.Now().Unix() > expiry {
				return nil, false
			}
			password := derivePassword(s.currentSecret(), username)
			return turn.GenerateAuthKey(username, realm, password), true
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn:            udpListener,
			RelayAddressGenerator: relayGen,
		}},
	})
	if err != nil {
		_ = udpListener.Close()
		return nil, fmt.Errorf("relay: start turn server: %w", err)
	}
	return turnSrv, nil
}

// SetSecret rotates the shared secret used to validate credentials.
func (s *Server) SetSecret(secret string) {
	select {
	case s.secretMu <- secret:
	default:
		<-s.secretMu
		s.secretMu <- secret
	}
}

func (s *Server) currentSecret() string {
	select {
	case v := <-s.secretMu:
		s.secretMu <- v
		return v
	default:
		return ""
	}
}

// parseUsername splits "<expiry-epoch>:<deviceID>" and reports validity.
func parseUsername(username string) (expiry int64, deviceID string, ok bool) {
	sep := -1
	for i := 0; i < len(username); i++ {
		if username[i] == ':' {
			sep = i
			break
		}
	}
	if sep <= 0 || sep == len(username)-1 {
		return 0, "", false
	}
	// deviceID must not contain another colon
	for i := sep + 1; i < len(username); i++ {
		if username[i] == ':' {
			return 0, "", false
		}
	}
	var e int64
	if _, err := fmt.Sscanf(username[:sep], "%d", &e); err != nil {
		return 0, "", false
	}
	return e, username[sep+1:], true
}

// derivePassword computes the TURN long-term credential password from the
// shared secret: password = base64(HMAC-SHA1(secret, username)), matching
// Issue and the UE WeaponRelaySocket client.
func derivePassword(secret, username string) string {
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(username))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
