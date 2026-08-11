// Package stun runs the standard STUN Binding service on UDP, and in TURN
// mode also serves relay allocations for the same listener.
//
// Built on pion/turn: STUN Binding requests are served without credentials;
// TURN requests are accepted only with a valid time-windowed credential
// (TURN REST pattern, see internal/relay). STUN only provides
// server-reflexive candidates; relay paths require a TURN allocation.
package stun

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/letsdeeprock/coordinator/internal/relay"
	"github.com/pion/turn/v4"
)

// Server wraps the pion/turn server with per-IP rate limiting and optional
// TURN allocation support.
type Server struct {
	srv      *turn.Server
	addr     string
	limitMu  sync.Mutex
	lastSeen map[string]time.Time
	minGap   time.Duration

	// TURN allocation support (nil when STUN-only).
	turnMu        sync.RWMutex
	turnEnabled   bool
	relayAddr     net.IP
	relayMinPort  uint16
	relayMaxPort  uint16
	sharedSecret  string
	maxAllocsUser int
	onAllocation  func(username string, clientAddr, relayAddr net.Addr) // nil when STUN-only
}

// Config controls the STUN/TURN listener.
type Config struct {
	Addr   string // UDP listen address, e.g. ":3478"
	MinGap time.Duration

	// TURN mode. When SharedSecret is empty the server is STUN-only.
	SharedSecret string
	RelayAddr    string // public IP advertised in allocation responses
	RelayMinPort uint16
	RelayMaxPort uint16
	MaxAllocsPerUser int
	OnAllocation func(username string, clientAddr, relayAddr net.Addr)
}

func New(cfg Config) *Server {
	if cfg.MinGap == 0 {
		cfg.MinGap = 200 * time.Millisecond
	}
	return &Server{
		addr:           cfg.Addr,
		lastSeen:       make(map[string]time.Time),
		minGap:         cfg.MinGap,
		turnEnabled:    cfg.SharedSecret != "",
		relayAddr:      net.ParseIP(cfg.RelayAddr),
		relayMinPort:   cfg.RelayMinPort,
		relayMaxPort:   cfg.RelayMaxPort,
		sharedSecret:   cfg.SharedSecret,
		maxAllocsUser:  cfg.MaxAllocsPerUser,
		onAllocation:   cfg.OnAllocation,
	}
}

// Start binds the UDP listener and begins serving STUN (and TURN) requests.
func (s *Server) Start(ctx context.Context) error {
	udpListener, err := net.ListenPacket("udp4", s.addr)
	if err != nil {
		return err
	}

	cfg := turn.ServerConfig{
		Realm: "letsdeeprock",
		// STUN Binding requests carry no credentials; anything else is
		// rejected (no key returned) so the service cannot be abused as
		// an open TURN relay.
		AuthHandler: func(username, realm string, srcAddr net.Addr) ([]byte, bool) {
			return s.auth(username, realm, srcAddr)
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: udpListener,
		}},
	}

	if s.turnEnabled {
		cfg.PacketConnConfigs[0].RelayAddressGenerator = &turn.RelayAddressGeneratorPortRange{
			RelayAddress: s.relayAddr,
			Address:      "0.0.0.0",
			MinPort:      s.relayMinPort,
			MaxPort:      s.relayMaxPort,
		}
		if s.maxAllocsUser > 0 {
			cfg.QuotaHandler = func(username, realm string, srcAddr net.Addr) bool {
				ok := s.allowAllocation(username, srcAddr)
				if !ok {
					slog.Warn("turn allocation quota reached", "username", username)
				}
				return ok
			}
		}
		cfg.EventHandler = s.eventHandler()
	} else {
		// No relay allocations are offered (STUN-only); the generator
		// requires a listening address for its relay socket.
		cfg.PacketConnConfigs[0].RelayAddressGenerator = &turn.RelayAddressGeneratorNone{
			Address: "0.0.0.0",
		}
	}

	srv, err := turn.NewServer(cfg)
	if err != nil {
		return err
	}
	s.srv = srv

	if s.turnEnabled {
		slog.Info("turn listening",
			"addr", s.addr, "relay_port_min", s.relayMinPort, "relay_port_max", s.relayMaxPort)
	} else {
		slog.Info("stun listening", "addr", s.addr)
	}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	go s.pruneLoop(ctx)
	return nil
}

// auth validates TURN credentials. It never denies Binding requests (pion
// serves those before consulting AuthHandler); denying here would only
// reject Allocate/Refresh/CreatePermission/ChannelBind.
func (s *Server) auth(username, realm string, srcAddr net.Addr) ([]byte, bool) {
	if !s.turnEnabled {
		return nil, false
	}
	// The credential is self-expiring: parse the embedded timestamp. No
	// database lookup needed for validity.
	matchID, peerID, err := relay.ParseUsername(username)
	if err != nil {
		slog.Warn("turn credential rejected", "username", username, "err", err)
		return nil, false
	}
	// Harden the parse: the credential must describe a real, non-empty
	// match and peer, so opaque usernames cannot be probed.
	if matchID == "" || peerID == "" {
		slog.Warn("turn credential with empty match/peer rejected", "username", username)
		return nil, false
	}
	// TURN long-term credential key: MD5(username:realm:password) where
	// password is base64(HMAC-SHA1(secret, username)) — the TURN REST form.
	password, _ := relay.LongTermPassword(s.sharedSecret, username)
	return turn.GenerateAuthKey(username, realm, password), true
}

// allowAllocation enforces a per-credential allocation quota for TURN.
// The quota key is the credential username (RFC 5766 §5.7 recommends
// keying on the authenticated username, not the transport address).
func (s *Server) allowAllocation(username string, srcAddr net.Addr) bool {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	now := time.Now()
	cutoff := now.Add(-time.Minute)
	count := 0
	for k, ts := range s.lastSeen {
		if ts.Before(cutoff) {
			delete(s.lastSeen, k)
			continue
		}
		if strings.HasPrefix(k, "alloc|"+username+"|") {
			count++
		}
	}
	if count >= s.maxAllocsUser {
		return false
	}
	s.lastSeen["alloc|"+username+"|"+now.Format(time.RFC3339Nano)] = now
	return true
}

// eventHandler reports allocation lifecycle events so the coordinator can
// record relay_allocations rows for relayed candidates.
func (s *Server) eventHandler() turn.EventHandler {
	return turn.EventHandler{
		OnAllocationCreated: func(srcAddr, dstAddr net.Addr, protocol, username, realm string, relayAddr net.Addr, requestedPort int) {
			if s.onAllocation != nil {
				s.onAllocation(username, srcAddr, relayAddr)
			}
			slog.Info("turn allocation created",
				"username", username, "client", srcAddr.String(), "relay", relayAddr.String())
		},
		OnAllocationDeleted: func(srcAddr, dstAddr net.Addr, protocol, username, realm string) {
			slog.Info("turn allocation deleted", "username", username, "client", srcAddr.String())
		},
		OnAllocationError: func(srcAddr, dstAddr net.Addr, protocol, message string) {
			slog.Warn("turn allocation error", "client", srcAddr.String(), "message", message)
		},
	}
}

// SetOnAllocation installs the callback invoked when a TURN allocation is
// created (with the credential username and the client/relay addresses).
// Only meaningful in TURN mode; safe to call before Start.
func (s *Server) SetOnAllocation(fn func(username string, clientAddr, relayAddr net.Addr)) {
	s.turnMu.Lock()
	defer s.turnMu.Unlock()
	s.onAllocation = fn
}

// AllowRequest reports whether a source IP may send another STUN request.
func (s *Server) AllowRequest(ip string) bool {
	now := time.Now()
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	last, ok := s.lastSeen[ip]
	if ok && now.Sub(last) < s.minGap {
		return false
	}
	s.lastSeen[ip] = now
	return true
}

func (s *Server) pruneLoop(ctx context.Context) {
	t := time.NewTicker(2 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.limitMu.Lock()
			for ip, ts := range s.lastSeen {
				if time.Since(ts) > 5*time.Minute {
					delete(s.lastSeen, ip)
				}
			}
			s.limitMu.Unlock()
		}
	}
}

func (s *Server) Close() error {
	if s.srv != nil {
		return s.srv.Close()
	}
	return nil
}
