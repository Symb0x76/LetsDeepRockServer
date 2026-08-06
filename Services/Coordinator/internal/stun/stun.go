// Package stun runs the standard STUN Binding service on UDP.
//
// Built on pion/turn, which contains a full RFC 5389 STUN server; we add
// per-IP request gap limiting. STUN only provides server-reflexive
// candidates; it never guarantees direct connectivity.
package stun

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/pion/turn/v4"
)

// Server wraps the pion/turn server with per-IP rate limiting.
type Server struct {
	srv      *turn.Server
	addr     string
	limitMu  sync.Mutex
	lastSeen map[string]time.Time
	minGap   time.Duration
}

// Config controls the STUN listener.
type Config struct {
	Addr   string // UDP listen address, e.g. ":3478"
	MinGap time.Duration
}

func New(cfg Config) *Server {
	if cfg.MinGap == 0 {
		cfg.MinGap = 200 * time.Millisecond
	}
	return &Server{
		addr:     cfg.Addr,
		lastSeen: make(map[string]time.Time),
		minGap:   cfg.MinGap,
	}
}

// Start binds the UDP listener and begins serving STUN requests.
func (s *Server) Start(ctx context.Context) error {
	udpListener, err := net.ListenPacket("udp4", s.addr)
	if err != nil {
		return err
	}

	srv, err := turn.NewServer(turn.ServerConfig{
		Realm: "letsdeeprock",
		// STUN Binding requests carry no credentials; anything else is
		// rejected (no key returned) so the service cannot be abused as
		// an open TURN relay.
		AuthHandler: func(username, realm string, srcAddr net.Addr) ([]byte, bool) {
			return nil, false
		},
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: udpListener,
			// No relay allocations are offered (STUN-only); the generator
			// requires a listening address for its relay socket.
			RelayAddressGenerator: &turn.RelayAddressGeneratorNone{
				Address: "0.0.0.0",
			},
		}},
	})
	if err != nil {
		return err
	}
	s.srv = srv

	slog.Info("stun listening", "addr", s.addr)
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	go s.pruneLoop(ctx)
	return nil
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
