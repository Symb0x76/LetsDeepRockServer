package match

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/letsdeeprock/coordinator/internal/protocol"
)

// Hub fans out match events to subscribed WebSocket connections. A single
// hub serves all matches; subscribers are keyed by match id.
type Hub struct {
	mu   sync.Mutex
	subs map[string]map[*subscription]struct{}
}

type subscription struct {
	conn      *websocket.Conn
	peerID    string
	done      chan struct{}
	closeOnce sync.Once
}

func NewHub() *Hub {
	return &Hub{subs: make(map[string]map[*subscription]struct{})}
}

// Subscribe registers a connection for a match's event stream. The caller
// must block on the returned channel and read from conn until it closes.
func (h *Hub) Subscribe(matchID, peerID string, conn *websocket.Conn) <-chan struct{} {
	sub := &subscription{conn: conn, peerID: peerID, done: make(chan struct{})}
	h.mu.Lock()
	if h.subs[matchID] == nil {
		h.subs[matchID] = make(map[*subscription]struct{})
	}
	h.subs[matchID][sub] = struct{}{}
	h.mu.Unlock()
	return sub.done
}

// Unsubscribe removes the subscription and closes its done channel.
func (h *Hub) Unsubscribe(matchID string, conn *websocket.Conn) {
	h.mu.Lock()
	for sub := range h.subs[matchID] {
		if sub.conn == conn {
			delete(h.subs[matchID], sub)
			sub.closeOnce.Do(func() { close(sub.done) })
		}
	}
	if len(h.subs[matchID]) == 0 {
		delete(h.subs, matchID)
	}
	h.mu.Unlock()
}

// Publish sends an event to all subscribers of a match. Delivery is
// best-effort: a slow or dead subscriber is dropped after a write timeout.
func (h *Hub) Publish(matchID string, ev protocol.Event) {
	payload, err := json.Marshal(ev)
	if err != nil {
		slog.Error("event marshal failed", "err", err)
		return
	}

	h.mu.Lock()
	targets := make([]*subscription, 0, len(h.subs[matchID]))
	for sub := range h.subs[matchID] {
		targets = append(targets, sub)
	}
	h.mu.Unlock()

	for _, sub := range targets {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := sub.conn.Write(ctx, websocket.MessageText, payload)
		cancel()
		if err != nil {
			slog.Debug("event write failed; dropping subscriber", "err", err)
			h.mu.Lock()
			delete(h.subs[matchID], sub)
			sub.closeOnce.Do(func() { close(sub.done) })
			h.mu.Unlock()
		}
	}
}
