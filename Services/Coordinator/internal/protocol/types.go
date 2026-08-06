// Package protocol defines the versioned contract shared between the UE
// client and the Go Coordinator (REST + WebSocket).
package protocol

import "time"

const (
	// ProtocolVersion is bumped on wire-incompatible changes (endpoints,
	// message shapes). SchemaVersion is bumped on incompatible state
	// migrations. UE and Go both pin these; mismatches are rejected.
	ProtocolVersion = 1
	SchemaVersion   = 1
)

type CandidateType string

const (
	CandidateTypeHost            CandidateType = "Host"
	CandidateTypeServerReflexive CandidateType = "ServerReflexive"
	CandidateTypeRelayed         CandidateType = "Relayed"
)

type Transport string

const (
	TransportUDP   Transport = "UDP"
	TransportTCP   Transport = "TCP"
	TransportRelay Transport = "Relay"
)

type ConnectivityState string

const (
	ConnectivityGathering    ConnectivityState = "Gathering"
	ConnectivityChecking     ConnectivityState = "Checking"
	ConnectivityDirect       ConnectivityState = "DirectConnected"
	ConnectivityRelayed      ConnectivityState = "Relayed"
	ConnectivityFailed       ConnectivityState = "Failed"
	ConnectivityExpired      ConnectivityState = "Expired"
)

// EndpointCandidate is the wire form of one candidate address. Every
// candidate, lease, checkpoint and migration event carries MatchId,
// AuthorityEpoch, RosterRevision and PeerId so that stale epochs can never
// overwrite newer ones.
type EndpointCandidate struct {
	MatchId          string         `json:"matchId"`
	AuthorityEpoch   uint64         `json:"authorityEpoch"`
	RosterRevision   uint64         `json:"rosterRevision"`
	PeerId           string         `json:"peerId"`
	CandidateId      string         `json:"candidateId"`
	Transport        Transport      `json:"transport"`
	CandidateType    CandidateType  `json:"candidateType"`
	Address          string         `json:"address"`
	Port             int            `json:"port"`
	Priority         uint32         `json:"priority"`
	Foundation       string         `json:"foundation"`
	RelayAllocationId string        `json:"relayAllocationId,omitempty"`
	ExpiresAt        time.Time      `json:"expiresAt"`
	Signature        string         `json:"signature"`
}

type EventType string

const (
	EventRoleAssignment       EventType = "RoleAssignment"
	EventLeaseGranted         EventType = "LeaseGranted"
	EventLeaseRenewed         EventType = "LeaseRenewed"
	EventHostEndpointPublished EventType = "HostEndpointPublished"
	EventCandidateUpdated     EventType = "CandidateUpdated"
	EventCheckpointCommitted  EventType = "CheckpointCommitted"
	EventMigrationStarted     EventType = "MigrationStarted"
	EventMigrationClaimed     EventType = "MigrationClaimed"
	EventMigrationCommitted   EventType = "MigrationCommitted"
	EventMigrationFailed      EventType = "MigrationFailed"
)

type Event struct {
	Type    EventType       `json:"type"`
	MatchId string          `json:"matchId"`
	Epoch   uint64          `json:"epoch"`
	PeerId  string          `json:"peerId,omitempty"`
	Payload map[string]any  `json:"payload,omitempty"`
}
