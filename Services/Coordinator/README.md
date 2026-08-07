# LetsDeepRock Coordinator

Central control-plane service for LetsDeepRock: device authentication, match
lifecycle, host lease / AuthorityEpoch fencing, endpoint candidate exchange,
checkpoint metadata and host migration coordination.

Gameplay authority stays with the UE host listen server; this service never
runs an Unreal world.

## Components

| Mode | Purpose |
| --- | --- |
| `coordinator` (default) | REST + WebSocket control plane, PostgreSQL + MinIO |
| `coordinator -stun` | Standalone STUN Binding service (UDP :3478) |

## Architecture

```
UE Host <-- Gameplay (UDP direct) --> UE Client
   |                                    |
   +-- control (HTTPS/WSS) --> Coordinator --> PostgreSQL / MinIO
```

- Device auth: Ed25519 challenge/signature, short-lived JWT access tokens.
- Match: join codes, role assignment, roster.
- Lease: single active host per match, monotonic AuthorityEpoch, successor
  claim only after expiry, stale-epoch writes rejected.
- Candidates: peer endpoint exchange with MatchId/Epoch/PeerId fencing.
- Checkpoint: host uploads SaveGame blob via MinIO presigned PUT; the
  coordinator verifies hash+size before atomically committing metadata.

## Development

Requirements: Go 1.26+, PostgreSQL (localhost), MinIO/Silo (localhost).

```bash
# PostgreSQL: create role + database
psql -U postgres -c "CREATE USER letsdeeprock WITH PASSWORD 'letsdeeprock123';"
psql -U postgres -c "CREATE DATABASE letsdeeprock OWNER letsdeeprock;"

# MinIO/Silo on 127.0.0.1:9000 with MINIO_ROOT_USER/PASSWORD matching .env.

# Configuration
cp .env.example .env   # adjust credentials; .env is gitignored

# Run (loads .env automatically; systemd EnvironmentFile covers production)
go run ./cmd/coordinator            # control plane (:8443) + STUN (:3478)
go run ./cmd/coordinator -stun      # STUN only

# Tests (need PostgreSQL + MinIO running)
go test ./...
```

Configuration is read from environment variables (see `.env.example`):
`COORDINATOR_HTTP_ADDR`, `COORDINATOR_STUN_ADDR`, `DATABASE_URL`,
`MINIO_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY`, `JWT_SECRET`,
`TLS_CERT_FILE`, `TLS_KEY_FILE`.

## API summary

```
POST /v1/devices/challenge          # nonce for Ed25519 signing
POST /v1/devices/register           # register device public key
POST /v1/devices/session            # exchange signed nonce for JWT
POST /v1/matches                    # create match (creator = Host)
POST /v1/matches/{id}/join          # join by code
POST /v1/matches/{id}/leave
GET  /v1/matches/{id}
GET  /v1/matches/{id}/events        # WebSocket event stream
POST /v1/matches/{id}/lease/claim   # establish AuthorityEpoch N+1
POST /v1/matches/{id}/lease/heartbeat
POST /v1/matches/{id}/lease/release
POST /v1/matches/{id}/peers/{pid}/candidates
GET  /v1/matches/{id}/peers/{pid}/candidates
POST /v1/matches/{id}/connectivity/check-result
POST /v1/matches/{id}/connectivity/nominate   # host publishes endpoint
POST /v1/matches/{id}/checkpoints/begin       # presigned PUT URL
POST /v1/matches/{id}/checkpoints/commit      # verify + commit metadata
GET  /v1/matches/{id}/checkpoints/latest
```

## Deployment

Linux native binary + systemd; see `deploy/` for unit files and the env
template. PostgreSQL and MinIO listen on localhost only.

## Phase 2 (not implemented)

Custom UE transport/NetDriver for relay paths, TURN allocation and relay
candidate support. `internal/relay/` is reserved.
