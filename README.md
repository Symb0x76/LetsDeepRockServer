# LetsDeepRockServer

A dedicated server for our game LetsDeepRock.

## Repo layout

| Path | Description |
| --- | --- |
| `Services/Coordinator/` | Central control-plane service (Go): device auth, match lifecycle, host lease/epoch fencing, endpoint candidates, checkpoints |
| `docs/` | Design and implementation plans |

## Services

- **Coordinator** — HTTPS/WSS control plane + STUN (UDP). See
  [Services/Coordinator/README.md](Services/Coordinator/README.md) for
  setup, API summary and deployment.
- Gameplay authority stays with the UE host listen server; relay/UE client
  integration is planned for a later phase.

## Development

Requirements: Go 1.26+, PostgreSQL, MinIO/Silo (see Coordinator README).

```bash
# From Services/Coordinator/
cp .env.example .env   # adjust credentials
go run ./cmd/coordinator
go test ./...
```
