-- Coordinator schema, version 1.
-- Every authz-relevant row carries MatchId / AuthorityEpoch / RosterRevision
-- so stale-epoch writes can be rejected at the SQL layer.

CREATE TABLE IF NOT EXISTS devices (
    device_id    TEXT PRIMARY KEY,
    public_key   BYTEA NOT NULL UNIQUE,      -- Ed25519 public key (32 bytes)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ
);

-- One-time server-issued nonces for Ed25519 challenge signing.
CREATE TABLE IF NOT EXISTS device_challenges (
    nonce       TEXT PRIMARY KEY,
    public_key  BYTEA NOT NULL,              -- key the nonce was minted for
    expires_at  TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS matches (
    match_id       TEXT PRIMARY KEY,
    join_code      TEXT NOT NULL UNIQUE,     -- short-lived join token
    join_code_exp  TIMESTAMPTZ NOT NULL,
    host_device_id TEXT,
    authority_epoch BIGINT NOT NULL DEFAULT 0,
    roster_revision BIGINT NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS match_peers (
    match_id   TEXT NOT NULL REFERENCES matches(match_id) ON DELETE CASCADE,
    peer_id    TEXT NOT NULL,
    device_id  TEXT NOT NULL REFERENCES devices(device_id),
    role       TEXT NOT NULL,                -- 'Host' | 'Client' | 'Observer'
    joined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, peer_id)
);

CREATE TABLE IF NOT EXISTS role_assignments (
    match_id      TEXT NOT NULL,
    authority_epoch BIGINT NOT NULL,
    peer_id       TEXT NOT NULL,
    role          TEXT NOT NULL,
    assigned_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, authority_epoch, peer_id)
);

CREATE TABLE IF NOT EXISTS leases (
    match_id        TEXT PRIMARY KEY,
    host_device_id  TEXT NOT NULL,
    host_peer_id    TEXT NOT NULL,
    authority_epoch BIGINT NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (authority_epoch >= 0)
);

CREATE TABLE IF NOT EXISTS endpoint_candidates (
    match_id      TEXT NOT NULL,
    peer_id       TEXT NOT NULL,
    candidate_id  TEXT NOT NULL,
    authority_epoch BIGINT NOT NULL,
    roster_revision BIGINT NOT NULL,
    transport     TEXT NOT NULL,
    candidate_type TEXT NOT NULL,
    address       TEXT NOT NULL,
    port          INT NOT NULL,
    priority      BIGINT NOT NULL,
    foundation    TEXT NOT NULL,
    relay_allocation_id TEXT,
    expires_at    TIMESTAMPTZ NOT NULL,
    signature     TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, peer_id, candidate_id)
);

CREATE TABLE IF NOT EXISTS checkpoints (
    match_id             TEXT NOT NULL,
    authority_epoch      BIGINT NOT NULL,
    roster_revision      BIGINT NOT NULL,
    snapshot_revision    BIGINT NOT NULL,
    object_key           TEXT NOT NULL,
    payload_hash         TEXT NOT NULL,
    payload_size         BIGINT NOT NULL,
    save_format_version  INT NOT NULL,
    snapshot_schema_version INT NOT NULL,
    committed_by         TEXT NOT NULL,
    committed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at           TIMESTAMPTZ NOT NULL DEFAULT now() + interval '24 hours',
    PRIMARY KEY (match_id, authority_epoch, snapshot_revision)
);

CREATE TABLE IF NOT EXISTS relay_allocations (
    allocation_id  TEXT PRIMARY KEY,
    match_id       TEXT NOT NULL,
    peer_id        TEXT NOT NULL,
    token          TEXT NOT NULL,
    relay_address  TEXT NOT NULL,
    relay_port     INT NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_events (
    id          BIGSERIAL PRIMARY KEY,
    match_id    TEXT,
    peer_id     TEXT,
    event_type  TEXT NOT NULL,
    details     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_candidates_match_epoch
    ON endpoint_candidates (match_id, authority_epoch);
CREATE INDEX IF NOT EXISTS idx_checkpoints_match
    ON checkpoints (match_id, authority_epoch);
CREATE INDEX IF NOT EXISTS idx_leases_expiry
    ON leases (expires_at);
