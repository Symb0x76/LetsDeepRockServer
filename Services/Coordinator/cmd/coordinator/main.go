// Coordinator is the central control-plane service for LetsDeepRock:
// device auth, match lifecycle, host lease/epoch fencing, endpoint candidate
// exchange, checkpoint metadata and host migration coordination.
//
// Gameplay authority stays with the UE host listen server; this service
// never runs an Unreal world.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/letsdeeprock/coordinator/internal/auth"
	"github.com/letsdeeprock/coordinator/internal/candidate"
	"github.com/letsdeeprock/coordinator/internal/checkpoint"
	"github.com/letsdeeprock/coordinator/internal/config"
	"github.com/letsdeeprock/coordinator/internal/httpapi"
	"github.com/letsdeeprock/coordinator/internal/lease"
	"github.com/letsdeeprock/coordinator/internal/match"
	"github.com/letsdeeprock/coordinator/internal/relay"
	"github.com/letsdeeprock/coordinator/internal/store"
	"github.com/letsdeeprock/coordinator/internal/stun"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/joho/godotenv"
)

func main() {
	// -stun runs the STUN listener only, as a standalone systemd unit;
	// it needs no database or MinIO.
	stunOnly := flag.Bool("stun", false, "run STUN service only")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	// Load .env for local development; systemd EnvironmentFile covers
	// production. Existing env vars win (godotenv never overwrites).
	_ = godotenv.Load()

	cfg := config.FromEnv()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *stunOnly {
		runSTUNOnly(ctx, cfg)
		return
	}
	runCoordinator(ctx, cfg, stop)
}

func runSTUNOnly(ctx context.Context, cfg config.Config) {
	srv := stun.New(stun.Config{Addr: cfg.STUNAddr})
	if err := srv.Start(ctx); err != nil {
		slog.Error("stun start failed", "err", err)
		os.Exit(1)
	}
	<-ctx.Done()
	_ = srv.Close()
}

func runCoordinator(ctx context.Context, cfg config.Config, stop context.CancelFunc) {

	st, err := store.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database connect failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		slog.Error("migration failed", "err", err)
		os.Exit(1)
	}
	slog.Info("migrations applied", "schema_version", 1)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	authSvc := auth.New(st.Pool, cfg.JWTSecret)
	deviceHandler := &httpapi.DeviceAuthHandler{Auth: authSvc}
	mux.HandleFunc("POST /v1/devices/challenge", deviceHandler.Challenge)
	mux.HandleFunc("POST /v1/devices/register", deviceHandler.Register)
	mux.HandleFunc("POST /v1/devices/session", deviceHandler.Session)

	matchHub := match.NewHub()
	matchSvc := match.NewService(st.Pool)
	matchHandler := &httpapi.MatchHandler{Service: matchSvc, Hub: matchHub}
	mux.HandleFunc("POST /v1/matches", httpapi.RequireDevice(authSvc, matchHandler.Create))
	mux.HandleFunc("POST /v1/matches/{matchId}/join", httpapi.RequireDevice(authSvc, matchHandler.Join))
	mux.HandleFunc("POST /v1/matches/join", httpapi.RequireDevice(authSvc, matchHandler.Join))
	mux.HandleFunc("POST /v1/matches/{matchId}/leave", httpapi.RequireDevice(authSvc, matchHandler.Leave))
	mux.HandleFunc("GET /v1/matches/{matchId}", httpapi.RequireDevice(authSvc, matchHandler.Get))
	mux.HandleFunc("GET /v1/matches/{matchId}/events", httpapi.RequireDevice(authSvc, matchHandler.Events))

	leaseSvc := lease.NewService(st.Pool, cfg.LeaseTTL)
	leaseHandler := &httpapi.LeaseHandler{Service: leaseSvc, Hub: matchHub}
	mux.HandleFunc("POST /v1/matches/{matchId}/lease/claim", httpapi.RequireDevice(authSvc, leaseHandler.Claim))
	mux.HandleFunc("POST /v1/matches/{matchId}/lease/heartbeat", httpapi.RequireDevice(authSvc, leaseHandler.Heartbeat))
	mux.HandleFunc("POST /v1/matches/{matchId}/lease/release", httpapi.RequireDevice(authSvc, leaseHandler.Release))

	candSvc := candidate.NewService(st.Pool)
	candHandler := &httpapi.CandidateHandler{Service: candSvc, Leases: leaseSvc, Hub: matchHub}
	mux.HandleFunc("POST /v1/matches/{matchId}/peers/{peerId}/candidates", httpapi.RequireDevice(authSvc, candHandler.AddCandidate))
	mux.HandleFunc("GET /v1/matches/{matchId}/peers/{peerId}/candidates", httpapi.RequireDevice(authSvc, candHandler.ListCandidates))
	mux.HandleFunc("POST /v1/matches/{matchId}/connectivity/check-result", httpapi.RequireDevice(authSvc, candHandler.CheckResult))
	mux.HandleFunc("POST /v1/matches/{matchId}/connectivity/nominate", httpapi.RequireDevice(authSvc, candHandler.Nominate))

	stunCfg := stun.Config{Addr: cfg.STUNAddr}
	if cfg.RelaySecret != "" {
		stunCfg.SharedSecret = cfg.RelaySecret
		stunCfg.RelayAddr = cfg.RelayAddr
		stunCfg.RelayMinPort = cfg.RelayMinPort
		stunCfg.RelayMaxPort = cfg.RelayMaxPort
		stunCfg.MaxAllocsPerUser = cfg.RelayMaxAllocsUser
	}
	stunSrv := stun.New(stunCfg)
	if err := stunSrv.Start(ctx); err != nil {
		slog.Error("stun start failed", "err", err)
		os.Exit(1)
	}
	defer stunSrv.Close()

	if cfg.RelaySecret != "" {
		relayStore := relay.NewStore(st.Pool)
		relaySvc := relay.NewService(relayStore, cfg.RelaySecret, cfg.RelayAddr, cfg.RelayTTL)
		go relaySvc.RunPruneLoop(ctx)
		// persist allocations as the TURN server creates them, so relayed
		// candidates can reference relay_allocation_id
		stunSrv.SetOnAllocation(func(username string, clientAddr, relayAddr net.Addr) {
			relaySvc.RecordAllocation(ctx, username, clientAddr, relayAddr)
		})
		relayHandler := &httpapi.RelayHandler{Service: relaySvc}
		mux.HandleFunc("POST /v1/matches/{matchId}/relay/credentials", httpapi.RequireDevice(authSvc, relayHandler.Credentials))
		slog.Info("turn relay enabled", "relay_addr", cfg.RelayAddr)
	}

	minioClient, err := minio.New(cfg.MinIOEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIOAccessKey, cfg.MinIOSecretKey, ""),
		Secure: false,
	})
	if err != nil {
		slog.Error("minio client failed", "err", err)
		os.Exit(1)
	}
	ckptSvc := checkpoint.NewService(st.Pool, minioClient, cfg.MinIOBucket)
	if err := ckptSvc.EnsureBucket(ctx); err != nil {
		slog.Error("ensure minio bucket failed", "err", err)
		os.Exit(1)
	}
	go ckptSvc.RunRetentionLoop(ctx)
	ckptHandler := &httpapi.CheckpointHandler{Service: ckptSvc, Hub: matchHub}
	mux.HandleFunc("POST /v1/matches/{matchId}/checkpoints/begin", httpapi.RequireDevice(authSvc, ckptHandler.Begin))
	mux.HandleFunc("POST /v1/matches/{matchId}/checkpoints/commit", httpapi.RequireDevice(authSvc, ckptHandler.Commit))
	mux.HandleFunc("GET /v1/matches/{matchId}/checkpoints/latest", httpapi.RequireDevice(authSvc, ckptHandler.Latest))

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("coordinator http listening", "addr", cfg.HTTPAddr)
		var err error
		if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
			err = srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
