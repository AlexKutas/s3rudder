package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// ── CLI flags ─────────────────────────────────────────────────────────
	cfgPath := flag.String("config", "config.yaml", "path to configuration file")
	syncCmd := flag.Bool("sync", false, "run a full sync between all backends and exit")
	deleteOrphans := flag.Bool("delete-orphans", false, "when syncing, remove objects in dst that don't exist in src")
	flag.Parse()

	// ── Load configuration ────────────────────────────────────────────────
	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("[main] config error: %v", err)
	}

	// ── Build backends ────────────────────────────────────────────────────
	backends, err := BuildBackends(cfg.Backends)
	if err != nil {
		log.Fatalf("[main] cannot build backends: %v", err)
	}

	// ── Open persistent queue ─────────────────────────────────────────────
	queue, err := OpenQueue(cfg.Queue)
	if err != nil {
		log.Fatalf("[main] cannot open queue: %v", err)
	}
	defer queue.Close()

	// ── One-shot sync mode ────────────────────────────────────────────────
	if *syncCmd {
		runSync(backends, *deleteOrphans)
		return
	}

	// ── Serve mode ────────────────────────────────────────────────────────
	router := NewRouter(cfg, backends, queue)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	router.Start(ctx)
	defer router.Stop()

	addr := fmt.Sprintf(":%d", cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 10 * time.Minute, // generous timeout for large uploads
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		log.Println("[main] shutting down…")
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutCancel()
		_ = srv.Shutdown(shutCtx)
	}()

	log.Printf("[main] S3Rudder listening on %s (read_mode=%s, write_policy=%s)",
		addr, cfg.Routing.ReadMode, cfg.Routing.WritePolicy)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[main] server error: %v", err)
	}
	log.Println("[main] stopped")
}

// runSync performs a full pairwise sync from the first healthy backend to
// all others and prints a summary.
func runSync(backends []*Backend, deleteOrphans bool) {
	if len(backends) < 2 {
		log.Println("[sync] nothing to sync: fewer than 2 backends configured")
		os.Exit(0)
	}

	src := backends[0]
	log.Printf("[sync] source: %s", src.Config.Name)

	ctx := context.Background()
	var totalCopied, totalFailed int

	for _, dst := range backends[1:] {
		log.Printf("[sync] syncing to %s …", dst.Config.Name)
		stats, err := SyncBackends(ctx, src, dst, deleteOrphans)
		if err != nil {
			log.Printf("[sync] ERROR %s: %v", dst.Config.Name, err)
			totalFailed++
			continue
		}
		totalCopied += stats.Copied
		totalFailed += stats.Failed
		fmt.Printf("  %-20s  compared=%-6d copied=%-6d deleted=%-6d failed=%d\n",
			dst.Config.Name, stats.Compared, stats.Copied, stats.Deleted, stats.Failed)
	}

	fmt.Printf("\nTotal: copied=%d failed=%d\n", totalCopied, totalFailed)
	if totalFailed > 0 {
		os.Exit(1)
	}
}
