package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// BackendConfig describes a single S3-compatible backend provider.
type BackendConfig struct {
	Name      string `yaml:"name"`
	Endpoint  string `yaml:"endpoint"`
	Region    string `yaml:"region"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Bucket    string `yaml:"bucket"`
	// Weight controls how often this backend is selected for writes (1–100).
	// Backends with higher weight receive proportionally more write traffic.
	Weight      int               `yaml:"weight"`
	HealthCheck HealthCheckConfig `yaml:"health_check"`
	// PathStyle forces path-style URLs (endpoint/bucket/key) instead of
	// virtual-hosted style (bucket.endpoint/key). Needed for MinIO and
	// some other S3-compatible providers.
	PathStyle bool `yaml:"path_style"`
}

// HealthCheckConfig defines how to probe a backend's availability.
type HealthCheckConfig struct {
	// Object key inside the bucket used for HEAD health-check probes.
	// The router creates a tiny canary object automatically on startup if it
	// does not exist yet.
	ObjectKey string        `yaml:"object_key"`
	Interval  time.Duration `yaml:"interval"`
	Timeout   time.Duration `yaml:"timeout"`
}

// RoutingConfig defines the routing policies for reads and writes.
type RoutingConfig struct {
	// ReadPolicy controls how a backend is selected for read operations.
	//   failover   — try backends in priority order; fall back on 5xx / timeout
	//   round-robin — distribute reads evenly across healthy backends
	//   latency     — always pick the backend with the lowest measured latency
	ReadPolicy string `yaml:"read_policy"`

	// WritePolicy controls where writes (PUT/DELETE/Multipart) are sent.
	//   weight_async — pick one backend by weight, replicate to others asynchronously
	//   primary_only — write only to the first (highest-weight) backend
	WritePolicy string `yaml:"write_policy"`

	// ReadMode determines how GET responses are served to clients.
	//   proxy    — stream the object body through the router (default)
	//   redirect — generate a Pre-signed URL for the chosen backend and return
	//              HTTP 302 so the client fetches directly from S3. Eliminates
	//              egress through the router and enables future geo-routing.
	ReadMode string `yaml:"read_mode"`

	// RedirectTTL is the lifetime of the generated Pre-signed URL when
	// read_mode is "redirect".
	RedirectTTL time.Duration `yaml:"redirect_ttl"`

	// SyncInterval controls the periodic background bucket reconciliation.
	// E.g. "1h" or "30m". If 0, periodic background sync is disabled.
	SyncInterval time.Duration `yaml:"sync_interval"`

	// CleanupInterval controls the periodic background deletion of orphan objects.
	// E.g. "12h" or "24h". If 0, periodic background orphan cleanup is disabled.
	CleanupInterval time.Duration `yaml:"cleanup_interval"`
}

// QueueConfig controls the persistent replication queue backed by BoltDB.
type QueueConfig struct {
	// DBPath is the filesystem path to the BoltDB database file.
	DBPath string `yaml:"db_path"`
	// Workers is the number of concurrent goroutines draining the queue.
	Workers int `yaml:"workers"`
	// RetryLimit is the maximum number of times a failed replication task
	// is retried before being moved to a dead-letter bucket.
	RetryLimit int `yaml:"retry_limit"`
	// RetryBackoff is the base duration between retry attempts (exponential).
	RetryBackoff time.Duration `yaml:"retry_backoff"`
}

// ServerConfig holds the HTTP listener and authentication settings.
type ServerConfig struct {
	Port      int    `yaml:"port"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
	Domain    string `yaml:"domain"`
}

// Config is the top-level configuration structure.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Backends []BackendConfig `yaml:"backends"`
	Routing  RoutingConfig  `yaml:"routing"`
	Queue    QueueConfig    `yaml:"queue"`
}

// LoadConfig reads and validates a YAML configuration file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: cannot read file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: cannot parse YAML: %w", err)
	}

	// ── Apply defaults ────────────────────────────────────────────────────
	if cfg.Server.Port == 0 {
		cfg.Server.Port = 8080
	}
	if cfg.Routing.ReadPolicy == "" {
		cfg.Routing.ReadPolicy = "failover"
	}
	if cfg.Routing.WritePolicy == "" {
		cfg.Routing.WritePolicy = "weight_async"
	}
	if cfg.Routing.ReadMode == "" {
		cfg.Routing.ReadMode = "proxy"
	}
	if cfg.Routing.RedirectTTL == 0 {
		cfg.Routing.RedirectTTL = 15 * time.Minute
	}
	if cfg.Queue.DBPath == "" {
		cfg.Queue.DBPath = "queue.db"
	}
	if cfg.Queue.Workers == 0 {
		cfg.Queue.Workers = 4
	}
	if cfg.Queue.RetryLimit == 0 {
		cfg.Queue.RetryLimit = 5
	}
	if cfg.Queue.RetryBackoff == 0 {
		cfg.Queue.RetryBackoff = 5 * time.Second
	}
	for i := range cfg.Backends {
		b := &cfg.Backends[i]
		if b.Weight == 0 {
			b.Weight = 10
		}
		if b.HealthCheck.ObjectKey == "" {
			b.HealthCheck.ObjectKey = ".s3rudder-health"
		}
		if b.HealthCheck.Interval == 0 {
			b.HealthCheck.Interval = 15 * time.Second
		}
		if b.HealthCheck.Timeout == 0 {
			b.HealthCheck.Timeout = 5 * time.Second
		}
	}

	// ── Validate ──────────────────────────────────────────────────────────
	if len(cfg.Backends) == 0 {
		return nil, fmt.Errorf("config: at least one backend must be configured")
	}
	if cfg.Server.AccessKey == "" || cfg.Server.SecretKey == "" {
		return nil, fmt.Errorf("config: server.access_key and server.secret_key are required")
	}
	for _, b := range cfg.Backends {
		if b.Name == "" {
			return nil, fmt.Errorf("config: backend is missing a name")
		}
		if b.Endpoint == "" || b.Bucket == "" {
			return nil, fmt.Errorf("config: backend %q is missing endpoint or bucket", b.Name)
		}
	}

	return &cfg, nil
}
