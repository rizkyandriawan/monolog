package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/rizkyandriawan/monolog/internal/config"
	"github.com/rizkyandriawan/monolog/internal/engine"
	"github.com/rizkyandriawan/monolog/internal/server"
	"github.com/rizkyandriawan/monolog/internal/store"
)

// acquireDataLock acquires an exclusive lock on the data directory
// Returns the lock file handle (must be kept open) or error if already locked
func acquireDataLock(dataDir string) (*os.File, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	lockPath := filepath.Join(dataDir, ".lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}

	// Try to acquire exclusive lock (non-blocking)
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another monolog instance is using data directory %s", dataDir)
	}

	return f, nil
}

var (
	version = "0.2.0"
	commit  = "none"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "version":
		fmt.Printf("monolog %s (%s)\n", version, commit)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`monolog - Kafka-shaped single-node message broker

Usage:
  monolog <command> [options]

Commands:
  serve     Start the Monolog server
  version   Print version information
  help      Print this help message

Run 'monolog serve --help' for serve options.`)
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)

	configFile := fs.String("config", "", "Path to config file (YAML)")
	kafkaAddr := fs.String("kafka-addr", ":9092", "Kafka protocol listen address")
	httpAddr := fs.String("http-addr", ":8080", "HTTP API listen address")
	dataDir := fs.String("data-dir", "./data", "Data directory for storage")
	logLevel := fs.String("log-level", "info", "Log level (debug, info, warn, error)")
	backend := fs.String("storage", "", "Storage backend: sqlite or sqlite:memory")

	fs.Parse(args)

	// Load config with precedence: flags > env > file > defaults
	cfg, err := config.Load(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Override with flags if provided
	if *kafkaAddr != ":9092" || cfg.Server.KafkaAddr == "" {
		cfg.Server.KafkaAddr = *kafkaAddr
	}
	if *httpAddr != ":8080" || cfg.Server.HTTPAddr == "" {
		cfg.Server.HTTPAddr = *httpAddr
	}
	if *dataDir != "./data" || cfg.Storage.DataDir == "" {
		cfg.Storage.DataDir = *dataDir
	}
	if *logLevel != "info" || cfg.Logging.Level == "" {
		cfg.Logging.Level = *logLevel
	}
	if *backend != "" {
		cfg.Storage.Backend = *backend
	}

	// Acquire data directory lock (except for memory mode)
	var lockFile *os.File
	if cfg.Storage.Backend != "sqlite:memory" {
		var err error
		lockFile, err = acquireDataLock(cfg.Storage.DataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to acquire data lock: %v\n", err)
			os.Exit(1)
		}
		defer lockFile.Close()
	}

	// Initialize store based on backend
	var topicStore store.TopicStoreInterface
	var groupStore store.GroupStoreInterface
	var closer func() error

	storageBackend := cfg.Storage.Backend
	switch {
	case storageBackend == "sqlite" || storageBackend == "sqlite:disk":
		fmt.Printf("Using SQLite storage backend (disk)\n")
		sqliteDB, err := store.OpenSQLite(cfg.Storage.DataDir, "disk")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open sqlite store: %v\n", err)
			os.Exit(1)
		}
		closer = sqliteDB.Close
		topicStore = store.NewSQLiteTopicStore(sqliteDB)
		groupStore = store.NewSQLiteGroupStore(sqliteDB)

	case storageBackend == "sqlite:memory":
		fmt.Printf("Using SQLite storage backend (in-memory)\n")
		sqliteDB, err := store.OpenSQLite(cfg.Storage.DataDir, "memory")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open sqlite store: %v\n", err)
			os.Exit(1)
		}
		closer = sqliteDB.Close
		topicStore = store.NewSQLiteTopicStore(sqliteDB)
		groupStore = store.NewSQLiteGroupStore(sqliteDB)

	default:
		fmt.Fprintf(os.Stderr, "unknown storage backend: %s (use 'sqlite' or 'sqlite:memory')\n", storageBackend)
		os.Exit(1)
	}
	defer closer()

	// Initialize engine
	eng := engine.New(cfg, topicStore, groupStore)
	eng.Start()
	defer eng.Stop()

	// Start servers
	kafkaSrv := server.NewKafkaServer(cfg, eng)
	httpSrv := server.NewHTTPServer(cfg, eng)

	go func() {
		fmt.Printf("Kafka server listening on %s\n", cfg.Server.KafkaAddr)
		if err := kafkaSrv.ListenAndServe(); err != nil {
			fmt.Fprintf(os.Stderr, "kafka server error: %v\n", err)
		}
	}()

	go func() {
		fmt.Printf("HTTP server listening on %s\n", cfg.Server.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil {
			fmt.Fprintf(os.Stderr, "http server error: %v\n", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\nShutting down...")
	kafkaSrv.Close()
	httpSrv.Close()
}
