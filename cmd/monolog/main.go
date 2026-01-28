package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rizkyandriawan/monolog/internal/config"
	"github.com/rizkyandriawan/monolog/internal/engine"
	"github.com/rizkyandriawan/monolog/internal/server"
	"github.com/rizkyandriawan/monolog/internal/store"
)

var (
	version = "dev"
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
	backend := fs.String("storage", "", "Storage backend: sqlite or badger")

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

	// Initialize store based on backend
	var topicStore store.TopicStoreInterface
	var groupStore store.GroupStoreInterface
	var closer func() error

	switch cfg.Storage.Backend {
	case "sqlite":
		fmt.Printf("Using SQLite storage backend\n")
		sqliteDB, err := store.OpenSQLite(cfg.Storage.DataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open sqlite store: %v\n", err)
			os.Exit(1)
		}
		closer = sqliteDB.Close
		topicStore = store.NewSQLiteTopicStore(sqliteDB)
		groupStore = store.NewSQLiteGroupStore(sqliteDB)

	case "badger":
		fmt.Printf("Using BadgerDB storage backend\n")
		db, err := store.Open(cfg.Storage.DataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open badger store: %v\n", err)
			os.Exit(1)
		}
		closer = db.Close
		topicStore = store.NewTopicStore(db)
		groupStore = store.NewGroupStore(db)

	default:
		fmt.Fprintf(os.Stderr, "unknown storage backend: %s (use 'sqlite' or 'badger')\n", cfg.Storage.Backend)
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
