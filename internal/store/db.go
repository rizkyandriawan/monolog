package store

import (
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"
)

// DB wraps BadgerDB
type DB struct {
	db *badger.DB
}

// Open opens or creates a BadgerDB at the given path
func Open(dataDir string) (*DB, error) {
	// Ensure data directory exists
	dbPath := filepath.Join(dataDir, "badger")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, err
	}

	opts := badger.DefaultOptions(dbPath)
	opts.Logger = nil // Disable badger's default logger

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &DB{db: db}, nil
}

// Close closes the database
func (d *DB) Close() error {
	return d.db.Close()
}

// Badger returns the underlying BadgerDB instance
func (d *DB) Badger() *badger.DB {
	return d.db
}

// RunGC runs garbage collection on the database
func (d *DB) RunGC() error {
	return d.db.RunValueLogGC(0.5)
}
