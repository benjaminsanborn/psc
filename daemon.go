package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Daemon watches a migrations repo directory and manages state.
type Daemon struct {
	RepoPath       string
	DefaultService string
	StateDB        *sql.DB
	Executor       *Executor

	mu         sync.Mutex
	migrations map[string]*Migration // parsed migrations by name
	mtimes     map[string]time.Time  // file mtimes
	records    []MigrationRecord     // cached DB records
	lastPoll   time.Time
	errLog     []string
}

// NewDaemon creates a new Daemon.
func NewDaemon(repoPath, defaultService string) (*Daemon, error) {
	if defaultService == "" {
		return nil, fmt.Errorf("--service is required (default pg_service.conf service name)")
	}

	stateDB, err := ConnectService(defaultService)
	if err != nil {
		return nil, fmt.Errorf("connecting to state DB (%s): %w", defaultService, err)
	}

	if err := EnsureMigrationsTable(stateDB); err != nil {
		return nil, fmt.Errorf("creating migrations table: %w", err)
	}

	d := &Daemon{
		RepoPath:       repoPath,
		DefaultService: defaultService,
		StateDB:        stateDB,
		migrations:     make(map[string]*Migration),
		mtimes:         make(map[string]time.Time),
	}
	d.Executor = NewExecutor(stateDB, defaultService)
	return d, nil
}

// Poll scans the repo directory for new/changed .sql files and refreshes DB records.
func (d *Daemon) Poll() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Scan directory
	entries, err := os.ReadDir(d.RepoPath)
	if err != nil {
		return fmt.Errorf("reading repo dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		path := filepath.Join(d.RepoPath, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}
		mtime := info.ModTime()

		if prev, ok := d.mtimes[path]; ok && !mtime.After(prev) {
			continue
		}
		d.mtimes[path] = mtime

		m, err := ParseMigrationFile(path)
		if err != nil {
			d.errLog = append(d.errLog, fmt.Sprintf("parse %s: %v", entry.Name(), err))
			continue
		}

		if m.Service == "" {
			m.Service = d.DefaultService
		}

		d.migrations[m.Name] = m
		if err := UpsertMigration(d.StateDB, m); err != nil {
			d.errLog = append(d.errLog, fmt.Sprintf("upsert %s: %v", m.Name, err))
		}
	}

	// Refresh records from DB
	records, err := LoadMigrations(d.StateDB)
	if err != nil {
		return err
	}
	d.records = records
	d.lastPoll = time.Now()
	return nil
}

// Records returns the current migration records (thread-safe copy).
func (d *Daemon) Records() []MigrationRecord {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]MigrationRecord, len(d.records))
	copy(out, d.records)
	return out
}

// GetMigration returns the parsed migration by name.
func (d *Daemon) GetMigration(name string) *Migration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.migrations[name]
}

// RunMigration starts a migration in the background.
func (d *Daemon) RunMigration(name string) error {
	m := d.GetMigration(name)
	if m == nil {
		return fmt.Errorf("migration %q not found", name)
	}
	if d.Executor.IsRunning(name) {
		return fmt.Errorf("migration %q is already running", name)
	}

	record, err := GetMigrationByName(d.StateDB, name)
	if err != nil {
		return err
	}
	if record.Status == "completed" {
		return fmt.Errorf("migration %q is already completed", name)
	}
	if record.Status == "running" {
		return fmt.Errorf("migration %q is already running", name)
	}

	go func() {
		if err := d.Executor.Run(m, record); err != nil {
			d.mu.Lock()
			d.errLog = append(d.errLog, fmt.Sprintf("run %s: %v", name, err))
			d.mu.Unlock()
		}
	}()
	return nil
}

// CancelMigration cancels a running migration.
func (d *Daemon) CancelMigration(name string) error {
	if !d.Executor.IsRunning(name) {
		return fmt.Errorf("migration %q is not running", name)
	}
	d.Executor.Cancel(name)
	return nil
}

// PopErrors returns and clears accumulated error messages.
func (d *Daemon) PopErrors() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	errs := d.errLog
	d.errLog = nil
	return errs
}
