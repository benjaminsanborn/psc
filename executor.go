package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ExecutionState tracks a running migration for the TUI.
type ExecutionState struct {
	Name            string
	Cancel          context.CancelFunc
	StartedAt       time.Time
	TotalAffected   atomic.Int64
	LastCompletedID atomic.Int64
	MaxID           int64
	Rate            atomic.Int64 // rows/sec rolling estimate
}

// Executor runs migrations against the database.
type Executor struct {
	stateDB        *sql.DB
	defaultService string

	mu       sync.Mutex
	running  map[string]*ExecutionState
}

// NewExecutor creates a new Executor.
func NewExecutor(stateDB *sql.DB, defaultService string) *Executor {
	return &Executor{
		stateDB:        stateDB,
		defaultService: defaultService,
		running:        make(map[string]*ExecutionState),
	}
}

// IsRunning returns true if the named migration is currently executing.
func (e *Executor) IsRunning(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.running[name]
	return ok
}

// GetState returns the execution state for a running migration.
func (e *Executor) GetState(name string) *ExecutionState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.running[name]
}

// Cancel cancels a running migration.
func (e *Executor) Cancel(name string) {
	e.mu.Lock()
	es, ok := e.running[name]
	e.mu.Unlock()
	if ok {
		es.Cancel()
	}
}

// Run starts executing a migration. It blocks until complete.
func (e *Executor) Run(m *Migration, record *MigrationRecord) error {
	service := m.Service
	if service == "" {
		service = e.defaultService
	}
	if service == "" {
		return fmt.Errorf("no target service specified for %s", m.Name)
	}

	targetDB, err := ConnectService(service)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", service, err)
	}
	defer targetDB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	es := &ExecutionState{
		Name:      m.Name,
		Cancel:    cancel,
		StartedAt: time.Now(),
	}
	es.TotalAffected.Store(record.TotalAffected)
	es.LastCompletedID.Store(record.LastCompletedID)

	e.mu.Lock()
	e.running[m.Name] = es
	e.mu.Unlock()

	defer func() {
		cancel()
		e.mu.Lock()
		delete(e.running, m.Name)
		e.mu.Unlock()
	}()

	if err := UpdateStatus(e.stateDB, m.Name, "running"); err != nil {
		return err
	}

	if m.IsBatched() {
		return e.runBatched(ctx, m, record, targetDB, es)
	}
	return e.runSingle(ctx, m, targetDB, es)
}

func (e *Executor) runSingle(ctx context.Context, m *Migration, targetDB *sql.DB, es *ExecutionState) error {
	var execCtx context.Context
	var execCancel context.CancelFunc
	if m.Timeout > 0 {
		execCtx, execCancel = context.WithTimeout(ctx, m.Timeout)
	} else {
		execCtx, execCancel = context.WithCancel(ctx)
	}
	defer execCancel()

	result, err := targetDB.ExecContext(execCtx, m.SQL)
	if err != nil {
		_ = RecordError(e.stateDB, m.Name, err.Error())
		_ = UpdateStatus(e.stateDB, m.Name, "failed")
		return err
	}

	affected, _ := result.RowsAffected()
	es.TotalAffected.Store(affected)
	_ = UpdateProgress(e.stateDB, m.Name, 0, affected)
	_ = UpdateStatus(e.stateDB, m.Name, "completed")
	return nil
}

func (e *Executor) runBatched(ctx context.Context, m *Migration, record *MigrationRecord, targetDB *sql.DB, es *ExecutionState) error {
	// Get max ID
	var maxID int64
	row := targetDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM (%s) AS _psc_sub",
		m.BatchColumn, stripWhereClause(m.SQL)))
	// Simpler: query the table directly. We need to extract table name or just use a simpler approach.
	// Actually, let's query max from the batch column directly.
	// We need the table name from the SQL. For simplicity, query it raw.
	row = targetDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s",
		m.BatchColumn, extractTableForMax(m.SQL, m.BatchColumn)))
	if err := row.Scan(&maxID); err != nil {
		_ = RecordError(e.stateDB, m.Name, "failed to get max id: "+err.Error())
		_ = UpdateStatus(e.stateDB, m.Name, "failed")
		return err
	}

	es.MaxID = maxID
	_ = UpdateMaxID(e.stateDB, m.Name, maxID)

	startFrom := record.LastCompletedID
	if startFrom < 0 {
		startFrom = 0
	}

	var counter atomic.Int64
	counter.Store(startFrom)

	chunkSize := int64(m.ChunkSize)
	parallelism := m.Parallelism
	if parallelism < 1 {
		parallelism = 1
	}

	var wg sync.WaitGroup
	var firstErr atomic.Value
	var totalAffected atomic.Int64
	totalAffected.Store(record.TotalAffected)

	rateStart := time.Now()

	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					if firstErr.Load() == nil {
						_ = UpdateStatus(e.stateDB, m.Name, "cancelled")
					}
					return
				default:
				}

				start := counter.Add(chunkSize) - chunkSize
				if start > maxID {
					return
				}
				end := start + chunkSize - 1
				if end > maxID {
					end = maxID
				}

				chunkSQL := strings.ReplaceAll(m.SQL, ":start", fmt.Sprintf("%d", start))
				chunkSQL = strings.ReplaceAll(chunkSQL, ":end", fmt.Sprintf("%d", end))

				var execCtx context.Context
				var execCancel context.CancelFunc
				if m.Timeout > 0 {
					execCtx, execCancel = context.WithTimeout(ctx, m.Timeout)
				} else {
					execCtx, execCancel = context.WithCancel(ctx)
				}

				result, err := targetDB.ExecContext(execCtx, chunkSQL)
				execCancel()

				if err != nil {
					errMsg := fmt.Sprintf("chunk %d-%d: %s", start, end, err.Error())
					_ = RecordError(e.stateDB, m.Name, errMsg)
					if m.OnError == "continue" {
						continue
					}
					firstErr.Store(err)
					_ = UpdateStatus(e.stateDB, m.Name, "failed")
					return
				}

				rows, _ := result.RowsAffected()
				newTotal := totalAffected.Add(rows)
				es.TotalAffected.Store(newTotal)
				es.LastCompletedID.Store(end)

				elapsed := time.Since(rateStart).Seconds()
				if elapsed > 0 {
					es.Rate.Store(int64(float64(newTotal-record.TotalAffected) / elapsed))
				}

				_ = UpdateProgress(e.stateDB, m.Name, end, newTotal)
			}
		}()
	}

	wg.Wait()

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if v := firstErr.Load(); v != nil {
		return v.(error)
	}

	_ = UpdateStatus(e.stateDB, m.Name, "completed")
	return nil
}

// extractTableForMax attempts to extract the table name from an UPDATE or DELETE statement
// for querying MAX(column). This is a simple heuristic.
func extractTableForMax(sqlStr, column string) string {
	upper := strings.ToUpper(sqlStr)
	// Handle UPDATE table SET ...
	if idx := strings.Index(upper, "UPDATE "); idx >= 0 {
		rest := strings.TrimSpace(sqlStr[idx+7:])
		fields := strings.Fields(rest)
		if len(fields) > 0 {
			return fields[0]
		}
	}
	// Handle DELETE FROM table ...
	if idx := strings.Index(upper, "FROM "); idx >= 0 {
		rest := strings.TrimSpace(sqlStr[idx+5:])
		fields := strings.Fields(rest)
		if len(fields) > 0 {
			return fields[0]
		}
	}
	return "unknown_table"
}

// stripWhereClause is unused but kept for reference.
func stripWhereClause(s string) string { return s }
