package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// CopyState holds the state of an ongoing copy operation
type CopyState struct {
	SourceService string   `json:"source_service"`
	TargetService string   `json:"target_service"`
	TableName     string   `json:"table_name"`
	PrimaryKey    string   `json:"primary_key"`
	ChunkSize     int64    `json:"chunk_size"`
	Parallelism   int      `json:"parallelism"`
	LastID        int64    `json:"last_id"` // Highest successfully completed ID
	StartTime     string   `json:"start_time"`
	LastUpdate    string   `json:"last_update"`
	Errors        []string `json:"errors,omitempty"` // Any errors encountered
}

// CopyProgress holds progress information for a copy operation
type CopyProgress struct {
	Message    string
	TotalRows  int64
	CopiedRows int64
	LastID     int64
	Percentage float64
	Done       bool
	Error      error
}

// CopyTableWithProgress copies a table with progress updates sent to a channel
func CopyTableWithProgress(ctx context.Context, sourceName, targetName string, source, target ServiceConfig, tableName, primaryKey string, lastID int64, chunkSize int64, parallelism int, progressChan chan<- CopyProgress) error {
	defer func() {
		if r := recover(); r != nil {
			progressChan <- CopyProgress{Error: fmt.Errorf("panic: %v", r)}
		}
	}()

	return copyTableInternal(ctx, sourceName, targetName, source, target, tableName, primaryKey, lastID, chunkSize, parallelism, progressChan)
}

// CopyTable copies a table from source to target database (non-interactive version)
func CopyTable(sourceName, targetName string, source, target ServiceConfig, tableName, primaryKey string, lastID int64, chunkSize int64, parallelism int) error {
	return copyTableInternal(context.Background(), sourceName, targetName, source, target, tableName, primaryKey, lastID, chunkSize, parallelism, nil)
}

func copyTableInternal(ctx context.Context, sourceName, targetName string, source, target ServiceConfig, tableName, primaryKey string, lastID int64, chunkSize int64, parallelism int, progressChan chan<- CopyProgress) error {
	sendProgress := func(msg string, totalRows, copiedRows, lastID int64, percentage float64) {
		if progressChan != nil {
			progressChan <- CopyProgress{
				Message:    msg,
				TotalRows:  totalRows,
				CopiedRows: copiedRows,
				LastID:     lastID,
				Percentage: percentage,
			}
		} else {
			fmt.Println(msg)
		}
	}
	sendProgress("Connecting to databases...", 0, 0, 0, 0)

	// Connect to source with SSL retry logic
	sourceDB, err := connectWithSSLRetry(source, "source", sendProgress)
	if err != nil {
		return err
	}
	defer sourceDB.Close()

	// Connect to target with SSL retry logic
	targetDB, err := connectWithSSLRetry(target, "target", sendProgress)
	if err != nil {
		return err
	}
	defer targetDB.Close()

	sendProgress("Checking target table...", 0, 0, 0, 0)

	// Check if table exists on target
	var exists bool
	checkSQL := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"
	if err := targetDB.QueryRow(checkSQL, tableName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("table '%s' does not exist on target database", tableName)
	}

	// Initialize copy state file
	stateFile := fmt.Sprintf("%s_%s_%s.pscstate", sourceName, targetName, tableName)
	state := CopyState{
		SourceService: sourceName,
		TargetService: targetName,
		TableName:     tableName,
		PrimaryKey:    primaryKey,
		ChunkSize:     chunkSize,
		Parallelism:   parallelism,
		LastID:        lastID,
		StartTime:     time.Now().Format(time.RFC3339),
		LastUpdate:    time.Now().Format(time.RFC3339),
		Errors:        []string{},
	}

	if err := saveCopyState(stateFile, &state); err != nil {
		sendProgress(fmt.Sprintf("Warning: failed to create state file: %v", err), 0, 0, 0, 0)
	}

	sendProgress("Initializing state file...", 0, 0, 0, 0)

	// Copy data
	sendProgress("Starting data copy...", 0, 0, 0, 0)
	return copyData(ctx, sourceName, targetName, sourceDB, targetDB, tableName, primaryKey, lastID, chunkSize, parallelism, stateFile, &state, progressChan)
}

// chunkResult holds the result of a chunk copy operation
type chunkResult struct {
	startID  int64
	endID    int64
	rowCount int64
	elapsed  time.Duration
	err      error
}

// copyData copies all rows from source to target using COPY commands in chunks with parallel workers
func copyData(ctx context.Context, sourceName, targetName string, sourceDB, targetDB *sql.DB, tableName, idColumn string, startID int64, chunkSize int64, parallelism int, stateFile string, state *CopyState, progressChan chan<- CopyProgress) error {
	// Create a child context so we can still cancel internally if needed
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sendProgress := func(msg string, totalRows, copiedRows, lastID int64, percentage float64) {
		if progressChan != nil {
			progressChan <- CopyProgress{
				Message:    msg,
				TotalRows:  totalRows,
				CopiedRows: copiedRows,
				LastID:     lastID,
				Percentage: percentage,
			}
		}
	}
	sendProgress("Getting row count...", 0, 0, 0, 0)

	// Get estimated row count
	var estimatedRows int64
	countSQL := fmt.Sprintf("SELECT reltuples::bigint FROM pg_class WHERE relname = '%s'", tableName)
	if err := sourceDB.QueryRow(countSQL).Scan(&estimatedRows); err != nil {
		// Fallback to COUNT(*) if estimate not available
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
		if err := sourceDB.QueryRow(countSQL).Scan(&estimatedRows); err != nil {
			return fmt.Errorf("failed to get row count: %w", err)
		}
	}
	sendProgress(fmt.Sprintf("Found %d rows to copy", estimatedRows), estimatedRows, 0, startID, 0)

	// Get column names
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`
	rows, err := sourceDB.Query(query, tableName)
	if err != nil {
		return err
	}

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			rows.Close()
			return err
		}
		columns = append(columns, col)
	}
	rows.Close()

	if len(columns) == 0 {
		return fmt.Errorf("no columns found for table %s", tableName)
	}

	sendProgress(fmt.Sprintf("Using column '%s' for chunking with %d workers", idColumn, parallelism), estimatedRows, 0, startID, 0)

	// Determine if we should suppress output (interactive mode)
	quiet := progressChan != nil

	// Parallel copy coordination
	var (
		mu                 sync.Mutex
		nextStartID        = startID
		highestCompletedID = startID - 1
		totalCopied        int64
		errors             []string
		wg                 sync.WaitGroup
	)

	// Result channel for collecting worker results
	resultChan := make(chan chunkResult, parallelism*2)

	// Start worker goroutines
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				// Check if cancelled
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Get next chunk to process
				mu.Lock()
				if nextStartID > estimatedRows+startID {
					mu.Unlock()
					return
				}
				myStartID := nextStartID
				myEndID := myStartID + chunkSize
				nextStartID = myEndID
				mu.Unlock()

				// Copy this chunk
				startTime := time.Now()
				copied, actualEndID, err := copyChunk(sourceName, targetName, sourceDB, tableName, idColumn, myStartID, chunkSize, quiet)
				elapsed := time.Since(startTime)

				// Send result
				resultChan <- chunkResult{
					startID:  myStartID,
					endID:    actualEndID,
					rowCount: copied,
					elapsed:  elapsed,
					err:      err,
				}

				// If no more rows or error, stop this worker
				if copied == 0 || err != nil {
					return
				}
			}
		}(i)
	}

	// Close result channel when all workers done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results
	for result := range resultChan {

		// Handle error
		if result.err != nil {
			mu.Lock()
			errMsg := fmt.Sprintf("Error copying chunk starting at %d: \n%v", result.startID, result.err)
			errors = append(errors, errMsg)
			state.Errors = errors
			mu.Unlock()

			// Log error
			if !quiet {
				fmt.Printf("ERROR: %s\n", errMsg)
			}
			sendProgress(errMsg, estimatedRows, totalCopied, highestCompletedID, float64(totalCopied)/float64(estimatedRows)*100)

			// Cancel all workers on error
			cancel()
			continue
		}

		// No rows means we're done
		if result.rowCount == 0 {
			continue
		}

		// Update progress
		mu.Lock()
		totalCopied += result.rowCount
		if result.endID > highestCompletedID {
			highestCompletedID = result.endID
		}

		// Update state file
		state.LastID = highestCompletedID
		state.LastUpdate = time.Now().Format(time.RFC3339)
		if err := saveCopyState(stateFile, state); err != nil {
			// Silently continue on state save error
		}

		percentage := float64(totalCopied) / float64(estimatedRows) * 100
		mu.Unlock()

		msg := fmt.Sprintf("Copied chunk %d-%d (%d rows in %v)", result.startID, result.endID, result.rowCount, result.elapsed)
		if !quiet {
			fmt.Println(msg)
		}
		sendProgress(msg, estimatedRows, totalCopied, highestCompletedID, percentage)
	}

	// Check if cancelled
	select {
	case <-ctx.Done():
		mu.Lock()
		totalCopiedFinal := totalCopied
		lastIDFinal := highestCompletedID
		mu.Unlock()
		sendProgress(fmt.Sprintf("Copy cancelled. Copied %d rows up to ID %d", totalCopiedFinal, lastIDFinal), estimatedRows, totalCopiedFinal, lastIDFinal, float64(totalCopiedFinal)/float64(estimatedRows)*100)
	default:
	}

	// Check for errors
	mu.Lock()
	hasErrors := len(errors) > 0
	mu.Unlock()

	if hasErrors {
		return fmt.Errorf("copy completed with %d error(s): \n%s", len(errors), strings.Join(errors, "; "))
	}

	sendProgress("Copy complete!", estimatedRows, totalCopied, highestCompletedID, 100)
	if progressChan != nil {
		progressChan <- CopyProgress{Done: true}
	}

	return nil
}

// copyChunk copies a single chunk of data
func copyChunk(sourceName, targetName string, sourceDB *sql.DB, tableName string,
	idColumn string, lastMaxID int64, chunkSize int64, quiet bool) (int64, int64, error) {

	// Get the MIN id in this chunk
	minIDQuery := fmt.Sprintf("SELECT MIN(%s) FROM (SELECT %s FROM %s WHERE %s >= %d ORDER BY %s LIMIT %d) t",
		idColumn, idColumn, tableName, idColumn, lastMaxID, idColumn, chunkSize)
	if !quiet {
		fmt.Printf("SQL: %s\n", minIDQuery)
	}

	var minID sql.NullInt64
	if err := sourceDB.QueryRow(minIDQuery).Scan(&minID); err != nil {
		return 0, lastMaxID, fmt.Errorf("failed to get min ID: %w", err)
	}

	if !minID.Valid {
		return 0, lastMaxID, nil
	}

	var maxID = minID.Int64 + chunkSize

	// Build the COPY query
	copySQL := fmt.Sprintf("COPY (SELECT * FROM %s WHERE %s >= %d AND %s < %d ORDER BY %s) TO STDOUT (FORMAT binary)",
		tableName, idColumn, minID.Int64, idColumn, maxID, idColumn)

	if !quiet {
		fmt.Printf("SQL: %s\n", copySQL)
	}

	// Create psql commands
	sourceCmd := exec.Command("psql", fmt.Sprintf("service=%s", sourceName), "-Atc", copySQL)
	targetCmd := exec.Command("psql", fmt.Sprintf("service=%s", targetName), "-c",
		fmt.Sprintf("COPY %s FROM STDIN (FORMAT binary)", tableName))

	// Set up pipes
	targetCmd.Stdin, _ = sourceCmd.StdoutPipe()

	// Capture stderr and stdout for both commands
	var sourceStderr, targetStderr, targetStdout strings.Builder
	sourceCmd.Stderr = &sourceStderr
	targetCmd.Stderr = &targetStderr
	targetCmd.Stdout = &targetStdout

	// Start target first (it will wait for input)
	if err := targetCmd.Start(); err != nil {
		return 0, lastMaxID, fmt.Errorf("failed to start target psql: %w", err)
	}

	// Start source
	if err := sourceCmd.Run(); err != nil {
		if !quiet {
			if sourceStderr.Len() > 0 {
				fmt.Printf("Source stderr: %s\n", sourceStderr.String())
			}
		}
		return 0, lastMaxID, fmt.Errorf("source psql failed: %w\nstderr: %s", err, sourceStderr.String())
	}

	// Wait for target to complete
	if err := targetCmd.Wait(); err != nil {
		if !quiet {
			if targetStderr.Len() > 0 {
				fmt.Printf("Target stderr: %s\n", targetStderr.String())
			}
			if targetStdout.Len() > 0 {
				fmt.Printf("Target stdout: %s\n", targetStdout.String())
			}
		}
		return 0, lastMaxID, fmt.Errorf("target psql failed: %w\nstderr: %s", err, targetStderr.String())
	}

	if !quiet {
		if sourceStderr.Len() > 0 {
			fmt.Printf("Source stderr: %s\n", sourceStderr.String())
		}
		if targetStderr.Len() > 0 {
			fmt.Printf("Target stderr: %s\n", targetStderr.String())
		}
		if targetStdout.Len() > 0 {
			fmt.Printf("Target stdout: %s\n", targetStdout.String())
		}
	}

	return chunkSize, maxID, nil
}

func saveCopyState(filename string, state *CopyState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// connectWithSSLRetry attempts to connect with SSL, and retries without SSL if the server doesn't support it
func connectWithSSLRetry(config ServiceConfig, dbName string, sendProgress func(string, int64, int64, int64, float64)) (*sql.DB, error) {
	// First try with SSL
	db, err := sql.Open("postgres", config.ConnectionString())
	if err != nil {
		return nil, fmt.Errorf("failed to open %s database connection: %w", dbName, err)
	}

	if err := db.Ping(); err != nil {
		// Check if it's an SSL error
		if strings.Contains(err.Error(), "SSL is not enabled on the server") {
			db.Close()
			msg := fmt.Sprintf("SSL not supported on %s, retrying without SSL...", dbName)
			sendProgress(msg, 0, 0, 0, 0)

			// Retry without SSL
			db, err = sql.Open("postgres", config.ConnectionStringWithSSL("disable"))
			if err != nil {
				return nil, fmt.Errorf("failed to open %s database connection (no SSL): %w", dbName, err)
			}

			if err := db.Ping(); err != nil {
				db.Close()
				return nil, fmt.Errorf("failed to ping %s database (no SSL): %w", dbName, err)
			}

			return db, nil
		}

		db.Close()
		return nil, fmt.Errorf("failed to ping %s database: %w", dbName, err)
	}

	return db, nil
}
