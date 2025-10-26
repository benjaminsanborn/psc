package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// CopyState holds the state of an ongoing copy operation
type CopyState struct {
	SourceService string `json:"source_service"`
	TargetService string `json:"target_service"`
	TableName     string `json:"table_name"`
	PrimaryKey    string `json:"primary_key"`
	ChunkSize     int64  `json:"chunk_size"`
	LastID        int64  `json:"last_id"`
	StartTime     string `json:"start_time"`
	LastUpdate    string `json:"last_update"`
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
func CopyTableWithProgress(sourceName, targetName string, source, target ServiceConfig, tableName, primaryKey string, lastID int64, chunkSize int64, progressChan chan<- CopyProgress) error {
	defer func() {
		if r := recover(); r != nil {
			progressChan <- CopyProgress{Error: fmt.Errorf("panic: %v", r)}
		}
	}()

	return copyTableInternal(sourceName, targetName, source, target, tableName, primaryKey, lastID, chunkSize, progressChan)
}

// CopyTable copies a table from source to target database (non-interactive version)
func CopyTable(sourceName, targetName string, source, target ServiceConfig, tableName, primaryKey string, lastID int64, chunkSize int64) error {
	return copyTableInternal(sourceName, targetName, source, target, tableName, primaryKey, lastID, chunkSize, nil)
}

func copyTableInternal(sourceName, targetName string, source, target ServiceConfig, tableName, primaryKey string, lastID int64, chunkSize int64, progressChan chan<- CopyProgress) error {
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

	// Connect to source
	sourceDB, err := sql.Open("postgres", source.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to connect to source: %w", err)
	}
	defer sourceDB.Close()

	if err := sourceDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping source database: %w", err)
	}

	// Connect to target
	targetDB, err := sql.Open("postgres", target.ConnectionString())
	if err != nil {
		return fmt.Errorf("failed to connect to target: %w", err)
	}
	defer targetDB.Close()

	if err := targetDB.Ping(); err != nil {
		return fmt.Errorf("failed to ping target database: %w", err)
	}

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
		LastID:        lastID,
		StartTime:     time.Now().Format(time.RFC3339),
		LastUpdate:    time.Now().Format(time.RFC3339),
	}

	if err := saveCopyState(stateFile, &state); err != nil {
		sendProgress(fmt.Sprintf("Warning: failed to create state file: %v", err), 0, 0, 0, 0)
	}

	sendProgress("Initializing state file...", 0, 0, 0, 0)

	// Copy data
	sendProgress("Starting data copy...", 0, 0, 0, 0)
	return copyData(sourceName, targetName, sourceDB, targetDB, tableName, primaryKey, lastID, chunkSize, stateFile, &state, progressChan)
}

// copyData copies all rows from source to target using COPY commands in chunks
func copyData(sourceName, targetName string, sourceDB, targetDB *sql.DB, tableName, idColumn string, startID int64, chunkSize int64, stateFile string, state *CopyState, progressChan chan<- CopyProgress) error {
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

	sendProgress(fmt.Sprintf("Using column '%s' for chunking", idColumn), estimatedRows, 0, startID, 0)

	// Copy data in chunks
	lastMaxID := startID
	totalCopied := int64(0)

	// Determine if we should suppress output (interactive mode)
	quiet := progressChan != nil

	for {
		startTime := time.Now()
		copied, newMaxID, err := copyChunk(sourceName, targetName, sourceDB, tableName, idColumn, lastMaxID, chunkSize, quiet)
		elapsed := time.Since(startTime)

		if err != nil {
			return fmt.Errorf("failed to copy chunk (lastMaxID=%v): %w", lastMaxID, err)
		}

		if copied == 0 {
			break
		}

		// Only update pointer after successful copy
		lastMaxID = newMaxID
		totalCopied += copied

		// Update state file with new progress
		state.LastID = lastMaxID
		state.LastUpdate = time.Now().Format(time.RFC3339)
		if err := saveCopyState(stateFile, state); err != nil {
			// Silently continue on state save error
		}

		percentage := float64(totalCopied) / float64(estimatedRows) * 100
		sendProgress(fmt.Sprintf("Copying... (chunk took %v)", elapsed), estimatedRows, totalCopied, lastMaxID, percentage)

		if copied < chunkSize {
			break
		}
	}

	sendProgress("Copy completed!", estimatedRows, totalCopied, lastMaxID, 100.0)
	if progressChan != nil {
		progressChan <- CopyProgress{Done: true}
	}
	return nil
}

// saveCopyState saves the copy state to a JSON file
func saveCopyState(filename string, state *CopyState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

// copyChunk copies a single chunk of data using psql COPY with binary format
func copyChunk(sourceName, targetName string, sourceDB *sql.DB, tableName string,
	idColumn string, lastMaxID int64, chunkSize int64, quiet bool) (int64, int64, error) {

	// Get the minimum ID for this chunk
	var minID int64 = 1
	if lastMaxID != 0 {
		minID = lastMaxID
	}
	var maxID = minID + chunkSize

	// Build the COPY command
	copySQL := fmt.Sprintf("COPY (SELECT * FROM %s WHERE %s >= %d AND %s < %d  ) TO STDOUT (FORMAT binary)",
		tableName, idColumn, minID, idColumn, maxID)

	// Build psql commands
	sourceCmd := exec.Command("psql", fmt.Sprintf("service=%s", sourceName), "-Atc", copySQL)
	targetCmd := exec.Command("psql", fmt.Sprintf("service=%s", targetName), "-c",
		fmt.Sprintf("COPY %s FROM STDIN (FORMAT binary)", tableName))

	// Pipe source to target
	pipe, err := sourceCmd.StdoutPipe()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create pipe: %w", err)
	}
	targetCmd.Stdin = pipe

	// Capture error and output
	sourceCmdErr := &strings.Builder{}
	targetCmdErr := &strings.Builder{}
	targetCmdOut := &strings.Builder{}
	sourceCmd.Stderr = sourceCmdErr
	targetCmd.Stderr = targetCmdErr
	targetCmd.Stdout = targetCmdOut

	// Start target first
	if err := targetCmd.Start(); err != nil {
		return 0, 0, fmt.Errorf("failed to start target psql: %w", err)
	}

	// Start source
	if err := sourceCmd.Start(); err != nil {
		targetCmd.Process.Kill()
		return 0, 0, fmt.Errorf("failed to start source psql: %w", err)
	}

	// Wait for source to finish
	if err := sourceCmd.Wait(); err != nil {
		targetCmd.Process.Kill()
		return 0, 0, fmt.Errorf("source psql failed: %w, stderr: %s", err, sourceCmdErr.String())
	}

	// Close the pipe
	pipe.Close()

	// Wait for target to finish
	if err := targetCmd.Wait(); err != nil {
		return 0, 0, fmt.Errorf("target psql failed: %w, stderr: %s", err, targetCmdErr.String())
	}

	// Print output from commands (only in non-quiet mode)
	if !quiet {
		if sourceCmdErr.Len() > 0 {
			fmt.Printf("Source stderr: %s\n", sourceCmdErr.String())
		}
		if targetCmdErr.Len() > 0 {
			fmt.Printf("Target stderr: %s\n", targetCmdErr.String())
		}
		if targetCmdOut.Len() > 0 {
			fmt.Printf("Target stdout: %s\n", targetCmdOut.String())
		}

		fmt.Printf("Shell: psql service=%s -Atc \"%s\" | psql service=%s -c \"COPY %s FROM STDIN (FORMAT binary)\"\n",
			sourceName, copySQL, targetName, tableName)
	}

	return chunkSize, maxID, nil
}
