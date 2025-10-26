package main

import (
	"database/sql"
	"fmt"
	"os/exec"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// CopyTable copies a table from source to target database
func CopyTable(sourceName, targetName string, source, target ServiceConfig, tableName, primaryKey string, lastID int64) error {
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

	// Check if table exists on target
	var exists bool
	checkSQL := "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"
	if err := targetDB.QueryRow(checkSQL, tableName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("table '%s' does not exist on target database", tableName)
	}

	// Copy data
	fmt.Printf("Copying data...\n")
	return copyData(sourceName, targetName, sourceDB, targetDB, tableName, primaryKey, lastID)
}

// copyData copies all rows from source to target using COPY commands in chunks
func copyData(sourceName, targetName string, sourceDB, targetDB *sql.DB, tableName, idColumn string, startID int64) error {
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
	fmt.Printf("Estimated rows: %d\n", estimatedRows)

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

	fmt.Printf("Using column '%s' for chunking\n", idColumn)

	// Copy data in chunks
	chunkSize := int64(1000)
	lastMaxID := startID
	totalCopied := int64(0)

	for {
		startTime := time.Now()
		copied, newMaxID, err := copyChunk(sourceName, targetName, sourceDB, tableName, idColumn, lastMaxID, chunkSize)
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

		fmt.Printf("Progress: %d/%d rows (%.1f%%) - chunk took %v\n", totalCopied, estimatedRows,
			float64(totalCopied)/float64(estimatedRows)*100, elapsed)

		if copied < chunkSize {
			break
		}
	}

	fmt.Printf("Total rows copied: %d\n", totalCopied)
	return nil
}

// copyChunk copies a single chunk of data using psql COPY with binary format
func copyChunk(sourceName, targetName string, sourceDB *sql.DB, tableName string,
	idColumn string, lastMaxID int64, chunkSize int64) (int64, int64, error) {

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

	// Print output from commands
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

	return chunkSize, maxID, nil
}
