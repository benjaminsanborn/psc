package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// CopyTable copies a table from source to target database
func CopyTable(source, target ServiceConfig, tableName string) error {
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
	fmt.Printf("SQL: %s\n", checkSQL)
	if err := targetDB.QueryRow(checkSQL, tableName).Scan(&exists); err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}
	if !exists {
		return fmt.Errorf("table '%s' does not exist on target database", tableName)
	}

	// Copy data
	fmt.Printf("Copying data...\n")
	return copyData(sourceDB, targetDB, tableName)
}

// copyData copies all rows from source to target
func copyData(sourceDB, targetDB *sql.DB, tableName string) error {
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

	// Read all data from source
	selectSQL := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columns, ", "), tableName)
	fmt.Printf("SQL: %s\n", selectSQL)
	dataRows, err := sourceDB.Query(selectSQL)
	if err != nil {
		return err
	}
	defer dataRows.Close()

	// Prepare insert statement
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	fmt.Printf("SQL: %s\n", insertSQL)

	stmt, err := targetDB.Prepare(insertSQL)
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Copy rows
	rowCount := 0
	for dataRows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := dataRows.Scan(valuePtrs...); err != nil {
			return err
		}

		if _, err := stmt.Exec(values...); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
		rowCount++

		if rowCount%1000 == 0 {
			fmt.Printf("Copied %d rows...\n", rowCount)
		}
	}

	fmt.Printf("Total rows copied: %d\n", rowCount)
	return dataRows.Err()
}
