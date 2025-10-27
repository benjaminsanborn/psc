package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	// Check if running without arguments - launch interactive mode
	if len(os.Args) == 1 {
		if err := runInteractive(); err != nil {
			log.Fatalf("Interactive mode failed: %v", err)
		}
		return
	}

	source := flag.String("source", "", "Source service name from pg_service.conf")
	target := flag.String("target", "", "Target service name from pg_service.conf")
	table := flag.String("table", "", "Table name to copy")
	whereClause := flag.String("where", "", "Optional WHERE clause to filter rows (e.g., 'status = active')")
	primaryKey := flag.String("primary-key", "id", "Primary key column for chunking (defaults to 'id')")
	lastID := flag.Int64("last-id", 0, "Resume copy from this ID (optional, defaults to 0)")
	chunkSize := flag.Int64("chunk-size", 1000, "Number of rows per batch (defaults to 1000)")
	parallelism := flag.Int("parallelism", 1, "Number of concurrent workers (defaults to 1)")
	targetSetup := flag.String("target-setup", "", "Optional SQL statements to execute on target before copy (semicolon-separated)")

	flag.Parse()

	if *source == "" || *target == "" || *table == "" {
		fmt.Println("Usage: psc -source <service> -target <service> -table <tablename>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Determine service file path
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("Failed to get home directory: %v", err)
	}
	configPath := fmt.Sprintf("%s/.pg_service.conf", home)

	// Parse service file
	services, err := ParseServiceFile(configPath)
	if err != nil {
		log.Fatalf("Failed to parse service file: %v", err)
	}

	sourceConfig, ok := services[*source]
	if !ok {
		log.Fatalf("Source service '%s' not found in %s", *source, configPath)
	}

	targetConfig, ok := services[*target]
	if !ok {
		log.Fatalf("Target service '%s' not found in %s", *target, configPath)
	}

	// Copy table
	if *lastID > 0 {
		if *whereClause != "" {
			fmt.Printf("Resuming copy of table '%s' from '%s' to '%s' starting at ID %d (WHERE: %s, chunk size: %d, workers: %d)...\n", *table, *source, *target, *lastID, *whereClause, *chunkSize, *parallelism)
		} else {
			fmt.Printf("Resuming copy of table '%s' from '%s' to '%s' starting at ID %d (chunk size: %d, workers: %d)...\n", *table, *source, *target, *lastID, *chunkSize, *parallelism)
		}
	} else {
		if *whereClause != "" {
			fmt.Printf("Copying table '%s' from '%s' to '%s' (WHERE: %s, chunk size: %d, workers: %d)...\n", *table, *source, *target, *whereClause, *chunkSize, *parallelism)
		} else {
			fmt.Printf("Copying table '%s' from '%s' to '%s' (chunk size: %d, workers: %d)...\n", *table, *source, *target, *chunkSize, *parallelism)
		}
	}
	if err := CopyTable(*source, *target, sourceConfig, targetConfig, *table, *whereClause, *primaryKey, *lastID, *chunkSize, *parallelism, *targetSetup); err != nil {
		log.Fatalf("Failed to copy table: %v", err)
	}

	fmt.Println("Table copied successfully!")
}
