package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	source := flag.String("source", "", "Source service name from pg_service.conf")
	target := flag.String("target", "", "Target service name from pg_service.conf")
	table := flag.String("table", "", "Table name to copy")
	primaryKey := flag.String("primary-key", "id", "Primary key column for chunking (defaults to 'id')")
	lastID := flag.Int64("last-id", 0, "Resume copy from this ID (optional, defaults to 0)")

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
		fmt.Printf("Resuming copy of table '%s' from '%s' to '%s' starting at ID %d...\n", *table, *source, *target, *lastID)
	} else {
		fmt.Printf("Copying table '%s' from '%s' to '%s'...\n", *table, *source, *target)
	}
	if err := CopyTable(*source, *target, sourceConfig, targetConfig, *table, *primaryKey, *lastID); err != nil {
		log.Fatalf("Failed to copy table: %v", err)
	}

	fmt.Println("Table copied successfully!")
}
