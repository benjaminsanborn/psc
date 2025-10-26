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
	serviceFile := flag.String("service-file", "", "Path to pg_service.conf (optional, defaults to ~/.pg_service.conf)")

	flag.Parse()

	if *source == "" || *target == "" || *table == "" {
		fmt.Println("Usage: psc -source <service> -target <service> -table <tablename>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Determine service file path
	configPath := *serviceFile
	if configPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatalf("Failed to get home directory: %v", err)
		}
		configPath = fmt.Sprintf("%s/.pg_service.conf", home)
	}

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
	fmt.Printf("Copying table '%s' from '%s' to '%s'...\n", *table, *source, *target)
	if err := CopyTable(sourceConfig, targetConfig, *table); err != nil {
		log.Fatalf("Failed to copy table: %v", err)
	}

	fmt.Println("Table copied successfully!")
}
