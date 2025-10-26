package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// ServiceConfig holds PostgreSQL connection parameters
type ServiceConfig struct {
	Host     string
	Port     string
	DBName   string
	User     string
	Password string
}

// ParseServiceFile reads and parses a pg_service.conf file
func ParseServiceFile(path string) (map[string]ServiceConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open service file: %w", err)
	}
	defer file.Close()

	services := make(map[string]ServiceConfig)
	var currentService string
	var currentConfig ServiceConfig

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}

		// Check for service section header
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			// Save previous service if exists
			if currentService != "" {
				services[currentService] = currentConfig
			}
			// Start new service
			currentService = strings.Trim(line, "[]")
			currentConfig = ServiceConfig{Port: "5432"} // Default port
			continue
		}

		// Parse key=value pairs
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "host":
			currentConfig.Host = value
		case "port":
			currentConfig.Port = value
		case "dbname":
			currentConfig.DBName = value
		case "user":
			currentConfig.User = value
		case "password":
			currentConfig.Password = value
		}
	}

	// Save last service
	if currentService != "" {
		services[currentService] = currentConfig
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading service file: %w", err)
	}

	return services, nil
}

// ConnectionString generates a PostgreSQL connection string
func (c ServiceConfig) ConnectionString() string {
	return fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=require",
		c.Host, c.Port, c.DBName, c.User, c.Password)
}
