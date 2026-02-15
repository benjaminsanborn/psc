package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
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
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			if currentService != "" {
				services[currentService] = currentConfig
			}
			currentService = strings.Trim(line, "[]")
			currentConfig = ServiceConfig{Port: "5432"}
			continue
		}
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
	if currentService != "" {
		services[currentService] = currentConfig
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading service file: %w", err)
	}
	return services, nil
}

// ConnectionString generates a PostgreSQL connection string with SSL required
func (c ServiceConfig) ConnectionString() string {
	return c.ConnectionStringWithSSL("require")
}

// ConnectionStringWithSSL generates a PostgreSQL connection string with specified SSL mode
func (c ServiceConfig) ConnectionStringWithSSL(sslmode string) string {
	return fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.DBName, c.User, c.Password, sslmode)
}

// ConnectService opens a DB connection to the given pg_service.conf service name.
// It tries SSL first, then falls back to sslmode=disable.
func ConnectService(serviceName string) (*sql.DB, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	services, err := ParseServiceFile(filepath.Join(home, ".pg_service.conf"))
	if err != nil {
		return nil, fmt.Errorf("parsing pg_service.conf: %w", err)
	}
	cfg, ok := services[serviceName]
	if !ok {
		return nil, fmt.Errorf("service %q not found in pg_service.conf", serviceName)
	}

	// Try with SSL first
	db, err := sql.Open("postgres", cfg.ConnectionString())
	if err == nil {
		if pingErr := db.Ping(); pingErr == nil {
			return db, nil
		}
		db.Close()
	}

	// Fallback to no SSL
	db, err = sql.Open("postgres", cfg.ConnectionStringWithSSL("disable"))
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("connecting to service %q: %w", serviceName, err)
	}
	return db, nil
}
