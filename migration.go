package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Migration represents a parsed SQL migration file.
type Migration struct {
	Name        string
	Filename    string
	SQL         string
	Service     string // target pg_service name (may be empty for default)
	BatchColumn string
	ChunkSize   int
	Parallelism int
	OnError     string // "abort" or "continue"
	Timeout     time.Duration
}

// IsBatched returns true if the migration uses batch processing.
func (m *Migration) IsBatched() bool {
	return m.BatchColumn != ""
}

// ParseMigrationFile parses a .sql migration file and extracts psc directives.
func ParseMigrationFile(path string) (*Migration, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := &Migration{
		Filename:    path,
		OnError:     "abort",
		Parallelism: 1,
		ChunkSize:   10000,
	}
	var sqlLines []string

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "-- psc:") {
			directive := strings.TrimPrefix(trimmed, "-- psc:")
			if err := parseDirective(m, directive); err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
		} else {
			sqlLines = append(sqlLines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	m.SQL = strings.TrimSpace(strings.Join(sqlLines, "\n"))
	if m.Name == "" {
		return nil, fmt.Errorf("%s: missing required psc:migrate name=<name> directive", path)
	}
	return m, nil
}

func parseDirective(m *Migration, directive string) error {
	parts := strings.Fields(directive)
	if len(parts) == 0 {
		return nil
	}

	switch parts[0] {
	case "migrate":
		kv := parseKV(parts[1:])
		if v, ok := kv["name"]; ok {
			m.Name = v
		}
	case "target":
		kv := parseKV(parts[1:])
		if v, ok := kv["service"]; ok {
			m.Service = v
		}
	case "batch":
		kv := parseKV(parts[1:])
		if v, ok := kv["column"]; ok {
			m.BatchColumn = v
		}
		if v, ok := kv["chunk"]; ok {
			if n, err := strconv.Atoi(v); err == nil {
				m.ChunkSize = n
			}
		}
		if v, ok := kv["parallelism"]; ok {
			if n, err := strconv.Atoi(v); err == nil {
				m.Parallelism = n
			}
		}
	case "on_error":
		if len(parts) > 1 {
			m.OnError = parts[1]
		}
	case "timeout":
		if len(parts) > 1 {
			d, err := time.ParseDuration(parts[1])
			if err != nil {
				return fmt.Errorf("invalid timeout: %w", err)
			}
			m.Timeout = d
		}
	}
	return nil
}

func parseKV(parts []string) map[string]string {
	kv := make(map[string]string)
	for _, p := range parts {
		if idx := strings.Index(p, "="); idx > 0 {
			kv[p[:idx]] = p[idx+1:]
		}
	}
	return kv
}
