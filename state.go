package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// LoadCopyState loads the copy state from a JSON file
// Supports both new multi-table format and old single-table format (for backward compatibility)
func LoadCopyState(filename string) (*CopyState, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	var state CopyState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state file: %w", err)
	}

	// Backward compatibility: migrate old single-table format to new format
	if len(state.Tables) == 0 {
		// Try to read as old format
		var oldState struct {
			SourceService string   `json:"source_service"`
			TargetService string   `json:"target_service"`
			TableName     string   `json:"table_name"`
			WhereClause   string   `json:"where_clause,omitempty"`
			PrimaryKey    string   `json:"primary_key"`
			ChunkSize     int64    `json:"chunk_size"`
			Parallelism   int      `json:"parallelism"`
			LastID        int64    `json:"last_id"`
			StartTime     string   `json:"start_time"`
			LastUpdate    string   `json:"last_update"`
			Errors        []string `json:"errors,omitempty"`
		}
		if err := json.Unmarshal(data, &oldState); err == nil && oldState.TableName != "" {
			// Migrate to new format
			state = CopyState{
				SourceService: oldState.SourceService,
				TargetService: oldState.TargetService,
				ChunkSize:     oldState.ChunkSize,
				Parallelism:   oldState.Parallelism,
				StartTime:     oldState.StartTime,
				LastUpdate:    oldState.LastUpdate,
				Tables: []TableState{
					{
						TableName:   oldState.TableName,
						WhereClause: oldState.WhereClause,
						PrimaryKey:  oldState.PrimaryKey,
						LastID:      oldState.LastID,
						Errors:      oldState.Errors,
					},
				},
			}
		}
	}

	return &state, nil
}

// FindCopyStateFile finds a state file for the given parameters in ~/.psc/in_progress/
// First tries new format (source_target.pscstate), then falls back to old format (source_target_table.pscstate)
func FindCopyStateFile(sourceName, targetName, tableName string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	// Try new format first (single file per operation)
	filename := fmt.Sprintf("%s/.psc/in_progress/%s_%s.pscstate", home, sourceName, targetName)
	if _, err := os.Stat(filename); err == nil {
		// Verify it contains the requested table
		if state, err := LoadCopyState(filename); err == nil {
			if state.GetTableState(tableName) != nil {
				return filename
			}
		}
	}

	// Fall back to old format (one file per table) for backward compatibility
	filename = fmt.Sprintf("%s/.psc/in_progress/%s_%s_%s.pscstate", home, sourceName, targetName, tableName)
	if _, err := os.Stat(filename); err == nil {
		return filename
	}
	return ""
}

// FindAllCopyStateFiles finds all copy state files in ~/.psc/in_progress/
func FindAllCopyStateFiles() ([]string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	inProgressDir := fmt.Sprintf("%s/.psc/in_progress", home)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(inProgressDir, 0755); err != nil {
		return nil, err
	}

	pattern := fmt.Sprintf("%s/*.pscstate", inProgressDir)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var stateFiles []string
	for _, file := range files {
		// Verify it's a valid state file by trying to parse it
		if _, err := LoadCopyState(file); err == nil {
			stateFiles = append(stateFiles, file)
		}
	}

	return stateFiles, nil
}
