package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// LoadCopyState loads the copy state from a JSON file
func LoadCopyState(filename string) (*CopyState, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	var state CopyState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state file: %w", err)
	}

	return &state, nil
}

// FindCopyStateFile finds a state file for the given parameters
func FindCopyStateFile(sourceName, targetName, tableName string) string {
	filename := fmt.Sprintf("%s_%s_%s.pscstate", sourceName, targetName, tableName)
	if _, err := os.Stat(filename); err == nil {
		return filename
	}
	return ""
}

// FindAllCopyStateFiles finds all copy state files in the current directory
func FindAllCopyStateFiles() ([]string, error) {
	files, err := filepath.Glob("*.pscstate")
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
