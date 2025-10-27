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

// FindCopyStateFile finds a state file for the given parameters in ~/.psc/in_progress/
func FindCopyStateFile(sourceName, targetName, tableName string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	filename := fmt.Sprintf("%s/.psc/in_progress/%s_%s_%s.pscstate", home, sourceName, targetName, tableName)
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
