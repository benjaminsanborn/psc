package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type screen int

const (
	screenResume screen = iota
	screenSource
	screenTarget
	screenTable
	screenWhereClause
	screenPrimaryKey
	screenLastID
	screenChunkSize
	screenParallelism
	screenConfirm
	screenCopying
	screenDone
)

type tableConfig struct {
	whereClause string
	primaryKey  string
	lastID      string
}

type model struct {
	screen              screen
	services            map[string]ServiceConfig
	serviceNames        []string
	source              string
	target              string
	table               string
	tables              []string
	selectedTables      map[string]bool
	tableConfigs        map[string]*tableConfig // per-table settings
	tablesToConfigure   []string                // ordered list of tables to configure
	currentConfigIndex  int                     // which table we're configuring
	chunkSize           string
	chunkSizeEdited     bool
	parallelism         string
	parallelismEdited   bool
	cursor              int
	viewportTop         int
	viewportSize        int
	err                 error
	result              string
	configPath          string
	resumeFiles         []string
	resumeStates        []*CopyState
	progressMsg         string
	totalRows           int64
	copiedRows          int64
	currentLastID       int64
	progressPct         float64
	timeRemaining       string
	estimatedCompletion string
	copyInProgress      bool
	progressChan        chan CopyProgress
	cancelCopy          context.CancelFunc
	cancelling          bool
	confirmCancel       bool
	filterText          string
	filteredItems       []string
	confirmDelete       bool
	deleteIndex         int
	tableProgress       map[string]tableProgressInfo // Progress per table
}

// tableProgressInfo holds progress information for a single table
type tableProgressInfo struct {
	tableName     string
	maxID         int64
	currentLastID int64
	percentage    float64
	message       string
	timeRemaining string
}

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205")).
			MarginBottom(1)

	selectedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("170")).
			Bold(true)

	normalStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("252"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196")).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("42")).
			Bold(true)

	promptStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("86"))
)

func runInteractive() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}
	configPath := fmt.Sprintf("%s/.pg_service.conf", home)

	services, err := ParseServiceFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to parse service file: %w", err)
	}

	serviceNames := make([]string, 0, len(services))
	for name := range services {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)

	if len(serviceNames) == 0 {
		return fmt.Errorf("no services found in %s", configPath)
	}

	// Check for existing copy state files
	resumeFiles, _ := FindAllCopyStateFiles()
	var resumeStates []*CopyState
	for _, file := range resumeFiles {
		if state, err := LoadCopyState(file); err == nil {
			resumeStates = append(resumeStates, state)
		}
	}

	// Start at resume screen if there are existing copies, otherwise source screen
	startScreen := screenSource
	if len(resumeFiles) > 0 {
		startScreen = screenResume
	}

	m := model{
		screen:         startScreen,
		services:       services,
		serviceNames:   serviceNames,
		selectedTables: make(map[string]bool),
		tableConfigs:   make(map[string]*tableConfig),
		chunkSize:      "1000",
		parallelism:    "1",
		configPath:     configPath,
		viewportSize:   10, // Show 10 items at a time
		resumeFiles:    resumeFiles,
		resumeStates:   resumeStates,
		tableProgress:  make(map[string]tableProgressInfo),
	}

	p := tea.NewProgram(m)
	if _, err := p.Run(); err != nil {
		return err
	}

	return nil
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			// If copy in progress, cancel it first
			if m.copyInProgress && m.cancelCopy != nil {
				m.progressMsg = "Cancelling... waiting for workers to finish"
				m.cancelCopy()
			}
			return m, tea.Quit

		case "esc":
			// If copying, handle cancellation with confirmation
			if m.screen == screenCopying && m.copyInProgress && m.cancelCopy != nil {
				if !m.cancelling {
					if m.confirmCancel {
						// Second ESC - actually cancel
						m.progressMsg = "⏳ Cancelling... waiting for workers to finish safely"
						m.cancelling = true
						m.confirmCancel = false
						m.cancelCopy()
						return m, nil
					} else {
						// First ESC - ask for confirmation
						m.confirmCancel = true
						return m, nil
					}
				}
				// If already cancelling, ignore ESC
				return m, nil
			}
			// Otherwise treat as quit
			return m, tea.Quit

		case "up", "k":
			m.confirmDelete = false
			m.deleteIndex = -1
			m.confirmCancel = false
			if m.cursor > 0 {
				m.cursor--
				// Adjust viewport if cursor moves above visible area
				if m.cursor < m.viewportTop {
					m.viewportTop = m.cursor
				}
			}

		case "down", "j":
			m.confirmDelete = false
			m.deleteIndex = -1
			m.confirmCancel = false
			var maxItems int
			switch m.screen {
			case screenResume:
				maxItems = len(m.resumeFiles) + 1 // +1 for "Start new copy" option
			case screenSource, screenTarget:
				if len(m.filterText) > 0 {
					maxItems = len(m.filteredItems)
				} else {
					maxItems = len(m.serviceNames)
				}
			case screenTable:
				if len(m.filterText) > 0 {
					maxItems = len(m.filteredItems)
				} else {
					maxItems = len(m.tables)
				}
			default:
				maxItems = 0
			}

			if maxItems > 0 && m.cursor < maxItems-1 {
				m.cursor++
				// Adjust viewport if cursor moves below visible area
				if m.cursor >= m.viewportTop+m.viewportSize {
					m.viewportTop = m.cursor - m.viewportSize + 1
				}
			}

		case " ":
			m.confirmCancel = false
			// Toggle table selection
			if m.screen == screenTable {
				var tableName string
				if len(m.filterText) > 0 {
					tableName = m.filteredItems[m.cursor]
				} else {
					tableName = m.tables[m.cursor]
				}
				if m.selectedTables[tableName] {
					delete(m.selectedTables, tableName)
				} else {
					m.selectedTables[tableName] = true
				}
				m.err = nil
				return m, nil
			}

		case "x":
			m.confirmCancel = false
			// Delete state file on resume screen
			if m.screen == screenResume && m.cursor < len(m.resumeFiles) {
				if m.confirmDelete && m.deleteIndex == m.cursor {
					// Confirmed - delete the file
					fileToDelete := m.resumeFiles[m.cursor]
					if err := os.Remove(fileToDelete); err != nil {
						m.err = fmt.Errorf("failed to delete state file: %w", err)
						return m, nil
					}

					// Refresh the list
					resumeFiles, _ := FindAllCopyStateFiles()
					var resumeStates []*CopyState
					for _, file := range resumeFiles {
						if state, err := LoadCopyState(file); err == nil {
							resumeStates = append(resumeStates, state)
						}
					}
					m.resumeFiles = resumeFiles
					m.resumeStates = resumeStates

					// Reset cursor if needed
					if m.cursor >= len(m.resumeFiles) && len(m.resumeFiles) > 0 {
						m.cursor = len(m.resumeFiles) - 1
					}
					if len(m.resumeFiles) == 0 {
						m.cursor = 0
					}

					m.confirmDelete = false
					m.deleteIndex = -1
				} else {
					// Ask for confirmation
					m.confirmDelete = true
					m.deleteIndex = m.cursor
				}
			}

		case "enter":
			m.confirmCancel = false
			switch m.screen {
			case screenResume:
				m.confirmDelete = false
				m.deleteIndex = -1

				if m.cursor == len(m.resumeFiles) {
					// "Start new copy" option selected
					m.screen = screenSource
					m.cursor = 0
					m.viewportTop = 0
					m.filterText = ""
					m.filteredItems = nil
				} else {
					// Resume existing copy
					state := m.resumeStates[m.cursor]
					m.source = state.SourceService
					m.target = state.TargetService

					// Handle multiple tables in state
					selectedTables := make(map[string]bool)
					m.tablesToConfigure = []string{}
					m.tableConfigs = make(map[string]*tableConfig)

					for _, tableState := range state.Tables {
						selectedTables[tableState.TableName] = true
						m.tablesToConfigure = append(m.tablesToConfigure, tableState.TableName)
						m.tableConfigs[tableState.TableName] = &tableConfig{
							whereClause: tableState.WhereClause,
							primaryKey:  tableState.PrimaryKey,
							lastID:      fmt.Sprintf("%d", tableState.LastID),
						}
					}

					// For backward compatibility: if no tables, try old format
					if len(state.Tables) == 0 {
						// This shouldn't happen with migration, but handle gracefully
						m.screen = screenSource
						m.cursor = 0
						m.viewportTop = 0
						return m, nil
					}

					m.selectedTables = selectedTables
					// Set m.table to first table for backward compatibility
					if len(m.tablesToConfigure) > 0 {
						m.table = m.tablesToConfigure[0]
					}

					if state.ChunkSize > 0 {
						m.chunkSize = fmt.Sprintf("%d", state.ChunkSize)
					}
					if state.Parallelism > 0 {
						m.parallelism = fmt.Sprintf("%d", state.Parallelism)
					}
					m.screen = screenConfirm
					m.cursor = 0
					m.viewportTop = 0
				}

			case screenSource:
				if len(m.filterText) > 0 {
					m.source = m.filteredItems[m.cursor]
				} else {
					m.source = m.serviceNames[m.cursor]
				}
				m.screen = screenTarget
				m.cursor = 0
				m.viewportTop = 0
				m.filterText = ""
				m.filteredItems = nil

			case screenTarget:
				if len(m.filterText) > 0 {
					m.target = m.filteredItems[m.cursor]
				} else {
					m.target = m.serviceNames[m.cursor]
				}
				if m.target == m.source {
					m.err = fmt.Errorf("target must be different from source")
					return m, nil
				}
				m.err = nil

				// Fetch tables from source
				tables, err := fetchTables(m.services[m.source])
				if err != nil {
					m.err = err
					return m, nil
				}
				m.tables = tables
				m.selectedTables = make(map[string]bool)
				m.screen = screenTable
				m.cursor = 0
				m.viewportTop = 0
				m.filterText = ""
				m.filteredItems = nil

			case screenTable:
				// Check if at least one table is selected
				if len(m.selectedTables) == 0 {
					m.err = fmt.Errorf("please select at least one table (use spacebar to select)")
					return m, nil
				}
				m.err = nil

				// Set up per-table configuration
				m.tablesToConfigure = m.getSelectedTablesList()
				m.currentConfigIndex = 0
				for _, tableName := range m.tablesToConfigure {
					if m.tableConfigs[tableName] == nil {
						m.tableConfigs[tableName] = &tableConfig{
							primaryKey: "id",
							lastID:     "0",
						}
					}
				}

				m.screen = screenWhereClause
				m.cursor = 0
				m.viewportTop = 0

			case screenWhereClause:
				m.screen = screenPrimaryKey

			case screenPrimaryKey:
				m.screen = screenLastID

			case screenLastID:
				// Move to next table's configuration or proceed to chunk size
				m.currentConfigIndex++
				if m.currentConfigIndex < len(m.tablesToConfigure) {
					m.screen = screenWhereClause
				} else {
					m.screen = screenChunkSize
				}

			case screenChunkSize:
				m.screen = screenParallelism

			case screenParallelism:
				m.screen = screenConfirm

			case screenConfirm:
				m.screen = screenCopying
				m.copyInProgress = true
				m.progressMsg = "Initializing copy..."
				m.copiedRows = 0
				m.totalRows = 0
				m.progressPct = 0
				m.progressChan = make(chan CopyProgress, 100)
				m.tableProgress = make(map[string]tableProgressInfo) // Reset table progress
				return m, m.performCopy()

				// screenDone is no longer used - we stay on screenCopying after completion
			}

		case "\\":
			m.confirmCancel = false
			// Go back to previous screen
			switch m.screen {
			case screenTarget:
				m.screen = screenSource
				m.cursor = 0
				m.viewportTop = 0
			case screenTable:
				m.screen = screenTarget
				m.cursor = 0
				m.viewportTop = 0
			case screenWhereClause:
				// If configuring first table, go back to table selection
				// Otherwise, go back to previous table's lastID
				if m.currentConfigIndex == 0 {
					m.screen = screenTable
				} else {
					m.currentConfigIndex--
					m.screen = screenLastID
				}
				m.cursor = 0
				m.viewportTop = 0
			case screenPrimaryKey:
				m.screen = screenWhereClause
				m.cursor = 0
				m.viewportTop = 0
			case screenLastID:
				m.screen = screenPrimaryKey
				m.cursor = 0
				m.viewportTop = 0
			case screenChunkSize:
				// Go back to last table's lastID screen
				m.currentConfigIndex = len(m.tablesToConfigure) - 1
				m.screen = screenLastID
				m.cursor = 0
				m.viewportTop = 0
				m.chunkSizeEdited = false
			case screenParallelism:
				m.screen = screenChunkSize
				m.cursor = 0
				m.viewportTop = 0
				m.parallelismEdited = false
			case screenConfirm:
				m.screen = screenParallelism
				m.cursor = 0
				m.viewportTop = 0
			}

		case "backspace":
			// Handle backspace in text input fields and filters
			if m.screen == screenWhereClause {
				currentTable := m.tablesToConfigure[m.currentConfigIndex]
				cfg := m.tableConfigs[currentTable]
				if len(cfg.whereClause) > 0 {
					cfg.whereClause = cfg.whereClause[:len(cfg.whereClause)-1]
				}
			} else if m.screen == screenPrimaryKey {
				currentTable := m.tablesToConfigure[m.currentConfigIndex]
				cfg := m.tableConfigs[currentTable]
				if len(cfg.primaryKey) > 0 {
					cfg.primaryKey = cfg.primaryKey[:len(cfg.primaryKey)-1]
				}
			} else if m.screen == screenLastID {
				currentTable := m.tablesToConfigure[m.currentConfigIndex]
				cfg := m.tableConfigs[currentTable]
				if len(cfg.lastID) > 0 {
					cfg.lastID = cfg.lastID[:len(cfg.lastID)-1]
					if len(cfg.lastID) == 0 {
						cfg.lastID = "0" // Reset to default
					}
				}
			} else if m.screen == screenChunkSize {
				if len(m.chunkSize) > 0 {
					m.chunkSize = m.chunkSize[:len(m.chunkSize)-1]
					m.chunkSizeEdited = true
					if len(m.chunkSize) == 0 {
						m.chunkSize = "1000" // Reset to default
						m.chunkSizeEdited = false
					}
				}
			} else if m.screen == screenParallelism {
				if len(m.parallelism) > 0 {
					m.parallelism = m.parallelism[:len(m.parallelism)-1]
					m.parallelismEdited = true
					if len(m.parallelism) == 0 {
						m.parallelism = "1" // Reset to default
						m.parallelismEdited = false
					}
				}
			} else if m.screen == screenSource || m.screen == screenTarget || m.screen == screenTable {
				// Handle filter backspace
				if len(m.filterText) > 0 {
					m.filterText = m.filterText[:len(m.filterText)-1]
					m.cursor = 0
					m.viewportTop = 0
					m.updateFilter()
				}
			}

		default:
			// Handle text input for WHERE clause
			if m.screen == screenWhereClause {
				if len(msg.String()) == 1 {
					currentTable := m.tablesToConfigure[m.currentConfigIndex]
					cfg := m.tableConfigs[currentTable]
					// Allow most characters for WHERE clause
					if msg.String()[0] >= ' ' && msg.String()[0] <= '~' {
						cfg.whereClause += msg.String()
					}
				}
			}
			// Handle text input for primary key
			if m.screen == screenPrimaryKey {
				if len(msg.String()) == 1 && (msg.String()[0] >= 'a' && msg.String()[0] <= 'z' ||
					msg.String()[0] >= 'A' && msg.String()[0] <= 'Z' ||
					msg.String()[0] >= '0' && msg.String()[0] <= '9' ||
					msg.String()[0] == '_') {
					currentTable := m.tablesToConfigure[m.currentConfigIndex]
					cfg := m.tableConfigs[currentTable]
					cfg.primaryKey += msg.String()
				}
			}
			// Handle numeric input for last-id
			if m.screen == screenLastID {
				if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
					currentTable := m.tablesToConfigure[m.currentConfigIndex]
					cfg := m.tableConfigs[currentTable]
					// Only allow digits
					if cfg.lastID == "0" && len(cfg.lastID) == 1 {
						// Replace default "0" with first digit
						cfg.lastID = msg.String()
					} else {
						cfg.lastID += msg.String()
					}
				}
			}
			// Handle numeric input for chunk-size
			if m.screen == screenChunkSize {
				if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
					// Only allow digits
					if !m.chunkSizeEdited && m.chunkSize == "1000" {
						// Replace default "1000" with first digit
						m.chunkSize = msg.String()
						m.chunkSizeEdited = true
					} else {
						m.chunkSize += msg.String()
						m.chunkSizeEdited = true
					}
				}
			}
			// Handle numeric input for parallelism
			if m.screen == screenParallelism {
				if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
					// Only allow digits
					if !m.parallelismEdited && m.parallelism == "1" {
						// Replace default "1" with first digit
						m.parallelism = msg.String()
						m.parallelismEdited = true
					} else {
						m.parallelism += msg.String()
						m.parallelismEdited = true
					}
				}
			}
			// Handle filter text input for source/target/table screens
			if m.screen == screenSource || m.screen == screenTarget || m.screen == screenTable {
				if len(msg.String()) == 1 && (msg.String()[0] >= 'a' && msg.String()[0] <= 'z' ||
					msg.String()[0] >= 'A' && msg.String()[0] <= 'Z' ||
					msg.String()[0] >= '0' && msg.String()[0] <= '9' ||
					msg.String()[0] == '_' || msg.String()[0] == '-') {
					m.filterText += msg.String()
					m.cursor = 0
					m.viewportTop = 0
					m.updateFilter()
				}
			}
		}

	case copyProgressMsg:
		m.progressMsg = msg.message
		m.totalRows = msg.totalRows
		m.copiedRows = msg.copiedRows
		m.currentLastID = msg.lastID
		m.progressPct = msg.percentage
		m.timeRemaining = msg.timeRemaining
		m.estimatedCompletion = msg.completion

		// Update per-table progress
		if msg.tableName != "" {
			if _, exists := m.tableProgress[msg.tableName]; !exists {
				m.tableProgress[msg.tableName] = tableProgressInfo{tableName: msg.tableName}
			}
			progress := m.tableProgress[msg.tableName]
			progress.maxID = msg.totalRows
			progress.currentLastID = msg.lastID
			progress.percentage = msg.percentage
			progress.message = msg.message
			progress.timeRemaining = msg.timeRemaining
			m.tableProgress[msg.tableName] = progress
		}

		// Keep listening for more progress updates
		return m, waitForProgress(m.progressChan)

	case copyResultMsg:
		m.result = string(msg)
		m.progressMsg = string(msg)
		m.copyInProgress = false
		m.cancelCopy = nil
		// Stay on screenCopying to show completion message
		return m, nil

	case copyErrorMsg:
		m.err = error(msg)
		m.progressMsg = fmt.Sprintf("Error: %v", error(msg))
		m.copyInProgress = false
		m.cancelCopy = nil
		// Stay on screenCopying to show error message
		return m, nil
	}

	return m, nil
}

func (m *model) getSelectedTablesList() []string {
	var selected []string
	for table := range m.selectedTables {
		selected = append(selected, table)
	}
	sort.Strings(selected)
	return selected
}

func (m *model) updateFilter() {
	var sourceList []string
	switch m.screen {
	case screenSource, screenTarget:
		sourceList = m.serviceNames
	case screenTable:
		sourceList = m.tables
	default:
		return
	}

	if m.filterText == "" {
		m.filteredItems = nil
		return
	}

	m.filteredItems = nil
	filterLower := strings.ToLower(m.filterText)
	for _, item := range sourceList {
		if strings.Contains(strings.ToLower(item), filterLower) {
			m.filteredItems = append(m.filteredItems, item)
		}
	}
}

func (m model) View() string {
	var s strings.Builder

	s.WriteString(titleStyle.Render("psc"))
	s.WriteString("\n")

	switch m.screen {
	case screenResume:
		s.WriteString(promptStyle.Render("Resume existing copy or start new?"))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("(%d existing operations: x to delete)", len(m.resumeFiles))))
		s.WriteString("\n\n")

		// Show scroll indicator at top
		if m.viewportTop > 0 {
			s.WriteString(normalStyle.Render("  ⬆ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more above)", m.viewportTop)))
			s.WriteString("\n")
		}

		// Show visible items
		totalItems := len(m.resumeFiles) + 1 // +1 for "Start new copy"
		start := m.viewportTop
		end := m.viewportTop + m.viewportSize
		if end > totalItems {
			end = totalItems
		}

		for i := start; i < end; i++ {
			if i == len(m.resumeFiles) {
				// "Start new copy" option
				if i == m.cursor {
					s.WriteString(selectedStyle.Render("▸ ✨ Start new copy"))
				} else {
					s.WriteString(normalStyle.Render("  ✨ Start new copy"))
				}
				s.WriteString("\n")
			} else {
				// Existing copy operation
				state := m.resumeStates[i]
				chunkInfo := ""
				if state.ChunkSize > 0 {
					chunkInfo = fmt.Sprintf(", chunk: %d", state.ChunkSize)
				}
				parallelInfo := ""
				if state.Parallelism > 1 {
					parallelInfo = fmt.Sprintf(", workers: %d", state.Parallelism)
				}

				// Display table info
				var label string
				if len(state.Tables) == 0 {
					// Old format - shouldn't happen with migration but handle gracefully
					label = fmt.Sprintf("📄 %s → %s: (no tables)", state.SourceService, state.TargetService)
				} else if len(state.Tables) == 1 {
					// Single table
					tableState := state.Tables[0]
					label = fmt.Sprintf("📄 %s → %s: %s (last ID: %d%s%s)",
						state.SourceService, state.TargetService, tableState.TableName, tableState.LastID, chunkInfo, parallelInfo)
				} else {
					// Multiple tables
					label = fmt.Sprintf("📄 %s → %s: %d tables%s%s",
						state.SourceService, state.TargetService, len(state.Tables), chunkInfo, parallelInfo)
				}

				if m.confirmDelete && m.deleteIndex == i {
					// Show delete confirmation
					s.WriteString(errorStyle.Render("▸ ⚠️  Press 'x' again to confirm deletion"))
				} else if i == m.cursor {
					s.WriteString(selectedStyle.Render("▸ " + label))
				} else {
					s.WriteString(normalStyle.Render("  " + label))
				}
				s.WriteString("\n")
			}
		}

		// Show scroll indicator at bottom
		if end < totalItems {
			s.WriteString(normalStyle.Render("  ⬇ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more below)", totalItems-end)))
			s.WriteString("\n")
		}

	case screenSource:
		s.WriteString(promptStyle.Render("Select source service:"))
		s.WriteString("\n")

		displayList := m.serviceNames
		if len(m.filterText) > 0 {
			displayList = m.filteredItems
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d of %d services) Filter: ", len(displayList), len(m.serviceNames))))
			s.WriteString(selectedStyle.Render(m.filterText))
		} else {
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d services)", len(m.serviceNames))))
		}
		s.WriteString("\n\n")

		// Show scroll indicator at top
		if m.viewportTop > 0 {
			s.WriteString(normalStyle.Render("  ⬆ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more above)", m.viewportTop)))
			s.WriteString("\n")
		}

		// Show visible items
		start := m.viewportTop
		end := m.viewportTop + m.viewportSize
		if end > len(displayList) {
			end = len(displayList)
		}

		for i := start; i < end; i++ {
			name := displayList[i]
			if i == m.cursor {
				s.WriteString(selectedStyle.Render("▸ " + name))
			} else {
				s.WriteString(normalStyle.Render("  " + name))
			}
			s.WriteString("\n")
		}

		// Show scroll indicator at bottom
		if end < len(displayList) {
			s.WriteString(normalStyle.Render("  ⬇ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more below)", len(displayList)-end)))
			s.WriteString("\n")
		}

	case screenTarget:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s", m.source)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Select target service:"))
		s.WriteString("\n")

		displayList := m.serviceNames
		if len(m.filterText) > 0 {
			displayList = m.filteredItems
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d of %d services) Filter: ", len(displayList), len(m.serviceNames))))
			s.WriteString(selectedStyle.Render(m.filterText))
		} else {
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d services)", len(m.serviceNames))))
		}
		s.WriteString("\n\n")

		// Show scroll indicator at top
		if m.viewportTop > 0 {
			s.WriteString(normalStyle.Render("  ⬆ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more above)", m.viewportTop)))
			s.WriteString("\n")
		}

		// Show visible items
		start := m.viewportTop
		end := m.viewportTop + m.viewportSize
		if end > len(displayList) {
			end = len(displayList)
		}

		for i := start; i < end; i++ {
			name := displayList[i]
			if i == m.cursor {
				s.WriteString(selectedStyle.Render("▸ " + name))
			} else {
				s.WriteString(normalStyle.Render("  " + name))
			}
			s.WriteString("\n")
		}

		// Show scroll indicator at bottom
		if end < len(displayList) {
			s.WriteString(normalStyle.Render("  ⬇ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more below)", len(displayList)-end)))
			s.WriteString("\n")
		}

		if m.err != nil {
			s.WriteString("\n")
			s.WriteString(errorStyle.Render(m.err.Error()))
		}

	case screenTable:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Select tables to copy (spacebar to toggle, Enter when done):"))
		s.WriteString("\n")

		displayList := m.tables
		if len(m.filterText) > 0 {
			displayList = m.filteredItems
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d of %d tables) Filter: ", len(displayList), len(m.tables))))
			s.WriteString(selectedStyle.Render(m.filterText))
		} else {
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d tables)", len(m.tables))))
		}
		selectedCount := len(m.selectedTables)
		if selectedCount > 0 {
			s.WriteString(normalStyle.Render(fmt.Sprintf(" • %d selected", selectedCount)))
		}
		s.WriteString("\n\n")

		if len(m.tables) == 0 {
			s.WriteString(errorStyle.Render("No tables found or error fetching tables"))
		} else {
			// Show scroll indicator at top
			if m.viewportTop > 0 {
				s.WriteString(normalStyle.Render("  ⬆ ... "))
				s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more above)", m.viewportTop)))
				s.WriteString("\n")
			}

			// Show visible items
			start := m.viewportTop
			end := m.viewportTop + m.viewportSize
			if end > len(displayList) {
				end = len(displayList)
			}

			for i := start; i < end; i++ {
				name := displayList[i]
				selected := m.selectedTables[name]
				checkbox := "☐ "
				if selected {
					checkbox = "☑ "
				}
				if i == m.cursor {
					s.WriteString(selectedStyle.Render("▸ " + checkbox + name))
				} else {
					if selected {
						s.WriteString(selectedStyle.Render("  " + checkbox + name))
					} else {
						s.WriteString(normalStyle.Render("  " + checkbox + name))
					}
				}
				s.WriteString("\n")
			}

			// Show scroll indicator at bottom
			if end < len(displayList) {
				s.WriteString(normalStyle.Render("  ⬇ ... "))
				s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more below)", len(displayList)-end)))
				s.WriteString("\n")
			}
		}

		if m.err != nil {
			s.WriteString("\n")
			s.WriteString(errorStyle.Render(m.err.Error()))
		}

	case screenWhereClause:
		currentTable := m.tablesToConfigure[m.currentConfigIndex]
		cfg := m.tableConfigs[currentTable]
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Configuring table %d of %d: %s", m.currentConfigIndex+1, len(m.tablesToConfigure), currentTable)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Enter WHERE clause (optional, leave empty to copy all rows, ex. 'id < 2000'):"))
		s.WriteString("\n\n")
		if cfg.whereClause == "" {
			s.WriteString(normalStyle.Render("[No filter - copying all rows]"))
		} else {
			s.WriteString(selectedStyle.Render(cfg.whereClause))
		}
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue"))

	case screenPrimaryKey:
		currentTable := m.tablesToConfigure[m.currentConfigIndex]
		cfg := m.tableConfigs[currentTable]
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Configuring table %d of %d: %s", m.currentConfigIndex+1, len(m.tablesToConfigure), currentTable)))
		s.WriteString("\n")
		if cfg.whereClause != "" {
			s.WriteString(normalStyle.Render(fmt.Sprintf("WHERE: %s", cfg.whereClause)))
			s.WriteString("\n")
		}
		s.WriteString("\n")
		s.WriteString(promptStyle.Render("Enter primary key column name:"))
		s.WriteString("\n\n")
		s.WriteString(selectedStyle.Render(cfg.primaryKey))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue"))

	case screenLastID:
		currentTable := m.tablesToConfigure[m.currentConfigIndex]
		cfg := m.tableConfigs[currentTable]
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Configuring table %d of %d: %s", m.currentConfigIndex+1, len(m.tablesToConfigure), currentTable)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Primary Key: %s", cfg.primaryKey)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Enter starting ID (for resuming copy):"))
		s.WriteString("\n\n")
		s.WriteString(selectedStyle.Render(cfg.lastID))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue (0 = start from beginning)"))

	case screenChunkSize:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		selectedTables := m.getSelectedTablesList()
		if len(selectedTables) == 1 {
			s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", selectedTables[0])))
		} else {
			s.WriteString(normalStyle.Render(fmt.Sprintf("Tables: %d selected", len(selectedTables))))
		}
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Enter chunk size (rows per batch):"))
		s.WriteString("\n\n")
		s.WriteString(selectedStyle.Render(m.chunkSize))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue (default: 1000)"))

	case screenParallelism:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		selectedTables := m.getSelectedTablesList()
		if len(selectedTables) == 1 {
			s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", selectedTables[0])))
		} else {
			s.WriteString(normalStyle.Render(fmt.Sprintf("Tables: %d selected", len(selectedTables))))
		}
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Chunk Size: %s", m.chunkSize)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Enter parallelism (concurrent workers):"))
		s.WriteString("\n\n")
		s.WriteString(selectedStyle.Render(m.parallelism))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue (default: 1, max recommended: 10)"))

	case screenConfirm:
		s.WriteString(titleStyle.Render("Confirm Copy Operation"))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source:      %s", m.source)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Target:      %s", m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Chunk Size:  %s rows", m.chunkSize)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Parallelism: %s workers", m.parallelism)))
		s.WriteString("\n\n")

		// Show per-table configurations
		selectedTables := m.getSelectedTablesList()
		if len(selectedTables) == 1 {
			// Show single table details inline
			table := selectedTables[0]
			cfg := m.tableConfigs[table]
			s.WriteString(normalStyle.Render(fmt.Sprintf("Table:       %s", table)))
			s.WriteString("\n")
			s.WriteString(normalStyle.Render(fmt.Sprintf("Primary Key: %s", cfg.primaryKey)))
			s.WriteString("\n")
			s.WriteString(normalStyle.Render(fmt.Sprintf("Starting ID: %s", cfg.lastID)))
			s.WriteString("\n")
			if cfg.whereClause != "" {
				s.WriteString(normalStyle.Render(fmt.Sprintf("WHERE:       %s", cfg.whereClause)))
				s.WriteString("\n")
			}
		} else {
			// Show multiple tables with their configs
			s.WriteString(normalStyle.Render(fmt.Sprintf("Tables:      %d tables", len(selectedTables))))
			s.WriteString("\n\n")
			for i, table := range selectedTables {
				if i >= 5 {
					s.WriteString(normalStyle.Render(fmt.Sprintf("... and %d more tables", len(selectedTables)-5)))
					s.WriteString("\n")
					break
				}
				cfg := m.tableConfigs[table]
				s.WriteString(normalStyle.Render(fmt.Sprintf("  %d. %s", i+1, table)))
				s.WriteString("\n")
				s.WriteString(normalStyle.Render(fmt.Sprintf("     pk=%s, start=%s", cfg.primaryKey, cfg.lastID)))
				if cfg.whereClause != "" {
					s.WriteString(normalStyle.Render(fmt.Sprintf(", where=%s", cfg.whereClause)))
				}
				s.WriteString("\n")
			}
		}
		s.WriteString("\n")
		s.WriteString(promptStyle.Render("Press Enter to start copy, \\ to go back"))

	case screenCopying:
		// Title with progress message on the same line, right-aligned
		titleText := titleStyle.Render("Copying Data")
		s.WriteString(titleText)
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render(m.progressMsg))
		s.WriteString("\n\n")

		selectedTables := m.getSelectedTablesList()

		// Show progress bars for each table
		if len(selectedTables) > 1 {
			// Multiple tables - show progress bar for each in table format
			// Find max table name length and max ID string length for alignment
			maxNameLen := 0
			maxIDStrLen := 0
			for _, tableName := range selectedTables {
				if len(tableName) > maxNameLen {
					maxNameLen = len(tableName)
				}
				if progress, exists := m.tableProgress[tableName]; exists && progress.maxID > 0 {
					idStr := fmt.Sprintf("%s/%s", formatNumber(progress.currentLastID), formatNumber(progress.maxID))
					if len(idStr) > maxIDStrLen {
						maxIDStrLen = len(idStr)
					}
				}
			}
			if maxNameLen < 8 {
				maxNameLen = 8 // minimum width
			}

			barWidth := 40

			// Format each table row with aligned columns
			for _, tableName := range selectedTables {
				progress, exists := m.tableProgress[tableName]
				if !exists {
					// Table hasn't started yet - show pending
					namePadded := fmt.Sprintf("%-*s", maxNameLen, tableName)
					s.WriteString(normalStyle.Render(fmt.Sprintf("⏳ %s  Waiting...", namePadded)))
					s.WriteString("\n")
					continue
				}

				// Progress bar for this table
				filled := int(float64(barWidth) * progress.percentage / 100.0)
				if filled > barWidth {
					filled = barWidth
				}
				bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

				// Status icon
				status := "🔄"
				if progress.percentage >= 100 {
					status = "✅"
				}

				// Format as table: Status | Name | Bar | Percentage | IDs | ETA
				namePadded := fmt.Sprintf("%-*s", maxNameLen, tableName)
				percentageStr := fmt.Sprintf("%5.1f%%", progress.percentage)

				// Build the row with aligned columns
				row := fmt.Sprintf("%s %s  [%s]  %s", status, namePadded, bar, percentageStr)

				// Add ID counts if available (left-aligned for consistent column alignment)
				if progress.maxID > 0 {
					idStr := fmt.Sprintf("%s/%s", formatNumber(progress.currentLastID), formatNumber(progress.maxID))
					if maxIDStrLen > 0 {
						row += fmt.Sprintf("  %-*s", maxIDStrLen, idStr)
					} else {
						row += fmt.Sprintf("  %s", idStr)
					}

					// Add ETA if available
					if progress.timeRemaining != "" {
						row += fmt.Sprintf("  ETA: %s", progress.timeRemaining)
					}
				}

				s.WriteString(row)
				s.WriteString("\n")
			}
		} else if len(selectedTables) == 1 {
			// Single table - show detailed progress
			tableName := selectedTables[0]
			s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", tableName)))
			s.WriteString("\n\n")

			if m.totalRows > 0 {
				// Progress bar
				barWidth := 50
				filled := int(float64(barWidth) * m.progressPct / 100.0)
				if filled > barWidth {
					filled = barWidth
				}
				bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
				s.WriteString(selectedStyle.Render(fmt.Sprintf("[%s] %.1f%%", bar, m.progressPct)))
				s.WriteString("\n\n")
				s.WriteString(normalStyle.Render(fmt.Sprintf("Max ID:         %s", formatNumber(m.totalRows))))
				s.WriteString("\n")
				s.WriteString(normalStyle.Render(fmt.Sprintf("Next ID:         %s", formatNumber(m.currentLastID))))
				s.WriteString("\n")
				if m.timeRemaining != "" {
					s.WriteString(normalStyle.Render(fmt.Sprintf("Time Left:       %s", m.timeRemaining)))
					s.WriteString("\n")
				}
				s.WriteString("\n")
			}

			// Progress message is now shown in header, don't duplicate here
			s.WriteString("\n")
		}

		// Show appropriate message based on state
		if !m.copyInProgress {
			// Copy completed or errored
			if m.err != nil {
				s.WriteString("\n")
				s.WriteString(errorStyle.Render(m.progressMsg))
			} else {
				s.WriteString("\n")
				s.WriteString(successStyle.Render(m.progressMsg))
			}
			s.WriteString("\n")
			s.WriteString(normalStyle.Render("Press esc to quit"))
		} else if m.cancelling {
			s.WriteString(errorStyle.Render("⏳ Cancelling... please wait for workers to finish safely"))
		} else if m.confirmCancel {
			s.WriteString(errorStyle.Render("⚠️  Press ESC again to confirm cancellation"))
		} else {
			s.WriteString(normalStyle.Render("Press ESC to cancel (copy will resume from last checkpoint)"))
		}
	}

	if m.screen != screenCopying {
		s.WriteString("\n\n")
		if m.screen == screenTable {
			s.WriteString(normalStyle.Render("↑/↓: navigate • Space: toggle • Enter: continue • \\: go back • esc: quit"))
		} else {
			s.WriteString(normalStyle.Render("↑/↓: navigate • Enter: select • \\: go back • esc: quit"))
		}
	}

	return s.String()
}

type copyResultMsg string
type copyErrorMsg error
type copyProgressMsg struct {
	tableName     string
	message       string
	totalRows     int64
	copiedRows    int64
	lastID        int64
	percentage    float64
	timeRemaining string
	completion    string
}

func (m *model) performCopy() tea.Cmd {
	sourceConfig := m.services[m.source]
	targetConfig := m.services[m.target]

	// Parse chunkSize
	var chunkSize int64 = 1000
	if m.chunkSize != "" {
		if parsed, err := strconv.ParseInt(m.chunkSize, 10, 64); err == nil {
			chunkSize = parsed
		}
	}

	// Parse parallelism
	var parallelism int = 1
	if m.parallelism != "" {
		if parsed, err := strconv.Atoi(m.parallelism); err == nil {
			parallelism = parsed
		}
	}

	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	m.cancelCopy = cancel

	// Get selected tables in sorted order
	selectedTables := m.getSelectedTablesList()

	// Initialize state file with all tables before starting any copy operations
	tableInfos := make([]struct {
		Name        string
		WhereClause string
		PrimaryKey  string
		LastID      int64
	}, 0, len(selectedTables))

	for _, tableName := range selectedTables {
		cfg := m.tableConfigs[tableName]
		var lastID int64 = 0
		if cfg.lastID != "" {
			if parsed, err := strconv.ParseInt(cfg.lastID, 10, 64); err == nil {
				lastID = parsed
			}
		}
		tableInfos = append(tableInfos, struct {
			Name        string
			WhereClause string
			PrimaryKey  string
			LastID      int64
		}{
			Name:        tableName,
			WhereClause: cfg.whereClause,
			PrimaryKey:  cfg.primaryKey,
			LastID:      lastID,
		})
	}

	stateFile, err := InitializeMultiTableState(m.source, m.target, tableInfos, chunkSize, parallelism)
	if err != nil {
		m.progressChan <- CopyProgress{Error: fmt.Errorf("failed to initialize state file: %w", err)}
		close(m.progressChan)
		return waitForProgress(m.progressChan)
	}

	// Start copy in goroutine
	go func() {
		for i, tableName := range selectedTables {
			// Get per-table config
			cfg := m.tableConfigs[tableName]

			// Parse this table's lastID
			var lastID int64 = 0
			if cfg.lastID != "" {
				if parsed, err := strconv.ParseInt(cfg.lastID, 10, 64); err == nil {
					lastID = parsed
				}
			}

			// Send message indicating which table we're copying
			progressMsg := fmt.Sprintf("Copying table %d of %d: %s", i+1, len(selectedTables), tableName)
			m.progressChan <- CopyProgress{TableName: tableName, Message: progressMsg}

			err := CopyTableWithProgress(ctx, m.source, m.target, sourceConfig, targetConfig, tableName, cfg.whereClause, cfg.primaryKey, lastID, chunkSize, parallelism, m.progressChan)
			if err != nil {
				m.progressChan <- CopyProgress{Error: fmt.Errorf("failed to copy table %s: %w", tableName, err)}
				close(m.progressChan)
				return
			}

			// If cancelled, stop copying remaining tables
			select {
			case <-ctx.Done():
				m.progressChan <- CopyProgress{Error: fmt.Errorf("copy cancelled")}
				close(m.progressChan)
				return
			default:
			}
		}

		// All tables copied successfully - move state file to completed
		if err := moveStateFileToCompleted(stateFile); err != nil {
			// Log warning but don't fail
			m.progressChan <- CopyProgress{Message: fmt.Sprintf("Warning: failed to move state file to completed: %v", err)}
		}
		m.progressChan <- CopyProgress{Done: true, Message: fmt.Sprintf("Successfully copied %d table(s)", len(selectedTables))}
		close(m.progressChan)
	}()

	// Return a command that listens to progress
	return waitForProgress(m.progressChan)
}

func waitForProgress(progressChan chan CopyProgress) tea.Cmd {
	return func() tea.Msg {
		progress, ok := <-progressChan
		if !ok {
			// Channel closed
			return copyResultMsg("Table copied successfully!")
		}

		if progress.Error != nil {
			return copyErrorMsg(progress.Error)
		}

		// Only treat Done: true as completion (but CopyTableWithProgress no longer sends this)
		// Instead, completion is handled by the goroutine closing the channel after all tables
		if progress.Done {
			if progress.Message != "" {
				return copyResultMsg(progress.Message)
			}
			return copyResultMsg("Tables copied successfully!")
		}

		return copyProgressMsg{
			tableName:     progress.TableName,
			message:       progress.Message,
			totalRows:     progress.TotalRows,
			copiedRows:    progress.CopiedRows,
			lastID:        progress.LastID,
			percentage:    progress.Percentage,
			timeRemaining: progress.EstimatedTimeRemaining,
			completion:    progress.EstimatedCompletion,
		}
	}
}

func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

func fetchTables(config ServiceConfig) ([]string, error) {
	// Try with SSL first
	db, err := sql.Open("postgres", config.ConnectionString())
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		// Check if it's an SSL error and retry without SSL
		if strings.Contains(err.Error(), "SSL is not enabled on the server") {
			db.Close()
			// Retry without SSL
			db, err = sql.Open("postgres", config.ConnectionStringWithSSL("disable"))
			if err != nil {
				return nil, err
			}
			err = db.Ping()
			if err != nil {
				db.Close()
				return nil, err
			}
		} else {
			db.Close()
			return nil, err
		}
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT tablename 
		FROM pg_tables 
		WHERE schemaname = 'public'
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Strings(tables)
	return tables, nil
}
