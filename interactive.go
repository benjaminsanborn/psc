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

type model struct {
	screen              screen
	services            map[string]ServiceConfig
	serviceNames        []string
	source              string
	target              string
	table               string
	tables              []string
	whereClause         string
	primaryKey          string
	lastID              string
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
		screen:       startScreen,
		services:     services,
		serviceNames: serviceNames,
		primaryKey:   "id",
		lastID:       "0",
		chunkSize:    "1000",
		parallelism:  "1",
		configPath:   configPath,
		viewportSize: 10, // Show 10 items at a time
		resumeFiles:  resumeFiles,
		resumeStates: resumeStates,
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
					m.table = state.TableName
					m.whereClause = state.WhereClause
					m.primaryKey = state.PrimaryKey
					m.lastID = fmt.Sprintf("%d", state.LastID)
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
				m.screen = screenTable
				m.cursor = 0
				m.viewportTop = 0
				m.filterText = ""
				m.filteredItems = nil

			case screenTable:
				if len(m.filterText) > 0 {
					m.table = m.filteredItems[m.cursor]
				} else {
					m.table = m.tables[m.cursor]
				}
				m.screen = screenWhereClause
				m.cursor = 0
				m.viewportTop = 0

			case screenWhereClause:
				m.screen = screenPrimaryKey

			case screenPrimaryKey:
				m.screen = screenLastID

			case screenLastID:
				m.screen = screenChunkSize

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
				return m, m.performCopy()

			case screenDone:
				return m, tea.Quit
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
				m.screen = screenTable
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
				if len(m.whereClause) > 0 {
					m.whereClause = m.whereClause[:len(m.whereClause)-1]
				}
			} else if m.screen == screenPrimaryKey {
				if len(m.primaryKey) > 0 {
					m.primaryKey = m.primaryKey[:len(m.primaryKey)-1]
				}
			} else if m.screen == screenLastID {
				if len(m.lastID) > 0 {
					m.lastID = m.lastID[:len(m.lastID)-1]
					if len(m.lastID) == 0 {
						m.lastID = "0" // Reset to default
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
					// Allow most characters for WHERE clause
					if msg.String()[0] >= ' ' && msg.String()[0] <= '~' {
						m.whereClause += msg.String()
					}
				}
			}
			// Handle text input for primary key
			if m.screen == screenPrimaryKey {
				if len(msg.String()) == 1 && (msg.String()[0] >= 'a' && msg.String()[0] <= 'z' ||
					msg.String()[0] >= 'A' && msg.String()[0] <= 'Z' ||
					msg.String()[0] >= '0' && msg.String()[0] <= '9' ||
					msg.String()[0] == '_') {
					m.primaryKey += msg.String()
				}
			}
			// Handle numeric input for last-id
			if m.screen == screenLastID {
				if len(msg.String()) == 1 && msg.String()[0] >= '0' && msg.String()[0] <= '9' {
					// Only allow digits
					if m.lastID == "0" && len(m.lastID) == 1 {
						// Replace default "0" with first digit
						m.lastID = msg.String()
					} else {
						m.lastID += msg.String()
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
		// Keep listening for more progress updates
		return m, waitForProgress(m.progressChan)

	case copyResultMsg:
		m.result = string(msg)
		m.copyInProgress = false
		m.cancelCopy = nil
		m.screen = screenDone
		return m, nil

	case copyErrorMsg:
		m.err = error(msg)
		m.copyInProgress = false
		m.cancelCopy = nil
		m.screen = screenDone
		return m, nil
	}

	return m, nil
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
				label := fmt.Sprintf("📄 %s → %s: %s (last ID: %d%s%s)",
					state.SourceService, state.TargetService, state.TableName, state.LastID, chunkInfo, parallelInfo)

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
		s.WriteString(promptStyle.Render("Select table to copy:"))
		s.WriteString("\n")

		displayList := m.tables
		if len(m.filterText) > 0 {
			displayList = m.filteredItems
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d of %d tables) Filter: ", len(displayList), len(m.tables))))
			s.WriteString(selectedStyle.Render(m.filterText))
		} else {
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d tables)", len(m.tables))))
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
		}

		if m.err != nil {
			s.WriteString("\n")
			s.WriteString(errorStyle.Render(m.err.Error()))
		}

	case screenWhereClause:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", m.table)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Enter WHERE clause (optional, leave empty to copy all rows, ex. 'id < 2000'):"))
		s.WriteString("\n\n")
		if m.whereClause == "" {
			s.WriteString(normalStyle.Render("[No filter - copying all rows]"))
		} else {
			s.WriteString(selectedStyle.Render(m.whereClause))
		}
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue"))

	case screenPrimaryKey:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", m.table)))
		s.WriteString("\n")
		if m.whereClause != "" {
			s.WriteString(normalStyle.Render(fmt.Sprintf("WHERE: %s", m.whereClause)))
			s.WriteString("\n")
		}
		s.WriteString("\n")
		s.WriteString(promptStyle.Render("Enter primary key column name:"))
		s.WriteString("\n\n")
		s.WriteString(selectedStyle.Render(m.primaryKey))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue"))

	case screenLastID:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", m.table)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Primary Key: %s", m.primaryKey)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Enter starting ID (for resuming copy):"))
		s.WriteString("\n\n")
		s.WriteString(selectedStyle.Render(m.lastID))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue (0 = start from beginning)"))

	case screenChunkSize:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", m.table)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Primary Key: %s", m.primaryKey)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Starting ID: %s", m.lastID)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Enter chunk size (rows per batch):"))
		s.WriteString("\n\n")
		s.WriteString(selectedStyle.Render(m.chunkSize))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Press Enter to continue (default: 1000)"))

	case screenParallelism:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", m.table)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Primary Key: %s", m.primaryKey)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Starting ID: %s", m.lastID)))
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
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table:       %s", m.table)))
		s.WriteString("\n")
		if m.whereClause != "" {
			s.WriteString(normalStyle.Render(fmt.Sprintf("WHERE:       %s", m.whereClause)))
			s.WriteString("\n")
		}
		s.WriteString(normalStyle.Render(fmt.Sprintf("Primary Key: %s", m.primaryKey)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Starting ID: %s", m.lastID)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Chunk Size:  %s rows", m.chunkSize)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Parallelism: %s workers", m.parallelism)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Press Enter to start copy, \\ to go back"))

	case screenCopying:
		s.WriteString(titleStyle.Render("Copying Data"))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", m.table)))
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
			s.WriteString(normalStyle.Render(fmt.Sprintf("Estimated Rows:  %s", formatNumber(m.totalRows))))
			s.WriteString("\n")
			s.WriteString(normalStyle.Render(fmt.Sprintf("Next ID:         %s", formatNumber(m.currentLastID))))
			s.WriteString("\n")
			if m.timeRemaining != "" {
				s.WriteString(normalStyle.Render(fmt.Sprintf("Time Left:       %s", m.timeRemaining)))
				s.WriteString("\n")
			}
			s.WriteString("\n")
		}

		s.WriteString(promptStyle.Render(m.progressMsg))
		s.WriteString("\n\n")

		// Show appropriate message based on state
		if m.cancelling {
			s.WriteString(errorStyle.Render("⏳ Cancelling... please wait for workers to finish safely"))
		} else if m.confirmCancel {
			s.WriteString(errorStyle.Render("⚠️  Press ESC again to confirm cancellation"))
		} else {
			s.WriteString(normalStyle.Render("Press ESC to cancel (copy will resume from last checkpoint)"))
		}

	case screenDone:
		if m.err != nil {
			s.WriteString(errorStyle.Render(m.err.Error()))
		} else {
			s.WriteString(normalStyle.Render(m.result))
		}

		// Show final statistics
		if m.totalRows > 0 {
			s.WriteString("\n\n")
			s.WriteString(normalStyle.Render("Final Statistics:"))
			s.WriteString("\n")

			// Progress bar
			barWidth := 50
			filled := int(float64(barWidth) * m.progressPct / 100.0)
			if filled > barWidth {
				filled = barWidth
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
			s.WriteString(selectedStyle.Render(fmt.Sprintf("[%s] %.1f%%", bar, m.progressPct)))
			s.WriteString("\n\n")

			s.WriteString(normalStyle.Render(fmt.Sprintf("Estimated Rows: %s", formatNumber(m.totalRows))))
			s.WriteString("\n")
			s.WriteString(normalStyle.Render(fmt.Sprintf("Copied Rows:    %s", formatNumber(m.copiedRows))))
			s.WriteString("\n")
			s.WriteString(normalStyle.Render(fmt.Sprintf("Last Copied ID: %s", formatNumber(m.currentLastID-1))))
			s.WriteString("\n")
		}

		s.WriteString("\n")
		s.WriteString(normalStyle.Render("Press esc to quit"))
	}

	if m.screen != screenDone && m.screen != screenCopying {
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("↑/↓: navigate • Enter: select • \\: go back • esc: quit"))
	}

	return s.String()
}

type copyResultMsg string
type copyErrorMsg error
type copyProgressMsg struct {
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

	// Parse lastID
	var lastID int64 = 1
	if m.lastID != "" {
		if parsed, err := strconv.ParseInt(m.lastID, 10, 64); err == nil {
			lastID = parsed
		}
	}

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

	// Start copy in goroutine
	go func() {
		err := CopyTableWithProgress(ctx, m.source, m.target, sourceConfig, targetConfig, m.table, m.whereClause, m.primaryKey, lastID, chunkSize, parallelism, m.progressChan)
		if err != nil {
			m.progressChan <- CopyProgress{Error: err}
		}
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

		if progress.Done {
			return copyResultMsg("Table copied successfully!")
		}

		return copyProgressMsg{
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
