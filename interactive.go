package main

import (
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
	screenSource screen = iota
	screenTarget
	screenTable
	screenPrimaryKey
	screenLastID
	screenConfirm
	screenCopying
	screenDone
)

type model struct {
	screen       screen
	services     map[string]ServiceConfig
	serviceNames []string
	source       string
	target       string
	table        string
	tables       []string
	primaryKey   string
	lastID       string
	cursor       int
	viewportTop  int
	viewportSize int
	err          error
	result       string
	configPath   string
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

	m := model{
		screen:       screenSource,
		services:     services,
		serviceNames: serviceNames,
		primaryKey:   "id",
		lastID:       "1",
		configPath:   configPath,
		viewportSize: 10, // Show 10 items at a time
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
		case "ctrl+c", "esc":
			return m, tea.Quit

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
				// Adjust viewport if cursor moves above visible area
				if m.cursor < m.viewportTop {
					m.viewportTop = m.cursor
				}
			}

		case "down", "j":
			var maxItems int
			switch m.screen {
			case screenSource, screenTarget:
				maxItems = len(m.serviceNames)
			case screenTable:
				maxItems = len(m.tables)
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

		case "enter":
			switch m.screen {
			case screenSource:
				m.source = m.serviceNames[m.cursor]
				m.screen = screenTarget
				m.cursor = 0
				m.viewportTop = 0

			case screenTarget:
				m.target = m.serviceNames[m.cursor]
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

			case screenTable:
				m.table = m.tables[m.cursor]
				m.screen = screenPrimaryKey
				m.cursor = 0
				m.viewportTop = 0

			case screenPrimaryKey:
				m.screen = screenLastID

			case screenLastID:
				m.screen = screenConfirm

			case screenConfirm:
				m.screen = screenCopying
				return m, m.performCopy()

			case screenDone:
				return m, tea.Quit
			}

		case "\\":
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
			case screenPrimaryKey:
				m.screen = screenTable
				m.cursor = 0
				m.viewportTop = 0
			case screenLastID:
				m.screen = screenPrimaryKey
				m.cursor = 0
				m.viewportTop = 0
			case screenConfirm:
				m.screen = screenLastID
				m.cursor = 0
				m.viewportTop = 0
			}

		case "backspace":
			// Handle backspace in text input fields for deleting characters
			if m.screen == screenPrimaryKey {
				if len(m.primaryKey) > 0 {
					m.primaryKey = m.primaryKey[:len(m.primaryKey)-1]
				}
			} else if m.screen == screenLastID {
				if len(m.lastID) > 0 {
					m.lastID = m.lastID[:len(m.lastID)-1]
					if len(m.lastID) == 0 {
						m.lastID = "1" // Reset to default
					}
				}
			}

		default:
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
					if m.lastID == "1" && len(m.lastID) == 1 {
						// Replace default "1" with first digit
						m.lastID = msg.String()
					} else {
						m.lastID += msg.String()
					}
				}
			}
		}

	case copyResultMsg:
		m.result = string(msg)
		m.screen = screenDone
		return m, nil

	case copyErrorMsg:
		m.err = error(msg)
		m.screen = screenDone
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	var s strings.Builder

	s.WriteString(titleStyle.Render("psc"))
	s.WriteString("\n")

	switch m.screen {
	case screenSource:
		s.WriteString(promptStyle.Render("Select source service:"))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("(%d services)", len(m.serviceNames))))
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
		if end > len(m.serviceNames) {
			end = len(m.serviceNames)
		}

		for i := start; i < end; i++ {
			name := m.serviceNames[i]
			if i == m.cursor {
				s.WriteString(selectedStyle.Render("▸ " + name))
			} else {
				s.WriteString(normalStyle.Render("  " + name))
			}
			s.WriteString("\n")
		}

		// Show scroll indicator at bottom
		if end < len(m.serviceNames) {
			s.WriteString(normalStyle.Render("  ⬇ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more below)", len(m.serviceNames)-end)))
			s.WriteString("\n")
		}

	case screenTarget:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s", m.source)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Select target service:"))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("(%d services)", len(m.serviceNames))))
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
		if end > len(m.serviceNames) {
			end = len(m.serviceNames)
		}

		for i := start; i < end; i++ {
			name := m.serviceNames[i]
			if i == m.cursor {
				s.WriteString(selectedStyle.Render("▸ " + name))
			} else {
				s.WriteString(normalStyle.Render("  " + name))
			}
			s.WriteString("\n")
		}

		// Show scroll indicator at bottom
		if end < len(m.serviceNames) {
			s.WriteString(normalStyle.Render("  ⬇ ... "))
			s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more below)", len(m.serviceNames)-end)))
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
		s.WriteString(normalStyle.Render(fmt.Sprintf("(%d tables)", len(m.tables))))
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
			if end > len(m.tables) {
				end = len(m.tables)
			}

			for i := start; i < end; i++ {
				name := m.tables[i]
				if i == m.cursor {
					s.WriteString(selectedStyle.Render("▸ " + name))
				} else {
					s.WriteString(normalStyle.Render("  " + name))
				}
				s.WriteString("\n")
			}

			// Show scroll indicator at bottom
			if end < len(m.tables) {
				s.WriteString(normalStyle.Render("  ⬇ ... "))
				s.WriteString(normalStyle.Render(fmt.Sprintf("(%d more below)", len(m.tables)-end)))
				s.WriteString("\n")
			}
		}

		if m.err != nil {
			s.WriteString("\n")
			s.WriteString(errorStyle.Render(m.err.Error()))
		}

	case screenPrimaryKey:
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source: %s → Target: %s", m.source, m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table: %s", m.table)))
		s.WriteString("\n\n")
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
		s.WriteString(normalStyle.Render("Press Enter to continue (1 = start from beginning)"))

	case screenConfirm:
		s.WriteString(titleStyle.Render("Confirm Copy Operation"))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Source:      %s", m.source)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Target:      %s", m.target)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Table:       %s", m.table)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Primary Key: %s", m.primaryKey)))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render(fmt.Sprintf("Starting ID: %s", m.lastID)))
		s.WriteString("\n\n")
		s.WriteString(promptStyle.Render("Press Enter to start copy, \\ to go back"))

	case screenCopying:
		s.WriteString(titleStyle.Render("Copying..."))
		s.WriteString("\n\n")
		s.WriteString(normalStyle.Render("Copy in progress. This may take a while..."))
		s.WriteString("\n")
		s.WriteString(normalStyle.Render("(Output is being streamed above)"))

	case screenDone:
		if m.err != nil {
			s.WriteString(errorStyle.Render("Copy Failed"))
			s.WriteString("\n\n")
			s.WriteString(errorStyle.Render(m.err.Error()))
		} else {
			s.WriteString(successStyle.Render("Copy Completed Successfully!"))
			s.WriteString("\n\n")
			s.WriteString(normalStyle.Render(m.result))
		}
		s.WriteString("\n\n")
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

func (m model) performCopy() tea.Cmd {
	return func() tea.Msg {
		sourceConfig := m.services[m.source]
		targetConfig := m.services[m.target]

		// Parse lastID
		var lastID int64 = 1
		if m.lastID != "" {
			if parsed, err := strconv.ParseInt(m.lastID, 10, 64); err == nil {
				lastID = parsed
			}
		}

		err := CopyTable(m.source, m.target, sourceConfig, targetConfig, m.table, m.primaryKey, lastID)
		if err != nil {
			return copyErrorMsg(err)
		}
		return copyResultMsg("Table copied successfully!")
	}
}

func fetchTables(config ServiceConfig) ([]string, error) {
	db, err := sql.Open("postgres", config.ConnectionString())
	if err != nil {
		return nil, err
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
