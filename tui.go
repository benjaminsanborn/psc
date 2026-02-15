package main

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// TUI screens
const (
	screenList   = "list"
	screenDetail = "detail"
)

// tickMsg triggers periodic refresh.
type tickMsg struct{}

// pollDoneMsg signals polling is complete.
type pollDoneMsg struct{}

// Model is the bubbletea model.
type Model struct {
	daemon   *Daemon
	records  []MigrationRecord
	cursor   int
	screen   string
	width    int
	height   int
	err      string
	lastTick time.Time
}

// NewModel creates a new TUI model.
func NewModel(daemon *Daemon) Model {
	return Model{
		daemon: daemon,
		screen: screenList,
	}
}

func tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(_ time.Time) tea.Msg {
		return tickMsg{}
	})
}

func pollCmd(d *Daemon) tea.Cmd {
	return func() tea.Msg {
		_ = d.Poll()
		return pollDoneMsg{}
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(pollCmd(m.daemon), tickCmd())
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tickMsg:
		return m, tea.Batch(pollCmd(m.daemon), tickCmd())

	case pollDoneMsg:
		m.records = m.daemon.Records()
		// Update live state from executor
		for i := range m.records {
			if es := m.daemon.Executor.GetState(m.records[i].Name); es != nil {
				m.records[i].TotalAffected = es.TotalAffected.Load()
				m.records[i].LastCompletedID = es.LastCompletedID.Load()
				if es.MaxID > 0 {
					maxID := es.MaxID
					m.records[i].MaxID.Int64 = maxID
					m.records[i].MaxID.Valid = true
				}
			}
		}
		if errs := m.daemon.PopErrors(); len(errs) > 0 {
			m.err = strings.Join(errs, "; ")
		} else {
			m.err = ""
		}
		if m.cursor >= len(m.records) && len(m.records) > 0 {
			m.cursor = len(m.records) - 1
		}
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "up", "k":
		if m.screen == screenList && m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.screen == screenList && m.cursor < len(m.records)-1 {
			m.cursor++
		}
	case "r":
		if m.screen == screenList && len(m.records) > 0 {
			r := m.records[m.cursor]
			if r.Status == "pending" || r.Status == "failed" || r.Status == "cancelled" {
				if err := m.daemon.RunMigration(r.Name); err != nil {
					m.err = err.Error()
				}
			}
		}
	case "c":
		if len(m.records) > 0 {
			r := m.selectedRecord()
			if r != nil && r.Status == "running" {
				if err := m.daemon.CancelMigration(r.Name); err != nil {
					m.err = err.Error()
				}
			}
		}
	case "d", "enter":
		if m.screen == screenList && len(m.records) > 0 {
			m.screen = screenDetail
		}
	case "b", "esc":
		if m.screen == screenDetail {
			m.screen = screenList
		}
	}
	return m, nil
}

func (m Model) selectedRecord() *MigrationRecord {
	if m.cursor >= 0 && m.cursor < len(m.records) {
		r := m.records[m.cursor]
		return &r
	}
	return nil
}

// Styles
var (
	titleStyle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("99"))
	headerStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("245"))
	doneStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	runStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("33"))
	pendStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	failStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	cancelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	selStyle    = lipgloss.NewStyle().Background(lipgloss.Color("236"))
	helpStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	errStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	labelStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Width(14)
	valStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
)

func (m Model) View() string {
	if m.screen == screenDetail {
		return m.viewDetail()
	}
	return m.viewList()
}

func (m Model) viewList() string {
	var b strings.Builder

	// Header
	title := titleStyle.Render("psc - datafix runner")
	watching := headerStyle.Render(fmt.Sprintf("watching: %s", m.daemon.RepoPath))
	headerGap := ""
	if m.width > len("psc - datafix runner")+len(m.daemon.RepoPath)+15 {
		headerGap = strings.Repeat(" ", m.width-len("psc - datafix runner")-len(m.daemon.RepoPath)-15)
	}
	b.WriteString(title + headerGap + watching + "\n\n")

	// Column headers
	b.WriteString(headerStyle.Render(fmt.Sprintf(" %-10s %-32s %-18s %s", "STATUS", "NAME", "PROGRESS", "AFFECTED")))
	b.WriteString("\n")

	// Rows
	for i, r := range m.records {
		line := formatRow(r, m.daemon.Executor)
		if i == m.cursor {
			line = selStyle.Render(line)
		}
		b.WriteString(line + "\n")
	}

	if len(m.records) == 0 {
		b.WriteString(pendStyle.Render(" No migrations found\n"))
	}

	b.WriteString("\n")

	// Error
	if m.err != "" {
		b.WriteString(errStyle.Render(" ⚠ "+m.err) + "\n")
	}

	// Help
	b.WriteString(helpStyle.Render(" [r] run  [c] cancel  [d] details  [↑↓] navigate  [q] quit"))
	return b.String()
}

func formatRow(r MigrationRecord, exec *Executor) string {
	var icon, status, progress, affected string

	switch r.Status {
	case "completed":
		icon = doneStyle.Render("✅ done")
		progress = "100%"
		affected = FormatNumber(r.TotalAffected)
	case "running":
		icon = runStyle.Render("🔄 run")
		progress = progressBar(r)
		affected = FormatNumber(r.TotalAffected)
	case "pending":
		icon = pendStyle.Render("⏳ pending")
		progress = "—"
		affected = "—"
	case "failed":
		icon = failStyle.Render("❌ failed")
		if r.BatchColumn.Valid {
			progress = fmt.Sprintf("chunk %d", r.LastCompletedID)
		} else {
			progress = "failed"
		}
		affected = FormatNumber(r.TotalAffected)
	case "cancelled":
		icon = cancelStyle.Render("⏸ cancel")
		progress = progressBar(r)
		affected = FormatNumber(r.TotalAffected)
	default:
		icon = r.Status
	}
	status = icon

	name := r.Name
	if len(name) > 30 {
		name = name[:27] + "..."
	}

	return fmt.Sprintf(" %-21s %-32s %-18s %s", status, name, progress, affected)
}

func progressBar(r MigrationRecord) string {
	if !r.MaxID.Valid || r.MaxID.Int64 == 0 {
		return "—"
	}
	pct := float64(r.LastCompletedID) / float64(r.MaxID.Int64) * 100
	filled := int(pct / 100 * 8)
	if filled > 8 {
		filled = 8
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", 8-filled)
	return fmt.Sprintf("[%s] %.0f%%", bar, pct)
}

func (m Model) viewDetail() string {
	r := m.selectedRecord()
	if r == nil {
		return "No migration selected"
	}

	var b strings.Builder

	title := titleStyle.Render(fmt.Sprintf("psc - %s", r.Name))
	statusLabel := headerStyle.Render(fmt.Sprintf("Status: %s", r.Status))
	b.WriteString(title + "    " + statusLabel + "\n\n")

	line := func(label, value string) {
		b.WriteString(labelStyle.Render(" "+label+":") + " " + valStyle.Render(value) + "\n")
	}

	svc := "—"
	if r.TargetService.Valid {
		svc = r.TargetService.String
	}
	line("Target", svc)

	if r.BatchColumn.Valid {
		chunk := "—"
		if r.ChunkSize.Valid {
			chunk = FormatNumber(int64(r.ChunkSize.Int32))
		}
		par := "1"
		if r.Parallelism.Valid {
			par = fmt.Sprintf("%d", r.Parallelism.Int32)
		}
		line("Batch", fmt.Sprintf("column=%s, chunk=%s, parallelism=%s", r.BatchColumn.String, chunk, par))

		if r.MaxID.Valid {
			line("Max ID", FormatNumber(r.MaxID.Int64))
		}
		line("Current ID", FormatNumber(r.LastCompletedID))

		// Progress bar
		if r.MaxID.Valid && r.MaxID.Int64 > 0 {
			pct := float64(r.LastCompletedID) / float64(r.MaxID.Int64) * 100
			filled := int(pct / 100 * 40)
			if filled > 40 {
				filled = 40
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", 40-filled)
			line("Progress", fmt.Sprintf("[%s] %.1f%%", bar, pct))
		}
	}

	line("Affected", FormatNumber(r.TotalAffected)+" rows")
	line("Errors", fmt.Sprintf("%d", r.ErrorCount))

	// Rate and ETA from executor state
	if es := m.daemon.Executor.GetState(r.Name); es != nil {
		rate := es.Rate.Load()
		if rate > 0 {
			line("Rate", fmt.Sprintf("~%s rows/sec", FormatNumber(rate)))
			if r.MaxID.Valid && r.MaxID.Int64 > 0 {
				remaining := r.MaxID.Int64 - r.LastCompletedID
				etaSec := remaining / rate
				if etaSec > 3600 {
					line("ETA", fmt.Sprintf("%dh %dm", etaSec/3600, (etaSec%3600)/60))
				} else if etaSec > 60 {
					line("ETA", fmt.Sprintf("%dm %ds", etaSec/60, etaSec%60))
				} else {
					line("ETA", fmt.Sprintf("%ds", etaSec))
				}
			}
		}
	}

	if r.StartedAt.Valid {
		line("Started", r.StartedAt.Time.Format("2006-01-02 15:04:05"))
	}
	if r.CompletedAt.Valid {
		line("Completed", r.CompletedAt.Time.Format("2006-01-02 15:04:05"))
	}

	b.WriteString("\n")
	if r.LastError.Valid && r.LastError.String != "" {
		b.WriteString(errStyle.Render(" Last error: "+r.LastError.String) + "\n")
	}

	b.WriteString("\n")
	b.WriteString(helpStyle.Render(" [c] cancel  [b] back  [q] quit"))
	return b.String()
}
