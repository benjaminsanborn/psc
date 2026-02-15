package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// Version is set by goreleaser via ldflags.
var version = "dev"

func main() {
	repo := flag.String("repo", ".", "path to migrations directory")
	service := flag.String("service", "", "default pg_service.conf service name")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("psc %s\n", version)
		return
	}

	args := flag.Args()

	if len(args) == 0 {
		// TUI daemon mode
		runTUI(*repo, *service)
		return
	}

	switch args[0] {
	case "status":
		runStatus(*repo, *service)
	case "run":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: psc run <name>")
			os.Exit(1)
		}
		runSingle(*repo, *service, args[1])
	case "cancel":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "usage: psc cancel <name>")
			os.Exit(1)
		}
		runCancel(*repo, *service, args[1])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		os.Exit(1)
	}
}

func runTUI(repo, service string) {
	d, err := NewDaemon(repo, service)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer d.StateDB.Close()

	p := tea.NewProgram(NewModel(d), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func runStatus(repo, service string) {
	d, err := NewDaemon(repo, service)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer d.StateDB.Close()

	if err := d.Poll(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	records := d.Records()
	if len(records) == 0 {
		fmt.Println("No migrations found.")
		return
	}

	fmt.Printf("%-12s %-32s %-18s %s\n", "STATUS", "NAME", "PROGRESS", "AFFECTED")
	fmt.Println(strings.Repeat("-", 80))
	for _, r := range records {
		progress := "—"
		affected := "—"
		if r.Status == "completed" {
			progress = "100%"
		}
		if r.TotalAffected > 0 {
			affected = FormatNumber(r.TotalAffected)
		}
		fmt.Printf("%-12s %-32s %-18s %s\n", r.Status, r.Name, progress, affected)
	}
}

func runSingle(repo, service, name string) {
	d, err := NewDaemon(repo, service)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer d.StateDB.Close()

	if err := d.Poll(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	m := d.GetMigration(name)
	if m == nil {
		fmt.Fprintf(os.Stderr, "migration %q not found in repo\n", name)
		os.Exit(1)
	}

	record, err := GetMigrationByName(d.StateDB, name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Running migration: %s\n", name)
	if err := d.Executor.Run(m, record); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Done.")
}

func runCancel(repo, service, name string) {
	// Cancel only works in TUI/daemon mode since it requires the running context.
	// For CLI, we just set the status to cancelled in the DB.
	d, err := NewDaemon(repo, service)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer d.StateDB.Close()

	if err := UpdateStatus(d.StateDB, name, "cancelled"); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Migration %q marked as cancelled.\n", name)
}
