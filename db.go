package main

import (
	"database/sql"
	"fmt"
	"time"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS psc_migrations (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    filename TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    target_service TEXT,
    batch_column TEXT,
    chunk_size INT,
    parallelism INT,
    max_id BIGINT,
    last_completed_id BIGINT DEFAULT 0,
    total_affected_rows BIGINT DEFAULT 0,
    error_count INT DEFAULT 0,
    last_error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);`

// MigrationRecord represents a row in the psc_migrations table.
type MigrationRecord struct {
	ID               int
	Name             string
	Filename         string
	Status           string
	TargetService    sql.NullString
	BatchColumn      sql.NullString
	ChunkSize        sql.NullInt32
	Parallelism      sql.NullInt32
	MaxID            sql.NullInt64
	LastCompletedID  int64
	TotalAffected    int64
	ErrorCount       int
	LastError        sql.NullString
	StartedAt        sql.NullTime
	CompletedAt      sql.NullTime
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// EnsureMigrationsTable creates the psc_migrations table if it doesn't exist.
func EnsureMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(createTableSQL)
	return err
}

// UpsertMigration inserts or updates a migration record from a parsed Migration.
func UpsertMigration(db *sql.DB, m *Migration) error {
	_, err := db.Exec(`
		INSERT INTO psc_migrations (name, filename, target_service, batch_column, chunk_size, parallelism)
		VALUES ($1, $2, $3, NULLIF($4,''), NULLIF($5,0), NULLIF($6,0))
		ON CONFLICT (name) DO UPDATE SET
			filename = EXCLUDED.filename,
			target_service = EXCLUDED.target_service,
			batch_column = EXCLUDED.batch_column,
			chunk_size = EXCLUDED.chunk_size,
			parallelism = EXCLUDED.parallelism,
			updated_at = NOW()
		WHERE psc_migrations.status = 'pending'`,
		m.Name, m.Filename, nullStr(m.Service), m.BatchColumn, m.ChunkSize, m.Parallelism)
	return err
}

// LoadMigrations loads all migration records from the DB.
func LoadMigrations(db *sql.DB) ([]MigrationRecord, error) {
	rows, err := db.Query(`
		SELECT id, name, filename, status, target_service, batch_column, chunk_size, parallelism,
		       max_id, last_completed_id, total_affected_rows, error_count, last_error,
		       started_at, completed_at, created_at, updated_at
		FROM psc_migrations ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []MigrationRecord
	for rows.Next() {
		var r MigrationRecord
		err := rows.Scan(&r.ID, &r.Name, &r.Filename, &r.Status, &r.TargetService,
			&r.BatchColumn, &r.ChunkSize, &r.Parallelism, &r.MaxID,
			&r.LastCompletedID, &r.TotalAffected, &r.ErrorCount, &r.LastError,
			&r.StartedAt, &r.CompletedAt, &r.CreatedAt, &r.UpdatedAt)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// UpdateStatus updates the migration status and related fields.
func UpdateStatus(db *sql.DB, name, status string) error {
	now := time.Now()
	switch status {
	case "running":
		_, err := db.Exec(`UPDATE psc_migrations SET status=$1, started_at=$2, updated_at=$2 WHERE name=$3`,
			status, now, name)
		return err
	case "completed":
		_, err := db.Exec(`UPDATE psc_migrations SET status=$1, completed_at=$2, updated_at=$2 WHERE name=$3`,
			status, now, name)
		return err
	default:
		_, err := db.Exec(`UPDATE psc_migrations SET status=$1, updated_at=$2 WHERE name=$3`,
			status, now, name)
		return err
	}
}

// UpdateProgress updates last_completed_id and total_affected_rows.
func UpdateProgress(db *sql.DB, name string, lastID, affected int64) error {
	_, err := db.Exec(`UPDATE psc_migrations SET last_completed_id=$1, total_affected_rows=$2, updated_at=NOW() WHERE name=$3`,
		lastID, affected, name)
	return err
}

// UpdateMaxID sets the max_id for a batched migration.
func UpdateMaxID(db *sql.DB, name string, maxID int64) error {
	_, err := db.Exec(`UPDATE psc_migrations SET max_id=$1, updated_at=NOW() WHERE name=$2`, maxID, name)
	return err
}

// RecordError increments error_count and sets last_error.
func RecordError(db *sql.DB, name string, errMsg string) error {
	_, err := db.Exec(`UPDATE psc_migrations SET error_count=error_count+1, last_error=$1, updated_at=NOW() WHERE name=$2`,
		errMsg, name)
	return err
}

// GetMigrationByName loads a single migration record.
func GetMigrationByName(db *sql.DB, name string) (*MigrationRecord, error) {
	r := &MigrationRecord{}
	err := db.QueryRow(`
		SELECT id, name, filename, status, target_service, batch_column, chunk_size, parallelism,
		       max_id, last_completed_id, total_affected_rows, error_count, last_error,
		       started_at, completed_at, created_at, updated_at
		FROM psc_migrations WHERE name=$1`, name).Scan(
		&r.ID, &r.Name, &r.Filename, &r.Status, &r.TargetService,
		&r.BatchColumn, &r.ChunkSize, &r.Parallelism, &r.MaxID,
		&r.LastCompletedID, &r.TotalAffected, &r.ErrorCount, &r.LastError,
		&r.StartedAt, &r.CompletedAt, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func nullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// FormatNumber adds commas to an integer for display.
func FormatNumber(n int64) string {
	if n < 0 {
		return "-" + FormatNumber(-n)
	}
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}
