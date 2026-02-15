# psc — datafix migration runner

A TUI-based daemon for running large-scale data fix migrations against PostgreSQL databases.

## Features

- **Watch mode** — monitors a directory for `.sql` migration files
- **Batched execution** — split large updates into chunks with configurable parallelism
- **Resume support** — restart psc and it picks up where it left off
- **Cancellation** — cancel running migrations gracefully via TUI or CLI
- **Multi-target** — each migration can target a different `pg_service.conf` service
- **Live TUI** — real-time progress bars, affected row counts, rate estimates, and ETAs

## Installation

### Homebrew

```bash
brew tap benjaminsanborn/benjaminsanborn
brew install psc
```

### From source

```bash
go install psc@latest
# or
git clone https://github.com/benjaminsanborn/psc.git
cd psc && go build -o psc .
```

## Usage

```bash
# Launch TUI daemon (watches for migrations, interactive control)
psc --repo /path/to/migrations --service my_db

# Show current migration status (non-interactive)
psc --repo /path/to/migrations --service my_db status

# Run a specific migration immediately (blocking)
psc --repo /path/to/migrations --service my_db run <name>

# Cancel a running migration
psc --repo /path/to/migrations --service my_db cancel <name>
```

### Flags

| Flag | Description |
|------|-------------|
| `--repo` | Path to the migrations directory (default: `.`) |
| `--service` | Default `pg_service.conf` service name (required) |

## Migration Format

Each migration is a `.sql` file with metadata in SQL comments:

```sql
-- psc:migrate name=convert_paths_to_ltree
-- psc:target service=my_production_db
-- psc:batch column=id chunk=10000 parallelism=4
-- psc:on_error continue
-- psc:timeout 30s

UPDATE categories
SET path = text2ltree(path_text)
WHERE id BETWEEN :start AND :end
  AND path IS NULL;
```

### Directives

| Directive | Required | Description |
|-----------|----------|-------------|
| `psc:migrate name=<name>` | ✅ | Unique migration name |
| `psc:target service=<name>` | No | Target `pg_service.conf` service (overrides `--service`) |
| `psc:batch column=<col> chunk=<size> parallelism=<n>` | No | Enable batched execution with `:start`/`:end` placeholders |
| `psc:on_error continue\|abort` | No | Error handling (default: `abort`) |
| `psc:timeout <duration>` | No | Per-chunk timeout (e.g., `30s`, `5m`) |

### Non-batched migrations

Without `psc:batch`, the SQL runs as a single statement:

```sql
-- psc:migrate name=add_default_role

INSERT INTO user_roles (user_id, role)
SELECT id, 'viewer' FROM users
WHERE id NOT IN (SELECT user_id FROM user_roles);
```

### Batched migrations

With `psc:batch`, the SQL must contain `:start` and `:end` placeholders:

```sql
-- psc:migrate name=backfill_emails
-- psc:batch column=id chunk=5000 parallelism=8

UPDATE users SET email_lower = LOWER(email)
WHERE id BETWEEN :start AND :end AND email_lower IS NULL;
```

psc will:
1. Query `SELECT MAX(id) FROM users` to determine the range
2. Spawn 8 parallel workers
3. Each worker processes chunks of 5,000 IDs
4. Progress is tracked in the `psc_migrations` table for resume support

## TUI Controls

| Key | Action |
|-----|--------|
| `↑`/`↓` or `k`/`j` | Navigate migration list |
| `r` | Run selected migration |
| `c` | Cancel selected migration |
| `d` or `Enter` | View migration details |
| `b` or `Esc` | Back to list |
| `q` | Quit |

## Database Setup

psc uses `~/.pg_service.conf` for connection details. Example:

```ini
[my_db]
host=localhost
port=5432
dbname=myapp
user=myuser
password=mypassword
```

psc automatically creates a `psc_migrations` table in the target database to track state.

## State Management

Migration states: `pending` → `running` → `completed` | `failed` | `cancelled`

- **pending** — detected but not started (user must press `r`)
- **running** — currently executing
- **completed** — finished successfully
- **failed** — encountered an error (with `on_error=abort`)
- **cancelled** — stopped by user; can be resumed with `r`
