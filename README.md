# psc

A fast, parallel table copier for PostgreSQL databases using pg_service.conf

## Features

- 🚀 **Fast parallel copying** - Multi-worker support for faster data transfer
- 📦 **Chunked transfers** - Configurable batch sizes for memory-efficient copying
- 🔄 **Resume capability** - Automatically tracks progress and resume interrupted copies
- 🎨 **Interactive TUI** - Beautiful terminal UI for easy operation
- ⚡ **CLI mode** - Full command-line support for automation
- 🔒 **SSL handling** - Automatic SSL fallback if server doesn't support it
- 📊 **Progress tracking** - Real-time progress with time estimates

## Installation

```bash
go install github.com/benjaminsanborn/psc@latest
```

Or build from source:

```bash
git clone https://github.com/benjaminsanborn/psc
cd psc
go build
```

## Prerequisites

- PostgreSQL `psql` command-line tool installed and in PATH
- A configured `~/.pg_service.conf` file with your database connections

### Example pg_service.conf

```ini
[source_db]
host=source.example.com
port=5432
dbname=mydb
user=postgres
password=secret

[target_db]
host=target.example.com
port=5432
dbname=mydb
user=postgres
password=secret
```

## Usage

### Interactive Mode (Recommended)

Simply run `psc` without arguments to launch the interactive TUI:

```bash
psc
```

The interactive mode provides:
- Service selection with filtering
- Table browsing from source database
- Resume existing copy operations
- Progress visualization
- Cancellation support (ESC to cancel)

### CLI Mode

```bash
psc -source <service> -target <service> -table <tablename> [options]
```

#### Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-source` | Source service name from pg_service.conf | *required* |
| `-target` | Target service name from pg_service.conf | *required* |
| `-table` | Table name to copy | *required* |
| `-primary-key` | Primary key column for chunking | `id` |
| `-last-id` | Resume from this ID (for resuming) | `0` |
| `-chunk-size` | Rows per batch | `1000` |
| `-parallelism` | Number of concurrent workers | `1` |

#### Examples

Basic table copy:
```bash
psc -source prod_db -target dev_db -table users
```

Fast copy with parallel workers:
```bash
psc -source prod_db -target dev_db -table users -parallelism 4 -chunk-size 5000
```

Resume an interrupted copy:
```bash
psc -source prod_db -target dev_db -table users -last-id 50000
```

Copy using a custom primary key:
```bash
psc -source prod_db -target dev_db -table orders -primary-key order_id
```

## How It Works

1. **Connects** to source and target databases using pg_service.conf configurations
2. **Validates** that the table exists on both source and target
3. **Chunks** the data based on primary key ranges
4. **Copies** data using PostgreSQL's native COPY command via psql
5. **Tracks** progress in `~/.psc/in_progress/` for resume capability
6. **Parallelizes** work across multiple workers when configured

The tool uses efficient binary COPY format and pipes data directly between databases for optimal performance.

## State Management

Copy operations are tracked in `~/.psc/`:
- **In progress**: `~/.psc/in_progress/` - Active copy operations
- **Completed**: `~/.psc/completed/` - Completed operations with timestamps

State files can be:
- Resumed from interactive mode
- Manually deleted with 'x' key in TUI
- Used to restart failed operations

## Performance Tips

- **Parallelism**: Start with 2-4 workers, test higher values based on your database capacity
- **Chunk size**: Larger chunks (5000-10000) are faster but use more memory
- **Network**: Works best on low-latency connections between databases
- **Indexes**: Consider temporarily dropping indexes on target table for faster inserts

## Requirements

Target table must:
- Already exist with matching schema
- Have a numeric primary key (or column specified with `-primary-key`)
- Be accessible by the target database user

## Limitations

- Only copies table data (not schema, indexes, or constraints)
- Requires numeric primary key for chunking
- Both databases must be accessible via psql with service names
- Parallel mode may cause gaps in ID sequences if chunks fail

## Contributing

Contributions welcome! Please feel free to submit a Pull Request.
