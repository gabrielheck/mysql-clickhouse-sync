# MySQL to ClickHouse Replicator

A tool for replicating data from MySQL to ClickHouse, including automatic schema conversion.

## Features

- **Two replication modes:**
  - **Snapshot**: One-time full copy of data
  - **CDC (Change Data Capture)**: Real-time replication via MySQL binlog
- Automatic schema conversion from MySQL to ClickHouse types
- Batch data transfer for efficient memory usage
- Server-side cursor (SSCursor) for memory-efficient streaming
- LZ4 compression for network optimization
- Configurable table selection
- Environment-based configuration
- Docker support with persistent binlog position

## Configuration

All settings are configured via environment variables:

### MySQL Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `MYSQL_HOST` | MySQL server hostname | Required |
| `MYSQL_PORT` | MySQL server port | `3306` |
| `MYSQL_USER` | MySQL username | Required |
| `MYSQL_PASSWORD` | MySQL password | Required |
| `MYSQL_DATABASE` | Source database name | Required |

### ClickHouse Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_HOST` | ClickHouse server hostname | Required |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port | `8123` |
| `CLICKHOUSE_USER` | ClickHouse username | `default` |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | Empty |
| `CLICKHOUSE_DATABASE` | Target database name | Required |

### Replication Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `REPLICATION_MODE` | `snapshot` or `cdc` | `snapshot` |
| `REPLICATION_BATCH_SIZE` | Rows per batch | `50000` |
| `REPLICATION_TABLES` | Comma-separated table list | All tables |
| `REPLICATION_DROP_EXISTING` | Drop tables before creating | `false` |
| `REPLICATION_PARALLEL_TABLES` | Tables to process in parallel | `1` |

## Usage

### With Docker Compose

1. Copy `env.example` to `.env` and configure your settings:

```bash
cp env.example .env
```

2. Edit `.env` with your database credentials.

3. Run snapshot mode (one-time copy):

```bash
REPLICATION_MODE=snapshot docker compose up --build
```

4. Run CDC mode (real-time):

```bash
REPLICATION_MODE=cdc docker compose up --build
```

### MySQL Requirements for CDC

For CDC mode, MySQL must have binlog enabled:

```ini
# my.cnf
[mysqld]
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

The MySQL user needs these permissions:

```sql
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%';
GRANT SELECT ON source_db.* TO 'user'@'%';
```

### Local Development

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set environment variables and run:

```bash
export MYSQL_HOST=localhost
export MYSQL_DATABASE=mydb
export REPLICATION_MODE=cdc
# ... set other variables
python -m src.main
```

## Type Mapping

| MySQL Type | ClickHouse Type |
|------------|-----------------|
| TINYINT | Int8 |
| SMALLINT | Int16 |
| INT | Int32 |
| BIGINT | Int64 |
| FLOAT | Float32 |
| DOUBLE | Float64 |
| DECIMAL(p,s) | Decimal(p,s) |
| DATE | Date |
| DATETIME | DateTime |
| TIMESTAMP | DateTime |
| VARCHAR/TEXT | String |
| JSON | String |
| ENUM | String |

Nullable columns are wrapped with `Nullable()`.

## CDC Mode Details

In CDC mode, the replicator:

1. **Initial sync**: Captures current binlog position, then copies all existing data
2. **Continuous sync**: Reads binlog events (INSERT, UPDATE, DELETE) and applies them

### ClickHouse Table Structure (CDC)

CDC mode uses `ReplacingMergeTree` engine with extra columns:

| Column | Type | Description |
|--------|------|-------------|
| `_version` | UInt64 | Microsecond timestamp for version ordering |
| `_deleted` | UInt8 | Soft delete flag (0=active, 1=deleted) |

### Querying CDC Data

For each table, a `_live` view is created that filters deleted rows:

```sql
-- Raw table (includes all versions and deleted rows)
SELECT * FROM target_db.users;

-- Live view (only current, non-deleted rows)
SELECT * FROM target_db.users_live;

-- Or use FINAL manually
SELECT * FROM target_db.users FINAL WHERE _deleted = 0;
```

## Architecture

```
src/
├── config.py           # Environment configuration
├── mysql_client.py     # MySQL connection and data extraction
├── clickhouse_client.py # ClickHouse connection and insertion
├── schema_converter.py # MySQL to ClickHouse schema conversion
├── replicator.py       # Snapshot replication orchestration
├── cdc_replicator.py   # CDC/binlog replication
└── main.py             # Entry point
```

## License

MIT

