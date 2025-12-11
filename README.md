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
- Parallel table replication support
- Environment-based configuration
- Docker support with persistent binlog position
- Structured JSON logging with `structlog`

## Quick Start with Docker Hub

The easiest way to use this replicator is via the published Docker image.

### Snapshot Mode (One-time Copy)

```bash
docker run --rm \
  -e MYSQL_HOST=your-mysql-host \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=root \
  -e MYSQL_PASSWORD=your-mysql-password \
  -e MYSQL_DATABASE=source_db \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=your-clickhouse-password \
  -e CLICKHOUSE_DATABASE=target_db \
  -e REPLICATION_MODE=snapshot \
  gabrielheck/mysql-clickhouse-sync:latest
```

### CDC Mode (Real-time Replication)

For CDC mode, you need a persistent volume to store the binlog position:

```bash
docker run -d \
  --name mysql-ch-sync \
  -v mysql_ch_sync_data:/data \
  -e MYSQL_HOST=your-mysql-host \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=replication_user \
  -e MYSQL_PASSWORD=your-mysql-password \
  -e MYSQL_DATABASE=source_db \
  -e CLICKHOUSE_HOST=your-clickhouse-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=your-clickhouse-password \
  -e CLICKHOUSE_DATABASE=target_db \
  -e REPLICATION_MODE=cdc \
  gabrielheck/mysql-clickhouse-sync:latest
```

### With Docker Network (Connecting to Other Containers)

```bash
# Create a network
docker network create replication-net

# Run with network access to other containers
docker run -d \
  --name mysql-ch-sync \
  --network replication-net \
  -v mysql_ch_sync_data:/data \
  -e MYSQL_HOST=mysql-container \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=root \
  -e MYSQL_PASSWORD=your-mysql-password \
  -e MYSQL_DATABASE=mydb \
  -e CLICKHOUSE_HOST=clickhouse-container \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=your-clickhouse-password \
  -e CLICKHOUSE_DATABASE=mydb \
  -e REPLICATION_MODE=cdc \
  gabrielheck/mysql-clickhouse-sync:latest
```

### Replicate Specific Tables

```bash
docker run --rm \
  -e MYSQL_HOST=mysql-host \
  -e MYSQL_PORT=3306 \
  -e MYSQL_USER=root \
  -e MYSQL_PASSWORD=your-mysql-password \
  -e MYSQL_DATABASE=source_db \
  -e CLICKHOUSE_HOST=clickhouse-host \
  -e CLICKHOUSE_PORT=8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=your-clickhouse-password \
  -e CLICKHOUSE_DATABASE=target_db \
  -e REPLICATION_MODE=snapshot \
  -e REPLICATION_TABLES=users,orders,products \
  -e REPLICATION_PARALLEL_TABLES=3 \
  gabrielheck/mysql-clickhouse-sync:latest
```

### Using Docker Secrets (Recommended for Production)

For secure credential management in Docker Swarm or Compose, use Docker Secrets:

**1. Create secret files:**

```bash
echo "your-mysql-password" | docker secret create mysql_password -
echo "your-clickhouse-password" | docker secret create clickhouse_password -
```

Or for Docker Compose, create local files:

```bash
mkdir -p ./secrets
echo -n "your-mysql-password" > ./secrets/mysql_password
echo -n "your-clickhouse-password" > ./secrets/clickhouse_password
chmod 600 ./secrets/*
```

**2. Docker Compose with Secrets:**

```yaml
services:
  replicator:
    image: gabrielheck/mysql-clickhouse-sync:latest
    environment:
      # MySQL Configuration
      MYSQL_HOST: mysql-server
      MYSQL_PORT: 3306
      MYSQL_USER: replication_user
      MYSQL_PASSWORD_FILE: /run/secrets/mysql_password
      MYSQL_DATABASE: source_db
      # ClickHouse Configuration
      CLICKHOUSE_HOST: clickhouse-server
      CLICKHOUSE_PORT: 8123
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD_FILE: /run/secrets/clickhouse_password
      CLICKHOUSE_DATABASE: target_db
      # Replication Configuration
      REPLICATION_MODE: cdc
    secrets:
      - mysql_password
      - clickhouse_password
    volumes:
      - replicator_data:/data
    restart: unless-stopped

secrets:
  mysql_password:
    file: ./secrets/mysql_password
  clickhouse_password:
    file: ./secrets/clickhouse_password

volumes:
  replicator_data:
```

**3. Docker Swarm with Secrets:**

```yaml
services:
  replicator:
    image: gabrielheck/mysql-clickhouse-sync:latest
    environment:
      MYSQL_HOST: mysql-server
      MYSQL_PORT: 3306
      MYSQL_USER: replication_user
      MYSQL_PASSWORD_FILE: /run/secrets/mysql_password
      MYSQL_DATABASE: source_db
      CLICKHOUSE_HOST: clickhouse-server
      CLICKHOUSE_PORT: 8123
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD_FILE: /run/secrets/clickhouse_password
      CLICKHOUSE_DATABASE: target_db
      REPLICATION_MODE: cdc
    secrets:
      - mysql_password
      - clickhouse_password
    volumes:
      - replicator_data:/data
    deploy:
      replicas: 1
      restart_policy:
        condition: on-failure

secrets:
  mysql_password:
    external: true
  clickhouse_password:
    external: true

volumes:
  replicator_data:
```

Deploy with:

```bash
docker stack deploy -c docker-compose.yml mysql-ch-sync
```

> **Note:** When using `*_FILE` environment variables, the application reads the password from the specified file path instead of using the value directly. This keeps sensitive credentials out of environment variable listings and container inspect output.

## Configuration

All settings are configured via environment variables:

### MySQL Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `MYSQL_HOST` | MySQL server hostname | Required |
| `MYSQL_PORT` | MySQL server port | `3306` |
| `MYSQL_USER` | MySQL username | Required |
| `MYSQL_PASSWORD` | MySQL password | Empty |
| `MYSQL_PASSWORD_FILE` | Path to file containing password (Docker Secrets) | - |
| `MYSQL_DATABASE` | Source database name | Required |

### ClickHouse Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `CLICKHOUSE_HOST` | ClickHouse server hostname | Required |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port | `8123` |
| `CLICKHOUSE_USER` | ClickHouse username | `default` |
| `CLICKHOUSE_PASSWORD` | ClickHouse password | Empty |
| `CLICKHOUSE_PASSWORD_FILE` | Path to file containing password (Docker Secrets) | - |
| `CLICKHOUSE_DATABASE` | Target database name | Required |

> **Note:** When both `*_PASSWORD` and `*_PASSWORD_FILE` are set, the file takes precedence.

### Replication Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `REPLICATION_MODE` | `snapshot` or `cdc` | `snapshot` |
| `REPLICATION_BATCH_SIZE` | Rows per batch | `50000` |
| `REPLICATION_TABLES` | Comma-separated table list | All tables |
| `REPLICATION_DROP_EXISTING` | Drop tables before creating | `false` |
| `REPLICATION_PARALLEL_TABLES` | Tables to process in parallel | `1` |
| `REPLICATION_POSITION_FILE` | Binlog position file path (CDC) | `/data/binlog_position.json` |

## Usage with Docker Compose

### Using Published Image

Create a `docker-compose.yml`:

```yaml
services:
  replicator:
    image: gabrielheck/mysql-clickhouse-sync:latest
    environment:
      # MySQL Configuration
      MYSQL_HOST: ${MYSQL_HOST}
      MYSQL_PORT: ${MYSQL_PORT:-3306}
      MYSQL_USER: ${MYSQL_USER}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
      MYSQL_DATABASE: ${MYSQL_DATABASE}
      # ClickHouse Configuration
      CLICKHOUSE_HOST: ${CLICKHOUSE_HOST}
      CLICKHOUSE_PORT: ${CLICKHOUSE_PORT:-8123}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER:-default}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-}
      CLICKHOUSE_DATABASE: ${CLICKHOUSE_DATABASE}
      # Replication Configuration
      REPLICATION_MODE: ${REPLICATION_MODE:-snapshot}
      REPLICATION_BATCH_SIZE: ${REPLICATION_BATCH_SIZE:-50000}
      REPLICATION_TABLES: ${REPLICATION_TABLES:-}
      REPLICATION_DROP_EXISTING: ${REPLICATION_DROP_EXISTING:-false}
      REPLICATION_PARALLEL_TABLES: ${REPLICATION_PARALLEL_TABLES:-1}
    volumes:
      - replicator_data:/data
    restart: unless-stopped

volumes:
  replicator_data:
```

Create a `.env` file:

```bash
# MySQL Configuration
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your-mysql-password
MYSQL_DATABASE=source_db

# ClickHouse Configuration
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-clickhouse-password
CLICKHOUSE_DATABASE=target_db

# Replication Configuration
REPLICATION_MODE=cdc
REPLICATION_BATCH_SIZE=50000
REPLICATION_TABLES=
REPLICATION_DROP_EXISTING=false
REPLICATION_PARALLEL_TABLES=1
```

Run:

```bash
docker compose up -d
```

### Building from Source

1. Clone the repository and copy `env.example` to `.env`:

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

## MySQL Requirements for CDC

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

## Local Development

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set environment variables and run:

```bash
export MYSQL_HOST=localhost
export MYSQL_USER=root
export MYSQL_PASSWORD=secret
export MYSQL_DATABASE=mydb
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_DATABASE=mydb
export REPLICATION_MODE=cdc
python -m src.main
```

## Type Mapping

| MySQL Type | ClickHouse Type |
|------------|-----------------|
| TINYINT | Int8 |
| SMALLINT | Int16 |
| MEDIUMINT | Int32 |
| INT/INTEGER | Int32 |
| BIGINT | Int64 |
| FLOAT | Float32 |
| DOUBLE | Float64 |
| DECIMAL/NUMERIC(p,s) | Decimal(p,s) |
| BIT | UInt64 |
| BOOL/BOOLEAN | Bool |
| DATE | Date |
| DATETIME | DateTime |
| TIMESTAMP | DateTime |
| TIME | String |
| YEAR | UInt16 |
| CHAR/VARCHAR | String |
| BINARY/VARBINARY | String |
| TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT | String |
| TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB | String |
| ENUM | String |
| SET | String |
| JSON | String |

Nullable columns are wrapped with `Nullable()`.

## CDC Mode Details

In CDC mode, the replicator:

1. **Initial sync**: Captures current binlog position, then copies all existing data
2. **Continuous sync**: Reads binlog events (INSERT, UPDATE, DELETE) and applies them
3. **Position persistence**: Saves binlog position every 5 seconds to resume after restarts

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

### Resume After Restart

CDC mode automatically resumes from the last saved binlog position. The position is stored in the file specified by `REPLICATION_POSITION_FILE` (default: `/data/binlog_position.json`).

To reset and start fresh:

```bash
# Remove the position file to trigger a full re-sync
docker exec mysql-ch-sync rm /data/binlog_position.json
docker restart mysql-ch-sync
```

## Limitations & Performance

### Performance Estimates

| Metric | Estimate | Notes |
|--------|----------|-------|
| **CDC throughput** | ~1,000-5,000 events/sec | Single-threaded binlog processing |
| **Snapshot throughput** | ~50,000-200,000 rows/sec | Depends on row size and network |
| **Latency (CDC)** | 10-100ms | From MySQL commit to ClickHouse insert |
| **Memory usage** | ~50-200 MB | Streaming with SSCursor, no full table buffering |

**Factors affecting performance:**
- Row size and number of columns
- Network latency between MySQL, replicator, and ClickHouse
- ClickHouse cluster load and insert queue
- MySQL binlog event density

**For high-throughput scenarios (>5,000 events/sec):**
- Consider partitioning tables and running multiple replicators
- Use dedicated MySQL replica for CDC to avoid impacting production
- Ensure ClickHouse has sufficient insert capacity

### CDC Limitations & Event Loss Scenarios

The CDC replicator provides **at-least-once** delivery semantics. Below are scenarios where events may be lost or duplicated:

| Scenario | Risk | Impact |
|----------|------|--------|
| **Normal operation** | None | Events processed, position saved every 5 seconds |
| **Graceful shutdown (SIGTERM)** | None | Final position saved before exit |
| **Crash / Kill -9** | Duplicates | May reprocess up to 5 seconds of events |
| **ClickHouse unavailable** | **Loss** | Events lost if insert fails without retry |
| **MySQL binlog expired** | **Loss** | Events permanently lost - requires full re-sync |
| **Network partition** | **Loss** | Events during disconnect may be lost |
| **Replicator offline > binlog retention** | **Loss** | Binlog purged before replicator catches up |
| **DDL changes (ALTER TABLE)** | **Inconsistency** | Schema changes not automatically detected |

### Why Duplicates Are Safe

ClickHouse uses `ReplacingMergeTree` with `_version` column. When duplicates occur:
- Same row with same `_version` → deduplicated automatically
- Same row with different `_version` → latest version wins after OPTIMIZE or query with FINAL

### Preventing Event Loss

#### 1. Keep MySQL binlogs longer

Configure MySQL retention to exceed your maximum expected downtime:

```ini
# my.cnf
expire_logs_days = 7
# or for MySQL 8.0+
binlog_expire_logs_seconds = 604800  # 7 days
```

#### 2. Monitor replication lag

Set up alerts if the replicator is offline or lagging:

```bash
# Check last sync time
cat /data/binlog_position.json | jq '.timestamp'

# Compare with current time - alert if > threshold
LAST_SYNC=$(cat /data/binlog_position.json | jq -r '.timestamp')
NOW=$(date +%s)
LAG=$((NOW - ${LAST_SYNC%.*}))
if [ $LAG -gt 3600 ]; then
  echo "ALERT: Replication lag is ${LAG} seconds"
fi
```

#### 3. Use persistent volumes

Always mount `/data` to preserve binlog position across restarts:

```bash
docker run -v mysql_ch_sync_data:/data ...
```

#### 4. Implement external health checks

Monitor the replicator container and restart if unhealthy:

```yaml
# docker-compose.yml
services:
  replicator:
    image: gabrielheck/mysql-clickhouse-sync:latest
    healthcheck:
      test: ["CMD", "test", "-f", "/data/binlog_position.json"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: unless-stopped
```

#### 5. Periodic full re-sync (for critical data)

For data that cannot tolerate any loss, schedule periodic full syncs:

```bash
# Weekly full re-sync (cron job)
0 3 * * 0 docker exec mysql-ch-sync rm -f /data/binlog_position.json && docker restart mysql-ch-sync
```

### Recovery from Binlog Expiration

If MySQL returns error "Could not find first log file name in binary log index file", the binlog has expired:

```bash
# 1. Stop the replicator
docker stop mysql-ch-sync

# 2. Remove position file to trigger full re-sync
docker exec mysql-ch-sync rm /data/binlog_position.json

# 3. Optionally, drop existing tables for clean slate
# Set REPLICATION_DROP_EXISTING=true

# 4. Restart - will do initial sync + resume CDC
docker start mysql-ch-sync
```

### Known Limitations

| Limitation | Description | Workaround |
|------------|-------------|------------|
| **No DDL replication** | Schema changes (ALTER, DROP) are not replicated | Manually apply DDL to ClickHouse |
| **Single database** | Only one MySQL database per instance | Run multiple replicator instances |
| **No retry on insert failure** | Failed ClickHouse inserts are not retried | Ensure ClickHouse availability |
| **No GTID support** | Uses file+position, not GTID | Ensure binlog file naming is consistent |
| **No filtering** | Cannot filter rows (WHERE clause) | Filter in ClickHouse views |
| **UNSIGNED not detected** | UNSIGNED INT mapped to signed types | May overflow on large values |

## Architecture

```
src/
├── config.py           # Environment configuration with Pydantic
├── mysql_client.py     # MySQL connection and data extraction
├── clickhouse_client.py # ClickHouse connection and insertion
├── schema_converter.py # MySQL to ClickHouse schema conversion
├── replicator.py       # Snapshot replication orchestration
├── cdc_replicator.py   # CDC/binlog replication
└── main.py             # Entry point
```

### Dependencies

- `pymysql` - MySQL connection
- `mysql-replication` - Binlog reading for CDC
- `clickhouse-connect` - ClickHouse HTTP client with LZ4 compression
- `pydantic-settings` - Environment configuration
- `structlog` - Structured JSON logging

## License

MIT
