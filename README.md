# Redpanda to Parquet Collector

Streams data from Redpanda topics and saves to date-partitioned Parquet files.

## Features

- **MessagePack and JSON decoding** - Tries MessagePack first, falls back to JSON
- **Automatic nested dictionary flattening** - Extracts 'data' field to top level
- **Incremental updates** - Only processes new messages since last run
- **Parallel topic processing** - With progress bars for monitoring
- **Date-partitioned output** - Organized by `YYYY/MM/DD/topic.parquet`
- **Performance optimized** - 100K message batches, aggressive consumer settings
- **Docker containerized** - Easy deployment on any Linux host
- **No consumer group pollution** - Unique consumer groups per run, no offset commits

## Architecture

```
Redpanda Topics → Python Consumer → Polars DataFrame → Parquet Files
                  (unique UUID)                            ↓
                  (no offset commit)         /data/datalake/redpanda_backup/
                                                    2025/10/17/topic1.parquet
```

### Consumer Behavior

**Important:** This application is designed to always have access to ALL data in your Redpanda topics:

- ✅ **Unique consumer group per run** - Each execution generates a UUID-based consumer group ID
- ✅ **No offset commits** - `enable.auto.commit=False` ensures offsets are never saved to Kafka/Redpanda
- ✅ **Full data availability** - All messages remain available for reading on every run
- ✅ **Incremental via Parquet files** - Progress tracking is done by reading existing Parquet files, NOT Kafka offsets

**Why this matters:** Without unique consumer groups, Kafka/Redpanda would "remember" where you left off, preventing you from re-reading historical data or getting complete exports.

## Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Redpanda instance accessible from the host
- Write permissions to `/data/datalake/redpanda_backup/` directory

### Setup

1. **Create output directory on Linux host:**
   ```bash
   sudo mkdir -p /data/datalake/redpanda_backup
   sudo chown -R 1000:1000 /data/datalake/redpanda_backup  # Match container user
   ```

2. **Clone or copy this repository**

3. **Update `docker-compose.yml`** (if needed):
   - Change `BOOTSTRAP_SERVERS` to your Redpanda address
   - Adjust volume mount if using different output path
   - Modify performance settings (BATCH_SIZE, MAX_WORKERS)

### Build the Docker Image

```bash
docker-compose build
```

## Usage

### Option 1: One-Time Run (Recommended)

Run the container once, process all topics, then exit:

```bash
# Build and run
docker-compose up --build

# Clean up stopped container
docker-compose down
```

**When to use:**
- Initial data export
- Scheduled/cron jobs
- Ad-hoc backups

### Option 2: Run with docker-compose run

For more control over individual runs (includes interactive progress bars):

```bash
# Build first
docker-compose build

# Run once (auto-removes container after exit)
# You'll see live progress bars and colored output
docker-compose run --rm redpanda-parquet-collector

# Run with custom environment variables
docker-compose run --rm \
  -e SKIP_EXISTING_CHECK=false \
  -e BATCH_SIZE=50000 \
  redpanda-parquet-collector
```

**Note:** Progress bars and colored output work best with `docker-compose run` as it allocates a TTY by default.

### Option 3: Continuous/Daemon Mode

If you want the container to restart automatically (change `restart: no` to `restart: unless-stopped` in docker-compose.yml):

```bash
# Start in background
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## Configuration

All configuration is done via environment variables in `docker-compose.yml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `192.168.1.110:19092` | Redpanda/Kafka bootstrap servers |
| `OUTPUT_DIR` | `/app/output` | Container output path (mounted to host) |
| `MAX_MESSAGES` | _(empty/None)_ | Limit messages per topic (for testing) |
| `BATCH_SIZE` | `100000` | Messages per Parquet write (higher = faster, more memory) |
| `MAX_WORKERS` | `4` | Parallel topic processing threads |
| `SKIP_EXISTING_CHECK` | `true` | `true` = overwrite (fast), `false` = incremental (slower) |
| `TERM` | `xterm-256color` | Terminal type for colored output (pre-configured) |
| `PYTHONUNBUFFERED` | `1` | Disable output buffering for real-time logs (pre-configured) |

### Configuration Modes

#### Fast/Fresh Export Mode (Default)
```yaml
- SKIP_EXISTING_CHECK=true
```
- **Faster**: No read-modify-write cycle
- **Use case**: Initial export, full refresh, testing
- **Behavior**: Overwrites existing files for same date

#### Incremental Mode
```yaml
- SKIP_EXISTING_CHECK=false
```
- **Slower**: Reads existing files and appends new data
- **Use case**: Ongoing backups, preserving historical data
- **Behavior**: Only processes messages newer than last run

## Output Structure

Files are organized by date in a hierarchical structure:

```
/data/datalake/redpanda_backup/
├── 2025/
│   ├── 10/
│   │   ├── 16/
│   │   │   ├── market_data.parquet
│   │   │   ├── trades.parquet
│   │   │   └── quotes.parquet
│   │   └── 17/
│   │       ├── market_data.parquet
│   │       └── trades.parquet
│   └── 11/
│       └── 01/
│           └── market_data.parquet
```

### Parquet File Schema

Each Parquet file contains:

- **Kafka Metadata Columns:**
  - `kafka_topic` - Topic name
  - `kafka_partition` - Partition number
  - `kafka_offset` - Message offset
  - `kafka_timestamp` - Message timestamp (ms since epoch)
  - `kafka_key` - Message key (if present)

- **Message Data Columns:**
  - All fields from the message payload (flattened)
  - Nested dictionaries flattened with `_` separator
  - Lists converted to JSON strings

## Scheduled Runs with Cron

For automated backups, add to your Linux crontab:

```bash
# Edit crontab
crontab -e

# Run every hour at minute 0
0 * * * * cd /path/to/redpanda-to-parquet-writer && docker-compose up --build >> /var/log/redpanda-backup.log 2>&1 && docker-compose down

# Run daily at 2 AM
0 2 * * * cd /path/to/redpanda-to-parquet-writer && docker-compose up --build && docker-compose down

# Run every 15 minutes (incremental mode)
*/15 * * * * cd /path/to/redpanda-to-parquet-writer && docker-compose run --rm -e SKIP_EXISTING_CHECK=false redpanda-parquet-collector
```

## Performance Tuning

### For Maximum Throughput

```yaml
environment:
  - BATCH_SIZE=200000      # Larger batches (more memory)
  - MAX_WORKERS=8          # More parallel processing
  - SKIP_EXISTING_CHECK=true  # Skip read-modify-write
```

### For Memory-Constrained Systems

```yaml
environment:
  - BATCH_SIZE=50000       # Smaller batches
  - MAX_WORKERS=2          # Fewer threads
  - SKIP_EXISTING_CHECK=true
```

### For Incremental Updates

```yaml
environment:
  - BATCH_SIZE=100000
  - MAX_WORKERS=4
  - SKIP_EXISTING_CHECK=false  # Preserve existing data
```

## Message Format

### Expected MessagePack/JSON Structure

```json
{
  "event_type": "market_data",
  "source": "ibkr",
  "data": {
    "symbol": "AAPL",
    "bid": 150.25,
    "ask": 150.30,
    "volume": 1000000
  },
  "metadata": {
    "exchange": "NASDAQ",
    "timestamp": "2025-10-17T10:30:00Z"
  }
}
```

### Flattening Behavior

The `data` dictionary contents are extracted to top-level columns:

```
Columns in Parquet:
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_timestamp
- kafka_key
- event_type
- source
- symbol          ← from data.symbol
- bid             ← from data.bid
- ask             ← from data.ask
- volume          ← from data.volume
- metadata_exchange    ← from metadata.exchange
- metadata_timestamp   ← from metadata.timestamp
```

## Monitoring

### Visual Output & Progress Bars

The application uses the **Rich** library to provide beautiful terminal output including:

- 🎨 **Colored output** - Green for success, red for errors, yellow for warnings
- 📊 **Live progress bars** - Per-topic and overall progress
- ⚡ **Real-time throughput** - Messages per second for each topic
- 📈 **Completion percentages** - Track progress across all topics

**Example Output:**
```
Session ID: a3f8c2b1
Using unique consumer groups: reader-a3f8c2b1-<topic>

Found 5 topics: market_data, trades, quotes, orders, positions

⠹ Overall Progress ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 3/5  60%  0:00:45
  ✓ market_data - 1,245,678 msgs (25,000 msg/s)
  ✓ trades - 892,345 msgs (18,000 msg/s)
  ⠹ quotes - 12,450 msg/s ━━━━━━━━━━━━━━━━━━━━━ 450K/1.2M  37%
  ⠹ orders - 8,200 msg/s ━━━━━━━━━━━━━━━━ 123K/890K  14%
  ⏸ positions - initializing...
```

### View Live Progress

```bash
# Best: Interactive mode with full rich output
docker-compose run --rm redpanda-parquet-collector

# Also shows progress bars (foreground)
docker-compose up

# Limited: Background mode (progress bars don't update smoothly)
docker-compose up -d
docker-compose logs -f  # Shows text output but not interactive progress
```

### Terminal Requirements

For the best visual experience:
- ✅ Use `docker-compose run` or `docker-compose up` (foreground)
- ✅ Terminal with 256-color support (most modern terminals)
- ✅ Terminal width of at least 80 characters
- ❌ Don't use `docker-compose run -T` (disables TTY)
- ❌ Background mode (`-d`) won't show live progress bars

### Check Output Files

```bash
# List generated files
ls -lR /data/datalake/redpanda_backup/

# Check file size
du -sh /data/datalake/redpanda_backup/*

# Count messages in a file (requires DuckDB or Polars)
python -c "import polars as pl; print(len(pl.read_parquet('/data/datalake/redpanda_backup/2025/10/17/market_data.parquet')))"
```

## Troubleshooting

### Permission Denied on Output Directory

```bash
# Fix permissions
sudo chown -R 1000:1000 /data/datalake/redpanda_backup

# Or run with different user (add to docker-compose.yml)
user: "0:0"  # Run as root (not recommended)
```

### Not Reading All Records / Missing Data

**Symptoms:** The script completes but not all expected records are in the Parquet files.

**Solution:** This has been fixed in the current version. The script now:
- Uses a **unique consumer group ID** for every run (UUID-based)
- Has `enable.auto.commit=False` to prevent offset persistence
- Always has access to all data in Kafka/Redpanda

**To verify the fix:**
```bash
# Look for the Session ID in the output
docker-compose run --rm redpanda-parquet-collector

# Should show:
# Session ID: a3f8c2b1
# Using unique consumer groups: reader-a3f8c2b1-<topic>
```

**If using an older version:** Manually delete consumer groups from Redpanda:
```bash
# List consumer groups
rpk group list --brokers 192.168.1.110:19092

# Delete old consumer groups
rpk group delete reader-<topic> --brokers 192.168.1.110:19092
```

### Can't Connect to Redpanda

1. Check `BOOTSTRAP_SERVERS` is correct
2. Ensure Redpanda is accessible from Docker container
3. Try `network_mode: host` in docker-compose.yml (already set)
4. Test connectivity:
   ```bash
   docker-compose run --rm redpanda-parquet-collector \
     bash -c "apt-get update && apt-get install -y curl && curl -v telnet://192.168.1.110:19092"
   ```

### Out of Memory Errors

Reduce `BATCH_SIZE` and `MAX_WORKERS`:

```yaml
environment:
  - BATCH_SIZE=25000
  - MAX_WORKERS=2
```

### Slow Performance

1. **Enable fast mode:**
   ```yaml
   - SKIP_EXISTING_CHECK=true
   ```

2. **Increase batch size:**
   ```yaml
   - BATCH_SIZE=200000
   ```

3. **More parallel workers:**
   ```yaml
   - MAX_WORKERS=8
   ```

### Container Exits Immediately

Check logs:
```bash
docker-compose logs

# Or run interactively
docker-compose run --rm redpanda-parquet-collector bash
```

### Progress Bars Not Showing / Garbled Output

**Symptoms:** Progress bars appear as text characters, colors missing, or output is garbled.

**Solutions:**

1. **Ensure TTY is allocated:**
   ```bash
   # Good (allocates TTY)
   docker-compose run --rm redpanda-parquet-collector
   
   # Bad (no TTY)
   docker-compose run --rm -T redpanda-parquet-collector
   ```

2. **Check terminal capabilities:**
   ```bash
   # Test your terminal supports colors
   echo $TERM  # Should show something like "xterm-256color"
   ```

3. **Force color output (if needed):**
   The `TERM=xterm-256color` and `PYTHONUNBUFFERED=1` environment variables are already set in docker-compose.yml.

4. **Verify Rich library is working:**
   ```bash
   # Test Rich inside container
   docker-compose run --rm redpanda-parquet-collector \
     python -c "from rich.console import Console; Console().print('[bold green]Success![/bold green]')"
   ```

5. **For automation/cron jobs without TTY:**
   The script will automatically fall back to simpler text output when no TTY is available.

## Development

### Local Testing (Without Docker)

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate  # Windows

# Install dependencies
pip install -r app/requirements.txt

# Set environment variables
export BOOTSTRAP_SERVERS="192.168.1.110:19092"
export OUTPUT_DIR="./data/redpanda_parquet"
export SKIP_EXISTING_CHECK="true"

# Run
python app/redpanda_to_parquet_collector.py
```

### Building for Different Architectures

```bash
# Build for ARM64 (e.g., Raspberry Pi, AWS Graviton)
docker buildx build --platform linux/arm64 -t redpanda-parquet-collector:arm64 -f docker/Dockerfile .

# Build multi-platform
docker buildx build --platform linux/amd64,linux/arm64 -t redpanda-parquet-collector:latest -f docker/Dockerfile .
```

## Project Structure

```
.
├── app/
│   ├── redpanda_to_parquet_collector.py  # Main application
│   └── requirements.txt                   # Python dependencies
├── docker/
│   └── Dockerfile                         # Multi-stage Docker build
├── docker-compose.yml                     # Docker Compose configuration
└── README.md                              # This file
```

## Dependencies

- **Python 3.11** - Runtime
- **Polars** - Fast DataFrame library for Parquet I/O
- **confluent-kafka** - Kafka/Redpanda consumer
- **msgpack** - MessagePack decoder
- **rich** - Terminal progress bars and formatting
- **pydantic** - Data validation
- **lz4** - Compression support

## License

(Add your license here)

## Support

For issues or questions:
1. Check logs: `docker-compose logs`
2. Verify configuration in `docker-compose.yml`
3. Review troubleshooting section above
4. Open an issue on GitHub (if applicable)

---

**Last Updated:** October 2025

