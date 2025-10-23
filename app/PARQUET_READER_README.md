# Parquet to Polars Reader

A Python script for reading, validating, and analyzing Parquet files created by the Redpanda to Parquet Collector. This tool provides comprehensive data analysis, schema validation, and programmatic access to your market data.

## Overview

The `parquet_to_polars.py` script reads Parquet files created by `redpanda_to_parquet_collector.py`, validates data against typed dataclasses matching Redpanda topic schemas, and displays detailed statistics and previews.

### Key Features

- **Schema Validation** - Validates data against predefined dataclasses for different security types
- **Rich Terminal Output** - Beautiful colored tables and progress indicators using Rich library
- **Date-Partitioned Support** - Handles the `YYYY/MM/DD/topic.parquet` directory structure
- **Programmatic Access** - Full API for loading data in custom applications
- **Statistics & Analysis** - Automatic generation of numeric statistics and data summaries
- **Security Type Detection** - Auto-detects market data types (IND, FUT, OPT, STK, BAG, ACCOUNT_VALUES)

## Quick Start

### Prerequisites

- Python 3.11+
- Required packages: `polars`, `rich`
- Parquet files created by `redpanda_to_parquet_collector.py`

### Installation

```bash
# Install dependencies
pip install polars rich

# Or install from requirements.txt
pip install -r app/requirements.txt
```

### Basic Usage

```bash
# Analyze all parquet files
python app/parquet_to_polars.py

# Analyze specific date
python app/parquet_to_polars.py 2025/10/23

# Analyze today's data
python app/parquet_to_polars.py today

# Show help
python app/parquet_to_polars.py --help
```

## Docker Usage

Run the parquet reader inside a Docker container (no Python installation required on host):

### Quick Start

```bash
# Build the image (if not already built)
docker-compose build

# Read all parquet files
docker-compose run --rm parquet-reader

# Read specific date
docker-compose run --rm parquet-reader python parquet_to_polars.py 2025/10/23

# Read today's data
docker-compose run --rm parquet-reader python parquet_to_polars.py today
```

### Why Use Docker?

- No need to install Python or dependencies on your host machine
- Same environment as the collector (consistent behavior)
- Easy to run on any machine with Docker installed
- Isolated from your host system

### Volume Mounts

The Docker container reads from `/app/output` which is mapped to `/data/datalake/redpanda_backup` on your host (configurable in docker-compose.yml).

### Custom Output Directory

To read from a different directory:

```bash
docker-compose run --rm \
  -v /path/to/your/parquet/files:/app/output \
  parquet-reader
```

### Helper Script (Linux/Mac)

For easier execution, use the provided helper script:

```bash
# Make executable (one-time setup)
chmod +x scripts/read-parquet.sh

# Read all parquet files
./scripts/read-parquet.sh

# Read specific date
./scripts/read-parquet.sh 2025/10/23

# Read today's data
./scripts/read-parquet.sh today
```

### Usage Options Summary

| Method | Command | Best For |
|--------|---------|----------|
| **Docker (Recommended)** | `docker-compose run --rm parquet-reader` | No Python installation, consistent environment |
| **Helper Script** | `./scripts/read-parquet.sh` | Linux/Mac users who want convenience |
| **Direct Python** | `python app/parquet_to_polars.py` | Users with Python environment |

## Command Line Interface

### Usage Patterns

| Command | Description |
|---------|-------------|
| `python parquet_to_polars.py` | Read and analyze all parquet files |
| `python parquet_to_polars.py 2025/10/23` | Read files from specific date |
| `python parquet_to_polars.py today` | Read today's files (shortcut) |
| `python parquet_to_polars.py --help` | Show usage information |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OUTPUT_DIR` | `./data/redpanda_parquet` | Directory containing parquet files |

## Output Explanation

When you run the script, you'll see comprehensive analysis for each parquet file:

### 1. File Summary
```
Summary: market_data.parquet
Rows: 1,245,678
Columns: 25

Schema:
┌─────────────────┬─────────────┬────────────┐
│ Column          │ Type       │ Null Count │
├─────────────────┼─────────────┼────────────┤
│ kafka_topic     │ String      │ 0          │
│ kafka_partition │ Int64       │ 0          │
│ kafka_offset    │ Int64       │ 0          │
│ conId           │ Int64       │ 0          │
│ symbol          │ String      │ 0          │
│ mid_price       │ Float64     │ 0          │
└─────────────────┴─────────────┴────────────┘
```

### 2. Schema Validation
```
Schema Validation (sec_type: OPT)
┌─────────────┬─────────────────┬─────────────────────┐
│ Status      │ Field           │ Expected Type        │
├─────────────┼─────────────────┼─────────────────────┤
│ [OK] Present│ conId           │ <class 'int'>       │
│ [OK] Present│ symbol          │ <class 'str'>       │
│ [OK] Present│ strike          │ <class 'float'>      │
│ [X] Missing │ implied_vol     │ <class 'float'>     │
│ [+] Extra   │ custom_field    │ Float64              │
└─────────────┴─────────────────┴─────────────────────┘
```

### 3. Data Preview
Shows the first 5 rows of data with all columns.

### 4. Numeric Statistics
```
Numeric Statistics
┌─────────────┬─────────┬─────────┬─────────┬─────────┐
│ Column      │ Min    │ Max     │ Mean    │ Std Dev │
├─────────────┼────────┼─────────┼─────────┼─────────┤
│ mid_price   │ 150.25 │ 152.30  │ 151.28  │ 0.45    │
│ volume      │ 1      │ 50000   │ 1250.5  │ 1250.2  │
│ strike      │ 100.0  │ 500.0   │ 300.0   │ 50.0    │
└─────────────┴────────┴─────────┴─────────┴─────────┘
```

## Programmatic Usage

The `ParquetReader` class provides a full API for programmatic access:

### Basic Initialization

```python
from app.parquet_to_polars import ParquetReader

# Use default directory
reader = ParquetReader()

# Specify custom directory
reader = ParquetReader("/path/to/parquet/files")
```

### Loading Data

```python
# Load all files from a specific date
dataframes = reader.load_topics_batch("2025/10/23")

# Load specific topics only
dataframes = reader.load_topics_batch(
    "2025/10/23", 
    topics=["market_data", "trades"]
)

# Silent loading (no console output)
dataframes = reader.load_topics_batch(
    "2025/10/23", 
    silent=True
)
```

### Discovery Methods

```python
# Get all available dates
dates = reader.get_available_dates()
# Returns: ['2025/10/21', '2025/10/22', '2025/10/23']

# Get topics for a specific date
topics = reader.get_topics_for_date("2025/10/23")
# Returns: ['market_data', 'trades', 'quotes']
```

### Full Analysis

```python
# Complete analysis with display output
results = reader.load_and_display_all("2025/10/23")

# Access individual dataframes
for topic_name, data in results.items():
    df = data["dataframe"]
    sec_type = data["sec_type"]
    filepath = data["filepath"]
    
    print(f"Topic: {topic_name}")
    print(f"Security Type: {sec_type}")
    print(f"Records: {len(df):,}")
```

### Converting to Typed Objects

```python
# Convert DataFrame to typed dataclass instances
df = reader.load_topics_batch("2025/10/23")["market_data"]
sec_type = reader.detect_security_type(df, "market_data.parquet")

if sec_type:
    typed_objects = reader.to_dataclass(df, sec_type)
    for obj in typed_objects[:5]:  # First 5 objects
        print(f"Symbol: {obj.symbol}, Price: {obj.mid_price}")
```

## Supported Schemas

The script supports 6 different security types with predefined dataclasses:

### Security Type Mapping

| Topic Name | Security Type | Description |
|------------|---------------|-------------|
| `spx_index` | IND | S&P 500 Index |
| `vix_index` | IND | VIX Volatility Index |
| `vix1d_index` | IND | VIX 1-Day Index |
| `vvix_index` | IND | VVIX Volatility Index |
| `es_futures` | FUT | E-mini S&P 500 Futures |
| `spx_options` | OPT | SPXW Options with Greeks |
| `spx_multileg` | BAG | Iron Butterfly Strategies |
| `trading_account_values` | ACCOUNT_VALUES | Trading Account Data |
| `ibkr_account_values` | ACCOUNT_VALUES | IBKR Account Data |

### Schema Details

#### IND (Indices) - IndexMarketData
- **Base Fields**: conId, symbol, local_symbol, symbol_key, currency, exchange, sec_type, timestamp, unix_timestamp_ms
- **Calculated Fields**: mid_price, spread, has_liquidity
- **Index Fields**: close, last, last_size, high, low, index_value

#### FUT (Futures) - FuturesMarketData
- **Base Fields**: Same as IND
- **Futures Fields**: last, last_size, bid_size, ask_size, high, low, close, volume
- **Optional Fields**: bid, ask

#### OPT (Options) - OptionsMarketData
- **Base Fields**: Same as IND
- **Options Fields**: bid_size, ask_size, last_size, volume, close, strike, right, expiry, putOpenInterest, callOpenInterest
- **Greeks**: implied_vol, delta, gamma, theta, vega, underlying_price
- **Gamma Exposure**: CallGammaExposure, PutGammaExposure, net_gamma_exposure, volume_weighted_*_gamma
- **Timing**: days_to_expiration, minutes_to_expiration

#### STK (Stocks) - StockMarketData
- **Base Fields**: Same as IND
- **Stock Fields**: last, last_size, bid, bid_size, ask, ask_size, high, low, close, volume

#### BAG (Multi-leg) - MultiLegOptionsData
- **Base Fields**: Same as IND (conId is comma-separated string)
- **Strategy Fields**: strategy_type, center_strike, wing_width, expiration, combo_legs, leg_count, leg_actions, leg_conIds
- **Market Fields**: bid, bid_size, ask, ask_size, last, last_size, volume, high, low, close

#### ACCOUNT_VALUES - AccountValuesData
- **Fields**: account, timestamp, values (dict), raw_count, processed_count

## Configuration

### Directory Structure

The script expects parquet files in a date-partitioned structure:

```
./data/redpanda_parquet/
├── 2025/
│   ├── 10/
│   │   ├── 21/
│   │   │   ├── market_data.parquet
│   │   │   ├── trades.parquet
│   │   │   └── quotes.parquet
│   │   ├── 22/
│   │   │   └── market_data.parquet
│   │   └── 23/
│   │       ├── market_data.parquet
│   │       └── trades.parquet
│   └── 11/
│       └── 01/
│           └── market_data.parquet
```

### Environment Variables

Set the `OUTPUT_DIR` environment variable to point to your parquet directory:

```bash
# Linux/Mac
export OUTPUT_DIR="/path/to/your/parquet/files"

# Windows
set OUTPUT_DIR=C:\path\to\your\parquet\files

# Or in Python
import os
os.environ["OUTPUT_DIR"] = "/path/to/your/parquet/files"
```

## Examples

### Example 1: Analyze All Data

```bash
python app/parquet_to_polars.py
```

This will scan the entire parquet directory and analyze all files, showing comprehensive statistics for each topic.

### Example 2: Analyze Specific Date

```bash
python app/parquet_to_polars.py 2025/10/23
```

Analyzes only the files from October 23, 2025.

### Example 3: Programmatic Data Loading

```python
from app.parquet_to_polars import ParquetReader

# Initialize reader
reader = ParquetReader("/data/datalake/redpanda_backup")

# Get available dates
dates = reader.get_available_dates()
print(f"Available dates: {dates}")

# Load specific date
data = reader.load_topics_batch("2025/10/23", silent=True)

# Analyze each topic
for topic, df in data.items():
    print(f"\n{topic}:")
    print(f"  Records: {len(df):,}")
    print(f"  Columns: {len(df.columns)}")
    
    # Get numeric columns
    numeric_cols = [col for col in df.columns 
                   if df[col].dtype in ['Float64', 'Int64']]
    print(f"  Numeric columns: {len(numeric_cols)}")
```

### Example 4: Custom Analysis

```python
from app.parquet_to_polars import ParquetReader
import polars as pl

reader = ParquetReader()

# Load market data
df = reader.load_topics_batch("2025/10/23")["market_data"]

# Custom analysis
print("Price Statistics:")
print(f"  Min price: ${df['mid_price'].min():.2f}")
print(f"  Max price: ${df['mid_price'].max():.2f}")
print(f"  Avg price: ${df['mid_price'].mean():.2f}")

# Volume analysis
volume_stats = df.group_by("symbol").agg([
    pl.col("volume").sum().alias("total_volume"),
    pl.col("volume").mean().alias("avg_volume"),
    pl.col("volume").max().alias("max_volume")
]).sort("total_volume", descending=True)

print("\nTop symbols by volume:")
print(volume_stats.head(10))
```

### Example 5: Schema Validation

```python
from app.parquet_to_polars import ParquetReader

reader = ParquetReader()

# Load and validate options data
df = reader.load_topics_batch("2025/10/23")["spx_options"]
sec_type = reader.detect_security_type(df, "spx_options.parquet")

if sec_type == "OPT":
    # Check for required fields
    required_fields = ["strike", "right", "expiry", "implied_vol", "delta"]
    missing_fields = [field for field in required_fields if field not in df.columns]
    
    if missing_fields:
        print(f"Missing required fields: {missing_fields}")
    else:
        print("All required fields present!")
        
        # Convert to typed objects
        options = reader.to_dataclass(df, sec_type)
        print(f"Successfully created {len(options)} option objects")
```

## Troubleshooting

### Common Issues

#### Directory Not Found
```
[ERROR] Directory not found: ./data/redpanda_parquet
```

**Solution**: Ensure the parquet directory exists and set the correct `OUTPUT_DIR`:
```bash
# Check if directory exists
ls -la ./data/redpanda_parquet

# Set correct path
export OUTPUT_DIR="/correct/path/to/parquet/files"
```

#### No Parquet Files Found
```
[ERROR] No parquet files found in ./data/redpanda_parquet
```

**Solution**: Verify files exist and check directory structure:
```bash
# Check for parquet files
find ./data/redpanda_parquet -name "*.parquet" -type f

# Check directory structure
tree ./data/redpanda_parquet
```

#### Schema Validation Warnings
```
[WARN] 3 missing field(s)
[INFO] 2 extra field(s)
```

**Solution**: This is normal when schemas evolve. The script handles missing/extra fields gracefully:
- Missing fields are logged but don't prevent processing
- Extra fields are preserved in the data
- Use the validation output to understand schema differences

#### Missing Dependencies
```
ModuleNotFoundError: No module named 'polars'
```

**Solution**: Install required packages:
```bash
pip install polars rich

# Or install from requirements
pip install -r app/requirements.txt
```

#### Permission Denied
```
PermissionError: [Errno 13] Permission denied: '/path/to/file.parquet'
```

**Solution**: Check file permissions:
```bash
# Fix permissions (Linux/Mac)
chmod 644 /path/to/file.parquet

# Or run with different user
sudo python app/parquet_to_polars.py
```

### Performance Tips

1. **Use Silent Mode**: For programmatic access, use `silent=True` to disable console output
2. **Filter Topics**: Load only needed topics to reduce memory usage
3. **Date Filtering**: Use specific dates instead of scanning all files
4. **Memory Management**: For large files, process in batches or use lazy loading

### Debug Mode

Enable debug output by modifying the script:

```python
# Add at the top of parquet_to_polars.py
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Integration with Redpanda Collector

This script is designed to work seamlessly with the `redpanda_to_parquet_collector.py`:

1. **Data Flow**: Redpanda → Collector → Parquet Files → Reader
2. **Schema Consistency**: Both scripts use the same dataclass definitions
3. **Directory Structure**: Reader expects the same date-partitioned structure
4. **Validation**: Reader validates data matches expected schemas

### Typical Workflow

```bash
# 1. Collect data from Redpanda
docker-compose run --rm redpanda-parquet-collector

# 2. Analyze collected data
python app/parquet_to_polars.py today

# 3. Programmatic analysis
python -c "
from app.parquet_to_polars import ParquetReader
reader = ParquetReader()
data = reader.load_topics_batch('2025/10/23')
print(f'Loaded {len(data)} topics')
"
```

## License

(Add your license information here)

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify your parquet files were created by the Redpanda collector
3. Ensure all dependencies are installed
4. Review the schema validation output for data issues

---

**Last Updated**: January 2025
