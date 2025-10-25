"""
Parquet to Polars Reader - Read and validate Redpanda topic data from parquet files.

This script reads parquet files created by redpanda_to_parquet_collector.py,
validates data against typed dataclasses matching Redpanda topic schemas,
and displays statistics and previews.

Supports date-partitioned directory structure: ./redpanda_parquet/YYYY/MM/DD/
"""

import os
import sys
from dataclasses import dataclass, fields
from typing import Optional, Dict, List, Any
from pathlib import Path
from datetime import datetime
import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

# Use legacy_windows=False for better Unicode support on Windows
console = Console(legacy_windows=False)

# Configuration - read from environment variables with sensible defaults
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data/redpanda_parquet")


# ============================================================================
# Topic-to-Schema Mapping
# ============================================================================

TOPIC_TO_SEC_TYPE = {
    "spx_index": "IND",
    "vix_index": "IND",
    "vix1d_index": "IND",
    "vvix_index": "IND",
    "es_futures": "FUT",
    "spx_options": "OPT",
    "spx_multileg": "BAG",
    "trading_account_values": "ACCOUNT_VALUES",
    "ibkr_account_values": "ACCOUNT_VALUES",
}


# ============================================================================
# Dataclass Models (Based on MICROSERVICE_TWS.md Section 2)
# ============================================================================

@dataclass
class IndexMarketData:
    """Index Market Data (sec_type: IND) - SPX, VIX, VIX1D, VVIX"""
    # Base fields
    conId: int
    symbol: str
    local_symbol: str
    symbol_key: str
    currency: str
    exchange: str
    sec_type: str
    timestamp: str
    unix_timestamp_ms: int
    
    # Calculated fields
    mid_price: float
    spread: float
    has_liquidity: bool
    
    # Index-specific fields
    close: float
    last: Optional[float] = None
    last_size: Optional[int] = None
    high: Optional[float] = None
    low: Optional[float] = None
    index_value: Optional[float] = None


@dataclass
class FuturesMarketData:
    """Futures Market Data (sec_type: FUT) - ES Futures"""
    # Base fields
    conId: int
    symbol: str
    local_symbol: str
    symbol_key: str
    currency: str
    exchange: str
    sec_type: str
    timestamp: str
    unix_timestamp_ms: int
    
    # Calculated fields
    mid_price: float
    spread: float
    has_liquidity: bool
    
    # Futures-specific required fields
    last: float
    last_size: int
    bid_size: int
    ask_size: int
    high: float
    low: float
    close: float
    volume: int
    
    # Optional fields
    bid: Optional[float] = None
    ask: Optional[float] = None


@dataclass
class OptionsMarketData:
    """Options Market Data (sec_type: OPT) - SPXW Options"""
    # Base fields
    conId: int
    symbol: str
    local_symbol: str
    symbol_key: str
    currency: str
    exchange: str
    sec_type: str
    timestamp: str
    unix_timestamp_ms: int
    
    # Calculated fields
    mid_price: float
    spread: float
    has_liquidity: bool
    
    # Options-specific required fields
    bid_size: int
    ask_size: int
    last_size: int
    volume: int
    close: float
    strike: float
    right: str
    expiry: str
    putOpenInterest: int
    callOpenInterest: int
    implied_vol: float
    delta: float
    gamma: float
    theta: float
    vega: float
    underlying_price: float
    
    # Optional pricing fields
    last: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    
    # Enhanced gamma exposure fields
    CallGammaExposure: Optional[float] = None
    PutGammaExposure: Optional[float] = None
    net_gamma_exposure: Optional[float] = None
    volume_weighted_call_gamma: Optional[float] = None
    volume_weighted_put_gamma: Optional[float] = None
    volume_weighted_net_gamma: Optional[float] = None
    dte_weight: Optional[float] = None
    volume_weight: Optional[float] = None
    combined_weight: Optional[float] = None
    
    # Expiration timing fields
    days_to_expiration: Optional[int] = None
    minutes_to_expiration: Optional[int] = None


@dataclass
class StockMarketData:
    """Stock Market Data (sec_type: STK)"""
    # Base fields
    conId: int
    symbol: str
    local_symbol: str
    symbol_key: str
    currency: str
    exchange: str
    sec_type: str
    timestamp: str
    unix_timestamp_ms: int
    
    # Calculated fields
    mid_price: float
    spread: float
    has_liquidity: bool
    
    # Stock-specific required fields
    last: float
    last_size: int
    bid: float
    bid_size: int
    ask: float
    ask_size: int
    high: float
    low: float
    close: float
    volume: int


@dataclass
class ComboLeg:
    """Combo leg structure for multi-leg strategies"""
    conId: int
    action: str
    ratio: int
    exchange: str


@dataclass
class MultiLegOptionsData:
    """Multi-Leg Options Data (sec_type: BAG) - IBFLY Strategies"""
    # Base fields
    conId: str  # Comma-separated contract IDs
    symbol: str
    local_symbol: str
    symbol_key: str
    currency: str
    exchange: str
    sec_type: str
    timestamp: str
    unix_timestamp_ms: int
    
    # Calculated fields
    mid_price: float
    spread: float
    has_liquidity: bool
    
    # BAG-specific required fields
    strategy_type: str
    center_strike: float
    wing_width: int
    expiration: str
    combo_legs: List[Dict[str, Any]]
    leg_count: int
    leg_actions: str
    leg_conIds: str
    
    # Optional market data fields
    bid: Optional[float] = None
    bid_size: Optional[int] = None
    ask: Optional[float] = None
    ask_size: Optional[int] = None
    last: Optional[float] = None
    last_size: Optional[int] = None
    volume: Optional[int] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    
    # Expiration timing fields
    days_to_expiration: Optional[int] = None
    minutes_to_expiration: Optional[int] = None


@dataclass
class AccountValuesData:
    """Account Values Data - IBKR Account Information"""
    account: str
    timestamp: str
    values: Dict[str, Any]
    raw_count: Optional[int] = None
    processed_count: Optional[int] = None


# ============================================================================
# Parquet Reader Class
# ============================================================================

class ParquetReader:
    """Read and validate parquet files with typed schemas."""
    
    def __init__(self, parquet_dir: str = None):
        self.parquet_dir = Path(parquet_dir if parquet_dir is not None else OUTPUT_DIR)
        self.dataclass_map = {
            "IND": IndexMarketData,
            "FUT": FuturesMarketData,
            "OPT": OptionsMarketData,
            "STK": StockMarketData,
            "BAG": MultiLegOptionsData,
            "ACCOUNT_VALUES": AccountValuesData,
        }
    
    def load_parquet(self, filepath: Path) -> pl.DataFrame:
        """Load parquet file with polars."""
        try:
            df = pl.read_parquet(filepath)
            # Show relative path from parquet_dir
            try:
                relative_path = filepath.relative_to(self.parquet_dir)
                console.print(f"[OK] Loaded {relative_path}", style="green")
            except ValueError:
                # If not relative to parquet_dir, just show filename
                console.print(f"[OK] Loaded {filepath.name}", style="green")
            return df
        except Exception as e:
            console.print(f"[ERROR] Failed to load {filepath}: {e}", style="red")
            raise
    
    def extract_value_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Extract and rename 'value.*' columns to remove prefix."""
        # Get columns that start with "value."
        value_cols = [col for col in df.columns if col.startswith("value.")]
        
        if not value_cols:
            return df
        
        # Create rename mapping: "value.conId" -> "conId"
        rename_map = {col: col.replace("value.", "") for col in value_cols}
        
        # Select value columns and rename them
        extracted_df = df.select(value_cols).rename(rename_map)
        
        # Also keep non-value metadata columns if they exist
        metadata_cols = [col for col in df.columns if not col.startswith("value.")]
        if metadata_cols:
            metadata_df = df.select(metadata_cols)
            # Combine metadata and extracted value columns
            extracted_df = pl.concat([metadata_df, extracted_df], how="horizontal")
        
        return extracted_df
    
    def detect_security_type(self, df: pl.DataFrame, filename: str) -> Optional[str]:
        """Determine sec_type from data or topic name."""
        # First try to get from topic name
        topic_name = Path(filename).stem  # Remove .parquet extension
        if topic_name in TOPIC_TO_SEC_TYPE:
            return TOPIC_TO_SEC_TYPE[topic_name]
        
        # Try to get from sec_type column
        if "sec_type" in df.columns:
            sec_types = df["sec_type"].unique().to_list()
            if len(sec_types) == 1:
                return sec_types[0]
            elif len(sec_types) > 1:
                console.print(f"[WARN] Multiple sec_types found: {sec_types}", style="yellow")
                return sec_types[0]  # Return first one
        
        # Try to detect from account column (account values)
        if "account" in df.columns:
            return "ACCOUNT_VALUES"
        
        console.print(f"[WARN] Could not detect security type for {filename}", style="yellow")
        return None
    
    def to_dataclass(self, df: pl.DataFrame, sec_type: str) -> List[Any]:
        """Convert polars DataFrame to typed dataclass instances."""
        if sec_type not in self.dataclass_map:
            console.print(f"[WARN] No dataclass mapping for sec_type: {sec_type}", style="yellow")
            return []
        
        dataclass_type = self.dataclass_map[sec_type]
        instances = []
        
        # Convert to list of dicts
        records = df.to_dicts()
        
        for record in records:
            try:
                # Filter record to only include fields that exist in dataclass
                dataclass_fields = {f.name for f in fields(dataclass_type)}
                filtered_record = {k: v for k, v in record.items() if k in dataclass_fields}
                
                # Create instance
                instance = dataclass_type(**filtered_record)
                instances.append(instance)
            except Exception as e:
                console.print(f"[WARN] Failed to create dataclass instance: {e}", style="yellow")
                continue
        
        return instances
    
    def show_summary(self, df: pl.DataFrame, filename: str = ""):
        """Print row count, columns, data types."""
        console.print()
        title = f"Summary: {Path(filename).name}" if filename else "DataFrame Summary"
        console.print(Panel(title, style="bold cyan"))
        
        console.print(f"Rows: {len(df):,}", style="bold")
        console.print(f"Columns: {len(df.columns)}", style="bold")
        console.print()
        
        # Show schema
        console.print("Schema:", style="bold yellow")
        table = Table(box=box.SIMPLE)
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="magenta")
        table.add_column("Null Count", style="yellow")
        
        for col in df.columns:
            null_count = df[col].null_count()
            table.add_row(col, str(df[col].dtype), str(null_count))
        
        console.print(table)
    
    def show_preview(self, df: pl.DataFrame, n: int = 10):
        """Display first n rows."""
        console.print()
        console.print(Panel(f"Preview (first {n} rows)", style="bold cyan"))
        console.print(df.head(n))
    
    def show_statistics(self, df: pl.DataFrame):
        """Show numeric column statistics (min, max, mean, std)."""
        console.print()
        console.print(Panel("Numeric Statistics", style="bold cyan"))
        
        # Get numeric columns
        numeric_cols = [col for col in df.columns if df[col].dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]]
        
        if not numeric_cols:
            console.print("No numeric columns found.", style="yellow")
            return
        
        table = Table(box=box.SIMPLE)
        table.add_column("Column", style="cyan")
        table.add_column("Min", style="green")
        table.add_column("Max", style="green")
        table.add_column("Mean", style="yellow")
        table.add_column("Std Dev", style="magenta")
        
        for col in numeric_cols:
            try:
                col_data = df[col]
                min_val = col_data.min()
                max_val = col_data.max()
                mean_val = col_data.mean()
                std_val = col_data.std()
                
                table.add_row(
                    col,
                    f"{min_val:.2f}" if min_val is not None else "N/A",
                    f"{max_val:.2f}" if max_val is not None else "N/A",
                    f"{mean_val:.2f}" if mean_val is not None else "N/A",
                    f"{std_val:.2f}" if std_val is not None else "N/A"
                )
            except Exception:
                continue
        
        console.print(table)
    
    def show_schema_validation(self, df: pl.DataFrame, sec_type: str):
        """Validate against expected schema."""
        console.print()
        console.print(Panel(f"Schema Validation (sec_type: {sec_type})", style="bold cyan"))
        
        if sec_type not in self.dataclass_map:
            console.print(f"[WARN] No schema defined for sec_type: {sec_type}", style="yellow")
            return
        
        dataclass_type = self.dataclass_map[sec_type]
        expected_fields = {f.name: f.type for f in fields(dataclass_type)}
        actual_fields = set(df.columns)
        
        # Check for missing fields
        missing_fields = set(expected_fields.keys()) - actual_fields
        extra_fields = actual_fields - set(expected_fields.keys())
        
        table = Table(box=box.SIMPLE)
        table.add_column("Status", style="bold")
        table.add_column("Field", style="cyan")
        table.add_column("Expected Type", style="magenta")
        
        # Show missing fields
        for field in missing_fields:
            table.add_row("[X] Missing", field, str(expected_fields[field]))
        
        # Show present fields
        for field in expected_fields.keys():
            if field in actual_fields:
                table.add_row("[OK] Present", field, str(expected_fields[field]))
        
        # Show extra fields
        for field in extra_fields:
            table.add_row("[+] Extra", field, str(df[field].dtype))
        
        console.print(table)
        
        # Summary
        console.print()
        if missing_fields:
            console.print(f"[WARN] {len(missing_fields)} missing field(s)", style="yellow")
        if extra_fields:
            console.print(f"[INFO] {len(extra_fields)} extra field(s)", style="blue")
        if not missing_fields and not extra_fields:
            console.print("[OK] Schema matches perfectly!", style="green bold")
    
    def load_topics_batch(
        self,
        date_filter: str,
        topics: Optional[List[str]] = None,
        silent: bool = False
    ) -> Dict[str, pl.DataFrame]:
        """
        Load specific topics from a date directory without display output.
        
        Utility method for programmatic access (used by replayer).
        
        Args:
            date_filter: Date filter in format 'YYYY/MM/DD'
            topics: List of topic names to load (None = all topics)
            silent: If True, suppress console output
            
        Returns:
            Dictionary mapping topic names to DataFrames
        """
        if not self.parquet_dir.exists():
            raise FileNotFoundError(f"Directory not found: {self.parquet_dir}")
        
        search_path = self.parquet_dir / date_filter
        if not search_path.exists():
            raise FileNotFoundError(f"Date directory not found: {search_path}")
        
        parquet_files = list(search_path.glob("*.parquet"))
        parquet_files = [f for f in parquet_files if not f.name.startswith(".")]
        
        # Filter by topics if specified
        if topics:
            parquet_files = [f for f in parquet_files if f.stem in topics]
        
        if not parquet_files:
            raise FileNotFoundError(f"No matching parquet files found in {search_path}")
        
        all_dataframes = {}
        
        for filepath in sorted(parquet_files):
            try:
                df = pl.read_parquet(filepath)
                df = self.extract_value_columns(df)
                all_dataframes[filepath.stem] = df
                
                if not silent:
                    console.print(f"[OK] Loaded {filepath.stem}: {len(df):,} rows", style="green")
            except Exception as e:
                if not silent:
                    console.print(f"[ERROR] Failed to load {filepath.stem}: {e}", style="red")
                continue
        
        return all_dataframes
    
    def get_available_dates(self) -> List[str]:
        """
        Get list of available dates in parquet directory.
        
        Returns:
            List of date strings in YYYY/MM/DD format
        """
        if not self.parquet_dir.exists():
            return []
        
        dates = []
        
        # Look for YYYY/MM/DD directory structure
        for year_dir in self.parquet_dir.iterdir():
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir() or not month_dir.name.isdigit():
                    continue
                
                for day_dir in month_dir.iterdir():
                    if not day_dir.is_dir() or not day_dir.name.isdigit():
                        continue
                    
                    # Check if directory has parquet files
                    if list(day_dir.glob("*.parquet")):
                        date_str = f"{year_dir.name}/{month_dir.name}/{day_dir.name}"
                        dates.append(date_str)
        
        return sorted(dates)
    
    def get_topics_for_date(self, date_filter: str) -> List[str]:
        """
        Get list of available topics for a specific date.
        
        Args:
            date_filter: Date filter in format 'YYYY/MM/DD'
            
        Returns:
            List of topic names
        """
        search_path = self.parquet_dir / date_filter
        
        if not search_path.exists():
            return []
        
        parquet_files = list(search_path.glob("*.parquet"))
        parquet_files = [f for f in parquet_files if not f.name.startswith(".")]
        
        return sorted([f.stem for f in parquet_files])
    
    def generate_content_hash(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Generate content hash for each record, excluding Kafka metadata fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with added _content_hash column
        """
        # Get all columns except those starting with 'kafka_'
        content_columns = [col for col in df.columns if not col.startswith("kafka_")]
        
        if not content_columns:
            # If no content columns, use all columns
            content_columns = df.columns
        
        # Create hash from content columns
        df_with_hash = df.with_columns([
            pl.struct(content_columns).hash().alias("_content_hash")
        ])
        
        return df_with_hash
    
    def create_backup(self, filepath: Path) -> Path:
        """
        Create a backup of the original file with timestamp.
        
        Args:
            filepath: Path to the file to backup
            
        Returns:
            Path to the backup file
        """
        # Create backup directory with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        backup_dir = self.parquet_dir / ".backups" / timestamp
        
        # Calculate relative path from parquet_dir
        try:
            relative_path = filepath.relative_to(self.parquet_dir)
        except ValueError:
            # If not relative to parquet_dir, use filename only
            relative_path = Path(filepath.name)
        
        # Create backup filepath preserving directory structure
        backup_filepath = backup_dir / relative_path
        
        # Create backup directory structure
        backup_filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy file to backup location
        import shutil
        shutil.copy2(filepath, backup_filepath)
        
        return backup_filepath
    
    def deduplicate_file(self, filepath: Path) -> Dict[str, Any]:
        """
        Deduplicate a single parquet file based on content hash.
        
        Args:
            filepath: Path to the parquet file to deduplicate
            
        Returns:
            Dictionary with deduplication statistics
        """
        try:
            # Load the parquet file
            df = self.load_parquet(filepath)
            original_count = len(df)
            
            if original_count == 0:
                return {
                    "filepath": str(filepath),
                    "original_count": 0,
                    "deduplicated_count": 0,
                    "duplicates_removed": 0,
                    "status": "skipped",
                    "error": None
                }
            
            # Generate content hash
            df_with_hash = self.generate_content_hash(df)
            
            # Deduplicate by content hash, keep first occurrence
            deduplicated_df = df_with_hash.unique(subset=["_content_hash"], keep="first")
            
            # Remove hash column before writing
            deduplicated_df = deduplicated_df.drop("_content_hash")
            
            deduplicated_count = len(deduplicated_df)
            duplicates_removed = original_count - deduplicated_count
            
            # Write deduplicated data back to file
            deduplicated_df.write_parquet(filepath)
            
            return {
                "filepath": str(filepath),
                "original_count": original_count,
                "deduplicated_count": deduplicated_count,
                "duplicates_removed": duplicates_removed,
                "status": "success",
                "error": None
            }
            
        except Exception as e:
            return {
                "filepath": str(filepath),
                "original_count": 0,
                "deduplicated_count": 0,
                "duplicates_removed": 0,
                "status": "error",
                "error": str(e)
            }
    
    def deduplicate_all_files(self, date_filter: Optional[str] = None) -> Dict[str, Any]:
        """
        Deduplicate all parquet files, optionally filtered by date.
        
        Args:
            date_filter: Optional date filter in format 'YYYY/MM/DD' to only process specific date
            
        Returns:
            Dictionary with deduplication summary statistics
        """
        if not self.parquet_dir.exists():
            console.print(f"[ERROR] Directory not found: {self.parquet_dir}", style="red")
            return {"status": "error", "error": f"Directory not found: {self.parquet_dir}"}
        
        # Search for parquet files (handles date-partitioned structure)
        if date_filter:
            search_path = self.parquet_dir / date_filter
            if not search_path.exists():
                console.print(f"[ERROR] Date directory not found: {search_path}", style="red")
                return {"status": "error", "error": f"Date directory not found: {search_path}"}
            parquet_files = list(search_path.glob("*.parquet"))
        else:
            parquet_files = list(self.parquet_dir.rglob("*.parquet"))
        
        # Exclude backup files and hidden files
        parquet_files = [f for f in parquet_files if not f.name.startswith(".") and ".backups" not in str(f)]
        
        if not parquet_files:
            search_loc = f"{self.parquet_dir}/{date_filter}" if date_filter else str(self.parquet_dir)
            console.print(f"[ERROR] No parquet files found in {search_loc}", style="red")
            return {"status": "error", "error": f"No parquet files found in {search_loc}"}
        
        console.print(f"\n[DEDUPLICATION] Found {len(parquet_files)} parquet file(s) to process\n", style="bold green")
        
        # Process each file
        results = []
        total_original = 0
        total_deduplicated = 0
        total_duplicates_removed = 0
        successful_files = 0
        error_files = 0
        
        for filepath in sorted(parquet_files):
            try:
                # Show relative path from parquet_dir
                try:
                    relative_path = filepath.relative_to(self.parquet_dir)
                    console.print(f"Processing: {relative_path}", style="bold blue")
                except ValueError:
                    console.print(f"Processing: {filepath.name}", style="bold blue")
                
                # Create backup first
                backup_path = self.create_backup(filepath)
                console.print(f"[BACKUP] Created backup: {backup_path.relative_to(self.parquet_dir)}", style="dim")
                
                # Deduplicate the file
                result = self.deduplicate_file(filepath)
                results.append(result)
                
                # Update totals
                if result["status"] == "success":
                    total_original += result["original_count"]
                    total_deduplicated += result["deduplicated_count"]
                    total_duplicates_removed += result["duplicates_removed"]
                    successful_files += 1
                    
                    console.print(f"[SUCCESS] {result['original_count']:,} → {result['deduplicated_count']:,} records ({result['duplicates_removed']:,} duplicates removed)", style="green")
                elif result["status"] == "error":
                    error_files += 1
                    console.print(f"[ERROR] {result['error']}", style="red")
                else:  # skipped
                    console.print(f"[SKIPPED] Empty file", style="yellow")
                
                console.print()
                
            except Exception as e:
                error_files += 1
                console.print(f"[ERROR] Failed to process {filepath.name}: {e}", style="red")
                console.print()
                continue
        
        # Summary report
        console.print("=" * 80, style="green")
        console.print(f"[SUMMARY] Deduplication Complete", style="bold green")
        console.print("=" * 80, style="green")
        console.print(f"Files processed: {len(parquet_files)}", style="bold")
        console.print(f"Successful: {successful_files}", style="green")
        console.print(f"Errors: {error_files}", style="red" if error_files > 0 else "green")
        console.print(f"Skipped: {len(parquet_files) - successful_files - error_files}", style="yellow")
        console.print()
        console.print(f"Total original records: {total_original:,}", style="bold")
        console.print(f"Total deduplicated records: {total_deduplicated:,}", style="bold")
        console.print(f"Total duplicates removed: {total_duplicates_removed:,}", style="bold")
        
        if total_original > 0:
            dedup_percentage = (total_duplicates_removed / total_original) * 100
            console.print(f"Deduplication rate: {dedup_percentage:.2f}%", style="cyan")
        
        return {
            "status": "success",
            "files_processed": len(parquet_files),
            "successful_files": successful_files,
            "error_files": error_files,
            "total_original_records": total_original,
            "total_deduplicated_records": total_deduplicated,
            "total_duplicates_removed": total_duplicates_removed,
            "results": results
        }
    
    def load_and_display_all(self, date_filter: Optional[str] = None):
        """Scan parquet directory and display all files (supports date-partitioned structure).
        
        Args:
            date_filter: Optional date filter in format 'YYYY/MM/DD' to only load specific date
        """
        if not self.parquet_dir.exists():
            console.print(f"[ERROR] Directory not found: {self.parquet_dir}", style="red")
            return {}
        
        # Search recursively for all parquet files (handles date-partitioned structure)
        if date_filter:
            search_path = self.parquet_dir / date_filter
            if not search_path.exists():
                console.print(f"[ERROR] Date directory not found: {search_path}", style="red")
                return {}
            parquet_files = list(search_path.glob("*.parquet"))
        else:
            parquet_files = list(self.parquet_dir.rglob("*.parquet"))
        
        # Exclude the .offsets.json metadata file
        parquet_files = [f for f in parquet_files if not f.name.startswith(".")]
        
        if not parquet_files:
            search_loc = f"{self.parquet_dir}/{date_filter}" if date_filter else str(self.parquet_dir)
            console.print(f"[ERROR] No parquet files found in {search_loc}", style="red")
            return {}
        
        console.print(f"\n[FILES] Found {len(parquet_files)} parquet file(s) in {self.parquet_dir}\n", style="bold green")
        
        all_dataframes = {}
        
        for filepath in sorted(parquet_files):
            try:
                console.print("=" * 80, style="blue")
                # Show relative path from parquet_dir to show date structure
                relative_path = filepath.relative_to(self.parquet_dir)
                console.print(f"Processing: {relative_path}", style="bold blue")
                console.print("=" * 80, style="blue")
                
                # Load parquet
                df = self.load_parquet(filepath)
                
                # Extract value columns
                df = self.extract_value_columns(df)
                
                # Detect security type
                sec_type = self.detect_security_type(df, filepath.name)
                
                if sec_type:
                    console.print(f"Detected security type: {sec_type}", style="cyan")
                
                # Display information
                self.show_summary(df, filepath.name)
                
                if sec_type:
                    self.show_schema_validation(df, sec_type)
                
                self.show_preview(df, n=5)
                self.show_statistics(df)
                
                # Store in dict
                all_dataframes[filepath.stem] = {
                    "dataframe": df,
                    "sec_type": sec_type,
                    "filepath": str(filepath)
                }
                
                console.print()
                
            except Exception as e:
                console.print(f"[ERROR] Error processing {filepath.name}: {e}", style="red")
                continue
        
        # Final summary
        console.print()
        console.print("=" * 80, style="green")
        console.print(f"[OK] Processed {len(all_dataframes)} file(s) successfully", style="bold green")
        console.print("=" * 80, style="green")
        
        return all_dataframes


# ============================================================================
# Main Script
# ============================================================================

def main():
    """Main entry point for the script."""
    console.print()
    console.print(Panel.fit(
        "Parquet to Polars Reader\n" +
        "Read and validate Redpanda topic data from parquet files\n" +
        "Supports date-partitioned structure: ./redpanda_parquet/YYYY/MM/DD/\n" +
        "Includes deduplication functionality",
        style="bold magenta"
    ))
    console.print()
    
    # Parse command-line arguments
    date_filter = None
    deduplicate_mode = False
    base_dir = None
    
    # Check for base directory flag
    if "--base-dir" in sys.argv:
        idx = sys.argv.index("--base-dir")
        if idx + 1 < len(sys.argv):
            base_dir = sys.argv[idx + 1]
            # Remove both flag and value from args
            sys.argv = [arg for i, arg in enumerate(sys.argv) if i not in [idx, idx + 1]]
    elif "-b" in sys.argv:
        idx = sys.argv.index("-b")
        if idx + 1 < len(sys.argv):
            base_dir = sys.argv[idx + 1]
            # Remove both flag and value from args
            sys.argv = [arg for i, arg in enumerate(sys.argv) if i not in [idx, idx + 1]]
    
    # Check for deduplication flag
    if "--deduplicate" in sys.argv or "-d" in sys.argv:
        deduplicate_mode = True
        # Remove the deduplication flag from args
        sys.argv = [arg for arg in sys.argv if arg not in ["--deduplicate", "-d"]]
    
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg in ["-h", "--help"]:
            console.print("Usage: python parquet_to_polars.py [options] [date]", style="bold")
            console.print("\nOptions:", style="cyan")
            console.print("  --base-dir, -b PATH    Specify base directory for parquet files (default: ./data/redpanda_parquet or OUTPUT_DIR env var)")
            console.print("  --deduplicate, -d      Deduplicate parquet files based on content hash")
            console.print("\nExamples:", style="cyan")
            console.print("  python parquet_to_polars.py                    # Read all dates")
            console.print("  python parquet_to_polars.py 2025/10/14         # Read specific date")
            console.print("  python parquet_to_polars.py today              # Read today's date")
            console.print("  python parquet_to_polars.py -b C:\DATA\my_parquet_files         # Custom base directory")
            console.print("  python parquet_to_polars.py --base-dir /mnt/data/parquet today  # Custom directory with date filter")
            console.print("  python parquet_to_polars.py --deduplicate       # Deduplicate all files")
            console.print("  python parquet_to_polars.py -d 2025/10/14      # Deduplicate specific date")
            return
        elif arg == "today":
            date_filter = datetime.now().strftime("%Y/%m/%d")
            console.print(f"[DATE] Filtering for today: {date_filter}\n", style="cyan")
        else:
            date_filter = arg
            console.print(f"[DATE] Filtering for date: {date_filter}\n", style="cyan")
    
    # Create reader with base directory
    reader = ParquetReader(parquet_dir=base_dir)
    
    if deduplicate_mode:
        # Deduplication mode
        console.print(Panel.fit(
            "DEDUPLICATION MODE\n" +
            "Removing duplicate records based on content hash\n" +
            "Original files will be backed up before modification",
            style="bold yellow"
        ))
        console.print()
        
        # Run deduplication
        result = reader.deduplicate_all_files(date_filter=date_filter)
        
        if result["status"] == "success":
            console.print("\n[bold green]✓ Deduplication completed successfully![/bold green]")
        else:
            console.print(f"\n[bold red]✗ Deduplication failed: {result.get('error', 'Unknown error')}[/bold red]")
            return None
        
        return result
    else:
        # Normal read mode
        # Load and display all parquet files
        dataframes = reader.load_and_display_all(date_filter=date_filter)
        
        # Optional: Return dataframes for programmatic access
        return dataframes


if __name__ == "__main__":
    main()

