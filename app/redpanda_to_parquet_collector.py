"""
Redpanda to Parquet Collector

Streams data from Redpanda topics and saves to date-partitioned Parquet files.

Features:
- MessagePack and JSON decoding support (tries MessagePack first)
- Automatic nested dictionary flattening (extracts 'data' field to top level)
- Incremental updates (only processes new messages since last run)
- Parallel topic processing with progress bars
- Date-partitioned output: ./redpanda_parquet/YYYY/MM/DD/topic.parquet

Performance Optimizations:
- BATCH_SIZE: 100K messages per write (reduces I/O operations)
- Aggressive consumer settings: 10MB per partition, 1GB queue
- SKIP_EXISTING_CHECK: Set to True for fresh exports (skips offset checks)
- Fast mode: No read-modify-write cycle when SKIP_EXISTING_CHECK=True

Message Format:
  Expected MessagePack structure:
    {
      'event_type': 'market_data',
      'source': 'ibkr',
      'data': { ... market data fields ... },
      'metadata': { ... }
    }
  
  Flattening behavior:
    - 'data' dict contents are extracted to top-level columns
    - Other nested dicts are flattened with underscore prefixes
    - Lists are converted to JSON strings for storage

Usage:
  Fresh export:  SKIP_EXISTING_CHECK = True   (faster, overwrites files)
  Incremental:   SKIP_EXISTING_CHECK = False  (slower, preserves existing data)
"""

import json
import os
import tempfile
import shutil
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl
import msgpack
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, MofNCompleteColumn
from rich.console import Console

# Configuration - read from environment variables with sensible defaults
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "192.168.1.110:19092")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./data/redpanda_parquet")
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES")) if os.getenv("MAX_MESSAGES") else None  # or set to an int limit per topic if needed
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100000"))  # Write to parquet every 100K messages (increased for performance)
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))  # Parallel topic processing
SKIP_EXISTING_CHECK = os.getenv("SKIP_EXISTING_CHECK", "true").lower() in ("true", "1", "yes")  # Set to True for fresh exports (faster, skips reading existing files)


def flatten_dict(d, parent_key='', sep='_'):
    """
    Flatten nested dictionary structure.
    
    Example:
        {'data': {'bid': 100, 'ask': 101}} -> {'data_bid': 100, 'data_ask': 101}
    
    For specific keys like 'data', we can extract them to top level without prefix.
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        # Special handling: extract 'data' dict contents to top level
        if k == 'data' and isinstance(v, dict):
            items.extend(flatten_dict(v, '', sep).items())
        elif isinstance(v, dict):
            # For other nested dicts, flatten with prefix
            items.extend(flatten_dict(v, new_key, sep).items())
        elif isinstance(v, list):
            # Convert lists to JSON strings for storage
            items.append((new_key, json.dumps(v) if v else None))
        else:
            items.append((new_key, v))
    
    return dict(items)


def get_all_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=5)
    topics = [t for t in metadata.topics.keys() if not t.startswith("__")]  # skip internals
    return topics


def get_partitions(consumer, topic):
    """Return list of partition numbers for given topic."""
    md = consumer.list_topics(topic, timeout=5)
    topic_md = md.topics[topic]
    return list(topic_md.partitions.keys())


def get_topic_high_watermarks(topic):
    """Get high watermark offsets for all partitions to calculate total messages."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"watermark-reader-{topic}",
        "auto.offset.reset": "earliest",
    })
    
    try:
        partitions = get_partitions(consumer, topic)
        watermarks = {}
        
        for partition in partitions:
            # Get low and high watermark for this partition
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=5)
            watermarks[partition] = high
        
        return watermarks
    finally:
        consumer.close()


def consume_topic_streaming(topic, start_offsets=None):
    """Stream messages from specified offsets (generator)."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"reader-{topic}",
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
        # Aggressive performance tuning for fast exports
        "fetch.min.bytes": 1024 * 1024,  # 1MB min fetch (increased from 100KB)
        "fetch.wait.max.ms": 100,  # Shorter wait (faster response)
        "max.partition.fetch.bytes": 1024 * 1024 * 10,  # 10MB per partition (increased from 1MB)
        "queued.min.messages": 100000,  # Queue up to 100K messages
        "queued.max.messages.kbytes": 1024 * 1024,  # 1GB queue size
    })

    # Set starting offsets per partition (or 0 if not specified)
    partitions = []
    for p in get_partitions(consumer, topic):
        # Start from offset+1 (next message after what we have)
        start_offset = start_offsets.get(p, -1) + 1 if start_offsets else 0
        partitions.append(TopicPartition(topic, p, start_offset))
    
    consumer.assign(partitions)

    # Track EOF per partition
    partition_eof = {p.partition: False for p in partitions}
    msg_count = 0
    consecutive_none = 0
    
    try:
        while True:
            # Dynamic timeout: short when flowing, longer when idle
            timeout = 0.1 if consecutive_none < 5 else 1.0
            msg = consumer.poll(timeout=timeout)
            
            if msg is None:
                consecutive_none += 1
                # If all partitions reached EOF, we're done
                if all(partition_eof.values()):
                    break
                continue
            
            consecutive_none = 0  # Reset on successful message
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Mark this partition as EOF
                    partition_eof[msg.partition()] = True
                    # Continue to check other partitions
                    if all(partition_eof.values()):
                        break
                    continue
                else:
                    raise KafkaException(msg.error())

            msg_value = msg.value()
            if msg_value is None:
                continue

            # Try to decode message (MessagePack first, then JSON)
            decoded = None
            try:
                # Try MessagePack first (primary format for this system)
                decoded = msgpack.unpackb(msg_value, raw=False)
            except Exception:
                try:
                    # Fallback to JSON
                    decoded = json.loads(msg_value.decode("utf-8"))
                except Exception:
                    # Last resort: store as raw string
                    decoded = {"raw_value": msg_value.decode("utf-8", errors="replace")}

            # Start with metadata fields (use kafka_ prefix to avoid conflicts)
            record = {
                "kafka_topic": msg.topic(),
                "kafka_partition": msg.partition(),
                "kafka_offset": msg.offset(),
                "kafka_timestamp": msg.timestamp()[1],
                "kafka_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
            }
            
            # Flatten and merge decoded JSON/MessagePack fields into top-level record
            if isinstance(decoded, dict):
                flattened = flatten_dict(decoded)
                record.update(flattened)
            else:
                # Fallback for non-dict values
                record["raw_value"] = str(decoded)

            yield record
            msg_count += 1

            if MAX_MESSAGES and msg_count >= MAX_MESSAGES:
                break

    finally:
        consumer.close()


def get_existing_max_offsets(topic):
    """Get the maximum offset for each partition from existing parquet files."""
    max_offsets = {}  # {partition: max_offset}
    
    # If skipping existing checks, return empty (start from beginning)
    if SKIP_EXISTING_CHECK:
        return max_offsets
    
    # Check all date directories for this topic
    if not os.path.exists(OUTPUT_DIR):
        return max_offsets
    
    # Walk through all date directories
    for root, dirs, files in os.walk(OUTPUT_DIR):
        parquet_file = os.path.join(root, f"{topic}.parquet")
        if os.path.exists(parquet_file):
            try:
                # Read only the columns we need
                df = pl.read_parquet(parquet_file, columns=["kafka_partition", "kafka_offset"])
                
                # Get max offset per partition
                partition_max = df.group_by("kafka_partition").agg(pl.col("kafka_offset").max())
                
                for row in partition_max.iter_rows(named=True):
                    partition = row["kafka_partition"]
                    max_offset = row["kafka_offset"]
                    
                    # Keep the highest offset across all date files
                    if partition not in max_offsets or max_offset > max_offsets[partition]:
                        max_offsets[partition] = max_offset
            except Exception:
                # Silently skip files that can't be read
                pass
    
    return max_offsets


def write_parquet_batch_fast(topic, records, console=None):
    """Write records with FAST append - no reading existing files."""
    if not records:
        return 0
    
    # Convert to Polars DataFrame
    df = pl.DataFrame(records)
    
    # Skip records with null kafka timestamps
    df = df.filter(pl.col("kafka_timestamp").is_not_null())
    
    if len(df) == 0:
        return 0
    
    records_written = len(df)
    
    # Sort by (kafka_topic, kafka_partition, kafka_offset) for organized storage
    df = df.sort(["kafka_topic", "kafka_partition", "kafka_offset"])
    
    # Add date column (convert Kafka ms timestamp to date)
    df = df.with_columns([
        (pl.from_epoch("kafka_timestamp", time_unit="ms")
         .dt.strftime("%Y/%m/%d")
         .alias("date_path"))
    ])
    
    # Get unique dates
    unique_dates = df["date_path"].unique().to_list()
    
    # Write one file per date with FAST append
    for date_path in unique_dates:
        date_df = df.filter(pl.col("date_path") == date_path)
        
        # Create directory structure
        output_path = os.path.join(OUTPUT_DIR, date_path)
        os.makedirs(output_path, exist_ok=True)
        
        filepath = os.path.join(output_path, f"{topic}.parquet")
        
        if os.path.exists(filepath) and not SKIP_EXISTING_CHECK:
            # INCREMENTAL MODE: Read existing data and combine (slower but preserves data)
            try:
                # Read existing data
                existing_df = pl.read_parquet(filepath)
                combined_df = pl.concat([existing_df, date_df.drop("date_path")])
                
                # Write to temp file first (avoids Windows file locking)
                temp_fd, temp_path = tempfile.mkstemp(suffix=".parquet", dir=output_path)
                os.close(temp_fd)  # Close the file descriptor
                
                try:
                    combined_df.write_parquet(temp_path, compression="snappy")
                    # Replace original with temp file (atomic on Windows)
                    shutil.move(temp_path, filepath)
                finally:
                    # Clean up temp file if it still exists
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except:
                            pass
            except Exception as e:
                if console:
                    console.log(f"[yellow][WARN][/yellow] {topic}: Append failed, creating new file: {e}")
                date_df.drop("date_path").write_parquet(filepath, compression="snappy")
        else:
            # FAST MODE or NEW FILE: Just write/overwrite without reading (much faster)
            date_df.drop("date_path").write_parquet(filepath, compression="snappy")
    
    return records_written


def process_topic(topic, progress=None, task_id=None, console=None):
    """Process a single topic - FAST incremental mode with progress tracking."""
    records_buffer = []
    total_written = 0
    start_time = time.time()
    
    try:
        # Get existing max offsets and high watermarks
        existing_max_offsets = get_existing_max_offsets(topic)
        high_watermarks = get_topic_high_watermarks(topic)
        
        # Calculate remaining messages to process
        remaining = 0
        for partition, high in high_watermarks.items():
            current = existing_max_offsets.get(partition, -1)
            remaining += max(0, high - current - 1)
        
        # Update task total if we have progress tracking
        if progress and task_id is not None:
            progress.update(task_id, total=remaining if remaining > 0 else 1)
            if remaining == 0:
                progress.update(task_id, description=f"[green]{topic}[/green] - up to date")
        
        # Consume ONLY from max offset onwards (skip already-written messages)
        for record in consume_topic_streaming(topic, start_offsets=existing_max_offsets):
            records_buffer.append(record)
            
            if len(records_buffer) >= BATCH_SIZE:
                written = write_parquet_batch_fast(topic, records_buffer, console)
                total_written += written
                records_buffer = []
                
                # Update progress
                if progress and task_id is not None:
                    elapsed = time.time() - start_time
                    throughput = total_written / elapsed if elapsed > 0 else 0
                    progress.update(
                        task_id, 
                        advance=written,
                        description=f"[green]{topic}[/green] - {throughput:.0f} msg/s"
                    )
        
        # Final flush
        if records_buffer:
            written = write_parquet_batch_fast(topic, records_buffer, console)
            total_written += written
            
            if progress and task_id is not None:
                progress.update(task_id, advance=written)
        
        # Mark as complete
        if progress and task_id is not None:
            elapsed = time.time() - start_time
            if total_written > 0:
                throughput = total_written / elapsed if elapsed > 0 else 0
                progress.update(
                    task_id,
                    description=f"[green]{topic}[/green] - {total_written:,} msgs ({throughput:.0f} msg/s)"
                )
            progress.update(task_id, completed=True)
        
        return topic, total_written, None
        
    except Exception as e:
        if console:
            console.log(f"[red][ERROR][/red] {topic}: {e}")
        if progress and task_id is not None:
            progress.update(task_id, description=f"[red]{topic}[/red] - ERROR")
        return topic, total_written, str(e)


def main():
    console = Console()
    topics = get_all_topics()
    console.print(f"\n[bold cyan]Found {len(topics)} topics:[/bold cyan] {', '.join(topics)}\n")
    
    # Create Rich Progress display
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        
        # Create overall task
        overall_task = progress.add_task("[cyan]Overall Progress", total=len(topics))
        
        # Create per-topic tasks
        topic_tasks = {}
        for topic in topics:
            task_id = progress.add_task(f"[yellow]{topic}[/yellow] - initializing...", total=100)
            topic_tasks[topic] = task_id
        
        # Process topics in parallel with progress tracking
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    process_topic, 
                    topic, 
                    progress, 
                    topic_tasks[topic],
                    console
                ): topic 
                for topic in topics
            }
            
            for future in as_completed(futures):
                topic = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    progress.update(overall_task, advance=1)
                except Exception as e:
                    console.log(f"[red][EXCEPTION][/red] {topic}: {e}")
                    results.append((topic, 0, str(e)))
                    progress.update(overall_task, advance=1)
    
    # Summary
    console.print("\n" + "="*60)
    console.print("[bold cyan]SUMMARY: Processing Complete[/bold cyan]")
    console.print("="*60)
    successful = [r for r in results if r[2] is None]
    failed = [r for r in results if r[2] is not None]
    
    if successful:
        console.print(f"\n[bold green]SUCCESS[/bold green] - Processed {len(successful)} topics:")
        for topic, count, _ in successful:
            console.print(f"  • {topic}: [green]{count:,}[/green] new messages written")
    
    if failed:
        console.print(f"\n[bold red]FAILED[/bold red] - {len(failed)} topics:")
        for topic, count, error in failed:
            console.print(f"  • {topic}: [red]{error}[/red]")
    
    total_messages = sum(r[1] for r in results)
    console.print(f"\n[bold]Total new messages written:[/bold] [green]{total_messages:,}[/green]")
    console.print("[bold cyan]✓ All topics processed and saved to Parquet.[/bold cyan]\n")


if __name__ == "__main__":
    main()
