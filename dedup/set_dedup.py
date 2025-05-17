import os
import tempfile
import heapq
import logging
import time
import psutil
import argparse
from typing import List, Set
from pathlib import Path
from dataclasses import dataclass
import sys
import mmap

os.system('ulimit -n 4096')  # Increase file descriptor limit

@dataclass
class Config:
    chunk_size: int
    input_file: str
    output_file: str
    encoding: str = 'ascii'
    max_memory_mb: int = 4096  # Reduced to 4GB to stay safely under 8GB
    debug: bool = False  

    @classmethod
    def from_args(cls) -> 'Config':
        """Create Config from command line arguments."""
        parser = argparse.ArgumentParser(description='Large file deduplication tool')
        parser.add_argument('input_file', help='Input file path')
        parser.add_argument('output_file', help='Output file path')
        parser.add_argument('--chunk-size', type=int, default=2_000_000,  # Increased from 1M to 2M
                            help='Number of lines per chunk')
        parser.add_argument('--encoding', type=str, default='ascii',
                            help='File encoding')
        parser.add_argument('--max-memory', type=int, default=4096,
                            help='Maximum memory usage in MB')
        parser.add_argument('--debug', action='store_true', help='Enable detailed resource logging')
        args = parser.parse_args()
        
        if not args.input_file or not args.output_file:
            parser.error('input_file and output_file are required.')
        
        if os.path.exists(args.input_file) and os.path.getsize(args.input_file) == 0:
            parser.error(f"Input file '{args.input_file}' is empty.")
        
        return cls(
            chunk_size=args.chunk_size,
            input_file=args.input_file,
            output_file=args.output_file,
            encoding=args.encoding,
            max_memory_mb=args.max_memory,
            debug=args.debug  
        )


TEMP_DIR: str = tempfile.mkdtemp()


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_memory_usage() -> None:
    """Log current process memory usage in MB and CPU cores."""
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / (1024 * 1024)
    logging.info(f"Memory usage: {memory_usage:.2f} MB")  # Simplified logging

def log_disk_usage(file_path: str) -> None:
    """Log file size in MB."""
    if os.path.exists(file_path):
        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        logging.info(f"File size ({file_path}): {size_mb:.2f} MB")

def log_resource_usage() -> None:
    """Log current process memory and CPU usage."""
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / (1024 * 1024)
    # No interval means "since last call" - much faster but less accurate
    cpu_percent = process.cpu_percent()
    logging.info(f"Memory usage: {memory_usage:.2f} MB, CPU usage: {cpu_percent:.1f}%")

def write_sorted_chunk(lines: List[str], chunk_idx: int) -> str:
    """
    Write a sorted, deduplicated chunk of lines to a temporary file.
    
    Args:
        lines: List of strings to process
        chunk_idx: Index of the current chunk
    
    Returns:
        Path to the written temporary file
    """
    unique: Set[str] = set(lines)  
    sorted_lines: List[str] = sorted(unique)
    temp_path = os.path.join(TEMP_DIR, f"chunk_{chunk_idx}.txt")
    
    try:
        with open(temp_path, 'w', encoding='ascii') as f:
            for line in sorted_lines:
                f.write(line)
    except IOError as e:
        logging.error(f"Failed to write chunk {chunk_idx}: {e}")
        raise
        
    logging.info(f"Chunk {chunk_idx} written to {temp_path}")
    log_resource_usage()  
    return temp_path


def merge_and_deduplicate_chunks(chunk_files: List[str], output_file: str) -> None:
    """Merge chunks with optimized disk space usage."""
    logging.info(f"Starting to merge and deduplicate chunks into: {output_file}")
    
    # Check for disk space before merging
    total_chunk_size = sum(os.path.getsize(f) for f in chunk_files) / (1024 * 1024)
    check_disk_space(total_chunk_size * 1.1, os.path.dirname(output_file) or '.')
    
    start_time = time.time()
    open_files = [open(path, 'r', encoding='ascii') for path in chunk_files]
    iterators = [map(str.rstrip, f) for f in open_files]
    merged = heapq.merge(*iterators)

    with open(output_file, 'w', encoding='ascii') as out:
        prev_line = None
        for line in merged:
            if line != prev_line:
                out.write(line + '\n')
                prev_line = line

    for f in open_files:
        f.close()

    elapsed_time = time.time() - start_time
    logging.info(f"Completed merging and deduplication in {elapsed_time:.2f} seconds")
    log_resource_usage() 

def clean_up(files):
    logging.info("Starting cleanup of temporary files")
    for path in files:
        os.remove(path)
        logging.info(f"Removed file: {path}")
    os.rmdir(TEMP_DIR)
    logging.info(f"Removed temporary directory: {TEMP_DIR}")

def process_file_with_set(input_file: str, output_file: str, chunk_size: int = 2_000_000, 
                         max_memory_mb: int = 4096, config=None) -> None:
    """Process file using a set to track seen lines with strict memory control."""
    # Default config if none provided
    if config is None:
        config = Config(chunk_size=chunk_size, input_file=input_file, 
                       output_file=output_file, max_memory_mb=max_memory_mb)
    
    # Check for disk space before starting (need input size + 20% for safety)
    input_size_mb = os.path.getsize(input_file) / (1024 * 1024)
    check_disk_space(input_size_mb * 1.2, os.path.dirname(output_file) or '.')
    
    logging.info(f"Starting deduplication of {input_file}")
    log_disk_usage(input_file)
    start_time = time.time()
    
    processed_lines = 0
    chunk_number = 0
    chunk_files = []
    current_chunk = set()
    lines_in_chunk = 0
    
    def write_chunk():
        nonlocal chunk_number
        if current_chunk:
            temp_file = os.path.join(TEMP_DIR, f'chunk_{chunk_number}.txt')
            with open(temp_file, 'w', encoding='ascii') as f:
                for line in sorted(current_chunk):
                    f.write(line + '\n')
            chunk_files.append(temp_file)
            chunk_number += 1
            current_chunk.clear()
            import gc
            gc.collect()
            # Only log every N chunks
            if chunk_number % 5 == 0:
                log_resource_usage()
    
    with open(input_file, 'rb') as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
        for line in iter(mm.readline, b''):
            try:
                line = line.decode('ascii').rstrip()
                processed_lines += 1
                current_chunk.add(line)
                lines_in_chunk += 1
                
                # Check memory usage less frequently
                if config.debug or lines_in_chunk >= chunk_size or (lines_in_chunk % 100000 == 0 and lines_in_chunk > 0):
                    memory_usage = psutil.Process().memory_info().rss / (1024 * 1024)
                    if memory_usage > max_memory_mb * 0.75:  # More conservative threshold
                        write_chunk()
                        lines_in_chunk = 0
                        log_resource_usage()  # Changed from log_memory_usage
                
            except MemoryError as e:
                logging.error(f"Memory limit exceeded: {e}")
                raise
    
    
    write_chunk()
    
    
    logging.info(f"Merging {len(chunk_files)} chunks...")
    merge_and_deduplicate_chunks(chunk_files, output_file)
    
    
    clean_up(chunk_files)
    
    elapsed_time = time.time() - start_time
    input_size_mb = os.path.getsize(input_file) / (1024 * 1024)
    logging.info(f"Completed deduplication in {elapsed_time:.2f} seconds")
    logging.info(f"Processing speed: {input_size_mb/elapsed_time:.2f} MB/second")
    logging.info(f"Total lines processed: {processed_lines}")
    log_disk_usage(output_file)

def verify_deduplication(input_file: str, output_file: str, sample_size: int = 100000) -> None:
    """Verify deduplication by checking if output has no duplicates and preserves unique lines."""
    logging.info(f"Verifying deduplication with {sample_size} line sample...")
    
    # Get sample of input file unique lines
    input_sample = set()
    with open(input_file, 'r', encoding='ascii') as f:
        for _ in range(sample_size):
            line = f.readline().rstrip()
            if not line:
                break
            input_sample.add(line)
    
    # Check output file for duplicates
    output_lines = set()
    duplicates_found = False
    with open(output_file, 'r', encoding='ascii') as f:
        for i, line in enumerate(f):
            line = line.rstrip()
            if line in output_lines:
                logging.error(f"Duplicate found in output: '{line}'")
                duplicates_found = True
                break
            output_lines.add(line)
            if i >= sample_size:
                break
    
    if not duplicates_found:
        logging.info("✅ No duplicates found in output file (sample check)")
    
    # Check if unique lines were preserved
    missing_lines = [line for line in input_sample if line not in output_lines]
    if missing_lines:
        logging.error(f"❌ {len(missing_lines)} unique lines from input missing in output")
    else:
        logging.info("✅ All unique input lines preserved in output")

def check_disk_space(required_mb, path='.'):
    """Check if there's enough disk space available."""
    disk = psutil.disk_usage(path)
    available_mb = disk.free / (1024 * 1024)
    logging.info(f"Available disk space: {available_mb:.2f} MB")
    if available_mb < required_mb:
        raise RuntimeError(f"Not enough disk space. Required: {required_mb} MB, Available: {available_mb:.2f} MB")

def main():
    config = Config.from_args()
    
    logging.info("Process started")
    log_resource_usage()
    process_file_with_set(
        config.input_file, 
        config.output_file, 
        config.chunk_size,
        config.max_memory_mb,
        config  
    )
    
    verify_deduplication(config.input_file, config.output_file)
    
    logging.info("Process completed successfully")

if __name__ == "__main__":
    main()
