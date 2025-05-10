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

@dataclass
class Config:
    chunk_size: int
    input_file: str
    output_file: str
    encoding: str = 'ascii'
    test_file: str = ''  # New field for test command

    @classmethod
    def from_args(cls) -> 'Config':
        """Create Config from command line arguments."""
        parser = argparse.ArgumentParser(description='Large file deduplication tool')
        parser.add_argument('input_file', help='Input file path', nargs='?')
        parser.add_argument('output_file', help='Output file path', nargs='?')
        parser.add_argument('--chunk-size', type=int, default=100_000,
                           help='Number of lines per chunk')
        parser.add_argument('--encoding', type=str, default='ascii',
                           help='File encoding')
        parser.add_argument('--test', type=str, help='Test if file is sorted')
        args = parser.parse_args()
        
        if args.test:
            return cls(chunk_size=0, input_file='', output_file='', test_file=args.test)
        elif not args.input_file or not args.output_file:
            parser.error('input_file and output_file are required unless --test is used')
        
        if args.input_file and os.path.exists(args.input_file):
            if os.path.getsize(args.input_file) == 0:
                parser.error(f"Input file '{args.input_file}' is empty.")
        elif args.input_file:
            parser.error(f"Input file '{args.input_file}' does not exist or is malformed.")
            
        return cls(
            chunk_size=args.chunk_size,
            input_file=args.input_file,
            output_file=args.output_file,
            encoding=args.encoding
        )

# Constants
CHUNK_SIZE_LINES: int = 100_000  # Number of lines to process in memory at once
TEMP_DIR: str = tempfile.mkdtemp()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_memory_usage() -> None:
    """Log current process memory usage in MB and CPU cores."""
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    cpu_count = psutil.cpu_count()
    
   
    process.cpu_percent()  
    time.sleep(0.1) 
    cpu_percent = process.cpu_percent() 
    
    logging.info(
        f"Memory usage: {memory_usage:.2f} MB, "
        f"CPU cores: {cpu_count}, "
        f"CPU usage: {cpu_percent:.1f}%"
    )

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
    log_memory_usage()
    return temp_path

def chunk_and_sort_input(input_file: str) -> List[str]:
    """
    Split input file into sorted, deduplicated chunks.
    
    Args:
        input_file: Path to input file
        
    Returns:
        List of paths to chunk files
    
    Raises:
        FileNotFoundError: If input file doesn't exist
    """
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    logging.info(f"Starting to chunk and sort input file: {input_file}")
    start_time = time.time()
    
    chunk_files: List[str] = []
    chunk: List[str] = []
    chunk_idx: int = 0

    try:
        with open(input_file, 'r', encoding='ascii') as f:
            for line in f:
                chunk.append(line)
                if len(chunk) >= CHUNK_SIZE_LINES:
                    path = write_sorted_chunk(chunk, chunk_idx)
                    chunk_files.append(path)
                    chunk = []
                    chunk_idx += 1

        if chunk:  
            path = write_sorted_chunk(chunk, chunk_idx)
            chunk_files.append(path)

    except IOError as e:
        logging.error(f"Error processing input file: {e}")
        raise

    elapsed_time = time.time() - start_time
    logging.info(f"Completed chunking and sorting in {elapsed_time:.2f} seconds")
    return chunk_files

def merge_and_deduplicate_chunks(chunk_files: List[str], output_file: str) -> None:
    logging.info(f"Starting to merge and deduplicate chunks into: {output_file}")
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
    log_memory_usage()

def clean_up(files):
    logging.info("Starting cleanup of temporary files")
    for path in files:
        os.remove(path)
        logging.info(f"Removed file: {path}")
    os.rmdir(TEMP_DIR)
    logging.info(f"Removed temporary directory: {TEMP_DIR}")

def verify_sorted_file(filename: str) -> bool:
    """
    Verify that a file is sorted and has no duplicates.
    
    Args:
        filename: Path to file to verify
    Returns:
        bool: True if file is sorted and deduplicated
    """
    logging.info(f"Verifying file is sorted: {filename}")
    prev_line = None
    line_count = 0
    
    try:
        with open(filename, 'r', encoding='ascii') as f:
            for line in f:
                line = line.rstrip()
                line_count += 1
                
                if prev_line is not None and line <= prev_line:
                    logging.error(f"File not sorted at line {line_count}: '{prev_line}' followed by '{line}'")
                    return False
                prev_line = line
                
        logging.info(f"Verified {line_count} lines are properly sorted")
        return True
        
    except IOError as e:
        logging.error(f"Error verifying file: {e}")
        return False

def main():
    config = Config.from_args()
    
    if config.test_file:
        
        is_sorted = verify_sorted_file(config.test_file)
        sys.exit(0 if is_sorted else 1)
        
    logging.info("Process started")
    log_memory_usage()

    global CHUNK_SIZE_LINES
    CHUNK_SIZE_LINES = config.chunk_size

    if config.test_file:
        if verify_sorted_file(config.test_file):
            logging.info(f"File {config.test_file} is sorted and deduplicated")
        else:
            logging.error(f"File {config.test_file} is not sorted or has duplicates")
    else:
        chunk_files = chunk_and_sort_input(config.input_file)
        merge_and_deduplicate_chunks(chunk_files, config.output_file)
        clean_up(chunk_files)

    logging.info("Process completed successfully")

if __name__ == "__main__":
    main()
