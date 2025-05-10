import os
import logging
import time
import psutil
import argparse
from dataclasses import dataclass
from pybloom_live import BloomFilter

@dataclass
class Config:
    input_file: str
    output_file: str
    expected_lines: int
    error_rate: float
    encoding: str = 'ascii'

    @classmethod
    def from_args(cls) -> 'Config':
        """Create Config from command line arguments."""
        parser = argparse.ArgumentParser(description='Large file deduplication tool using Bloom Filter')
        parser.add_argument('input_file', help='Input file path')
        parser.add_argument('output_file', help='Output file path')
        parser.add_argument('--expected-lines', type=int, default=5_000_000,
                           help='Expected number of lines in input file')
        parser.add_argument('--error-rate', type=float, default=0.001,
                           help='Bloom filter error rate')
        parser.add_argument('--encoding', type=str, default='ascii',
                           help='File encoding')
        args = parser.parse_args()
        
        return cls(
            input_file=args.input_file,
            output_file=args.output_file,
            expected_lines=args.expected_lines,
            error_rate=args.error_rate,
            encoding=args.encoding
        )


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_memory_usage() -> None:
    """Log current process memory usage in MB."""
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

def deduplicate_with_bloom(config: Config) -> None:
    """
    Deduplicate file contents using a Bloom filter.
    
    Args:
        config: Configuration object containing processing parameters
    """
    logging.info(f"Starting deduplication with Bloom Filter")
    logging.info(f"Input file: {config.input_file}, Output file: {config.output_file}")
    logging.info(f"Expected lines: {config.expected_lines}, Error rate: {config.error_rate}")

    start_time = time.time()
    bf = BloomFilter(capacity=config.expected_lines, error_rate=config.error_rate)
    log_memory_usage()

    try:
        with open(config.input_file, 'r', encoding=config.encoding) as inp, \
             open(config.output_file, 'w', encoding=config.encoding) as out:

            line_count = 0
            unique_count = 0

            for line in inp:
                line = line.rstrip('\n')
                line_count += 1
                if line not in bf:
                    bf.add(line)
                    out.write(line + '\n')
                    unique_count += 1

                
                if line_count % 1_000_000 == 0:
                    logging.info(f"Processed {line_count} lines, {unique_count} unique lines so far")
                    log_memory_usage()

    except IOError as e:
        logging.error(f"Error processing files: {e}")
        raise

    elapsed_time = time.time() - start_time
    logging.info(f"Deduplication completed in {elapsed_time:.2f} seconds")
    logging.info(f"Total lines processed: {line_count}, Unique lines written: {unique_count}")
    log_memory_usage()

def main():
    config = Config.from_args()
    logging.info("Process started")
    log_memory_usage()

    deduplicate_with_bloom(config)

    logging.info("Process completed successfully")

if __name__ == "__main__":
    main()
