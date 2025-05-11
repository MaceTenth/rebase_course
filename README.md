# Data Deduplication Tool

A high-performance Python tool for deduplicating large text files with multiple implementation strategies.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Implementation Strategies](#implementation-strategies)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Performance Considerations](#performance-considerations)
- [Docker Support](#docker-support)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project provides efficient solutions for deduplicating large text files where traditional in-memory approaches might fail. It implements two distinct strategies: a memory-optimized Bloom filter approach and a disk-based chunking strategy using Python sets.

## Features

- **Memory efficiency**: Process gigabytes of data with limited RAM
- **Multiple deduplication strategies**: Choose based on your memory constraints and accuracy requirements
- **Performance monitoring**: Built-in memory and CPU usage tracking
- **Docker support**: Run in containerized environments
- **Configurable parameters**: Tune for your specific dataset characteristics

## Implementation Strategies

| Strategy | File | Pros | Cons | Best For |
|----------|------|------|------|----------|
| **Bloom Filter** | `bloom_dedup.py` | Very memory efficient, fast | Small possibility of false positives | Extremely large datasets with tight memory constraints |
| **Set-based Chunking** | `set_dedup.py` | 100% accuracy, simpler | Higher memory requirements | Medium-sized datasets where perfect accuracy is required |

## Requirements

- Python 3.13+
- Required packages:
  - `pybloom_live` - Bloom filter implementation
  - `psutil` - For resource monitoring
- Docker (optional)

## Installation

### Using Python

```sh
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Using Docker

```sh
# Build the Docker images
docker-compose build
```

## Usage

### Set-based Deduplication

```sh
python set_dedup.py input.txt output.txt --chunk-size 100000
```

Parameters:
- `--chunk-size`: Number of lines to process in memory at once (default: 100000)
- `--encoding`: File encoding (default: ascii)
- `--max-memory`: Maximum memory usage in MB (default: 4096)
- `--debug`: Enable detailed resource logging for monitoring memory and CPU usage

Example with debug mode:
```sh
python set_dedup.py input.txt output.txt --chunk-size 100000 --debug
```

### Bloom Filter Deduplication

```sh
python bloom_dedup.py input.txt output.txt --expected-lines 1000000 --error-rate 0.001
```

Parameters:
- `--expected-lines`: Estimated number of lines in input file
- `--error-rate`: Acceptable false positive rate (lower = more accurate but uses more memory)

## Performance Considerations

- The Bloom filter implementation uses approximately 9-12 bits per element at 1% error rate
- Memory usage is actively monitored during execution
- For files larger than available RAM, use the set-based implementation with appropriate chunk size
- Lower error rates in Bloom filters provide better accuracy but require more memory
- Use `--debug` flag for more detailed and frequent resource usage logs when troubleshooting performance issues

## Docker Support

Place your input files in the `data` directory to make them accessible to the Docker containers.

```sh
# Run set-based deduplication
docker-compose run set_dedup python set_dedup.py /app/data/input.txt /app/data/output.txt --chunk-size 100000

# Run with debug mode for detailed logging
docker-compose run set_dedup python set_dedup.py /app/data/input.txt /app/data/output.txt --chunk-size 100000 --debug

# Run bloom filter deduplication  
docker-compose run bloom_dedup python bloom_dedup.py /app/data/input.txt /app/data/output.txt --expected-lines 5000000
```

### Docker Commands Reference

```sh
# Build the Docker images
docker-compose build

# Test with small file
docker-compose run set_dedup python set_dedup.py /app/data/test_input.txt /app/data/test_output.txt --chunk-size 1000

# Open a shell in the container
docker-compose run set_dedup bash

# View logs
docker-compose logs

# Clean up containers
docker-compose down
```

## Troubleshooting

- **"Too many open files" error**: The tool automatically sets `ulimit -n 4096` but you may need to increase this value on your system
- **High memory usage**: Decrease chunk size for set-based deduplication or increase error rate for Bloom filters
- **Docker container crashes**: Check available memory in Docker settings
- **Monitoring resource usage**: Use the `--debug` flag to get detailed logs about memory and CPU usage during processing

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

