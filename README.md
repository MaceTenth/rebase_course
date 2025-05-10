# Bloom Filter Deduplication Tool

A Python tool that uses Bloom filters for efficient data deduplication with low memory overhead.

## Overview

This project implements a memory-efficient deduplication system using Bloom filters - a probabilistic data structure that can quickly determine whether an element is present in a set. The tool is particularly useful for deduplicating large datasets where storing the complete data in memory is impractical.

## Requirements

- Python 3.13+
- Docker (alternatively)
- Required packages:
  - `pybloom_live` - Bloom filter implementation
  - `psutil` - For memory usage monitoring

## Installation

### Using Python directly

1. Create a virtual environment:
   ```sh
   python -m venv bloom
   source bloom/bin/activate
   ```

2. Install dependencies:
   ```sh
   pip install pybloom-live psutil
   ```

### Using Docker

1. Build the Docker images:
   ```sh
   docker-compose build
   ```

## Usage

The tool provides two deduplication implementations:

1. `bloom_dedup.py` - Uses Bloom filter for memory-efficient deduplication
2. `set_dedup.py` - Uses Python sets for exact but more memory-intensive deduplication

### Running with Python

```sh
python bloom_dedup.py input.txt output.txt --expected-lines 1000000
python set_dedup.py input.txt output.txt --chunk-size 100000
```

### Running with Docker

1. Place your input files in the `data` directory
2. Run either implementation:

```sh
# Using set-based deduplication
docker-compose run set_dedup python set_dedup.py /app/data/input.txt /app/data/output.txt --chunk-size 100000

# Using bloom filter deduplication
docker-compose run bloom_dedup python bloom_dedup.py /app/data/input.txt /app/data/output.txt --expected-lines 5000000 --error-rate 0.01
```

### Docker Commands Reference

```sh
# Build the Docker images
docker-compose build

# Run set-based deduplication with custom chunk size
docker-compose run set_dedup python set_dedup.py /app/data/input.txt /app/data/output.txt --chunk-size 100000

# Run bloom filter deduplication with custom parameters
docker-compose run bloom_dedup python bloom_dedup.py /app/data/input.txt /app/data/output.txt --expected-lines 5000000 --error-rate 0.01

# Test with small file
docker-compose run set_dedup python set_dedup.py /app/data/test_input.txt /app/data/test_output.txt --chunk-size 1000

# Test with bloom filter
docker-compose run bloom_dedup python bloom_dedup.py /app/data/test_input.txt /app/data/test_output.txt --expected-lines 100 --error-rate 0.01

# Open a shell in the container
docker-compose run set_dedup bash

# View logs
docker-compose logs

# Clean up containers
docker-compose down
```

Note: 
- All input and output files should be placed in the `data` directory
- The `--error-rate` parameter controls the false positive probability (default: 0.001)
- Lower error rates require more memory but provide better accuracy
- Higher error rates use less memory but may occasionally miss duplicates

## Memory Usage

The tool actively monitors memory usage through the `psutil` library and logs memory consumption during execution. The Bloom filter implementation provides significant memory savings compared to storing the complete dataset, with a small false positive probability.

