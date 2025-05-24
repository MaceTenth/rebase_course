"""Configuration settings for the HTTP File Server."""

# Storage limits
MAX_LENGTH = 10 * 1024 * 1024  # 10MB
MAX_DISK_QUOTA = 1024 * 1024 * 1024  # 1GB

# Header constraints
MAX_HEADER_LENGTH = 100
MAX_HEADER_COUNT = 20

# Blob constraints
MAX_ID_LENGTH = 200

# Directory paths
DATA_DIR = "./data"
TEMP_DIR = "./temp"