"""Configuration settings for the HTTP File Server."""

# Storage limits
MAX_LENGTH = 10 * 1024 * 1024  # 10MB
MAX_DISK_QUOTA = 1024 * 1024 * 1024  # 1GB

# Header constraints
MAX_HEADER_LENGTH = 50
MAX_HEADER_COUNT = 20

# Blob constraints
MAX_ID_LENGTH = 200
MAX_BLOBS_IN_FOLDER = 10000

# Directory paths
DATA_DIR = "./data"
TEMP_DIR = "./temp"