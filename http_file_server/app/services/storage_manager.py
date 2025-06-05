import os
import shutil
from pathlib import Path
import asyncio
import aiofiles
import aiofiles.os
import hashlib
from typing import Dict, Tuple
import logging
from logger_config import setup_logger
import config
import json

logger = setup_logger()

class StorageManager:
    def __init__(self, data_dir: Path, temp_dir: Path):
        self.data_dir = data_dir
        self.temp_dir = temp_dir
        self.disk_usage: int = 0
        self.disk_usage_lock = asyncio.Lock()
        
    async def get_file_size(self, path: Path) -> int:
        """Get file size asynchronously."""
        if await aiofiles.os.path.exists(path):
            stat = await aiofiles.os.stat(path)
            return stat.st_size
        return 0
        
    async def initialize(self):
        """Initialize the storage manager, calculate current disk usage."""
        logger.info("Initializing storage manager...")
        
        # Create directories if they don't exist
        self.data_dir.mkdir(exist_ok=True, parents=True)
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        logger.debug(f"Storage directories created/verified: {self.data_dir}, {self.temp_dir}")

        # RON: consider refactoring to a standalone method
        # Clean temp directory at startup
        files_removed = 0
        for file in self.temp_dir.glob("*"):
            if file.is_file():
                await aiofiles.os.unlink(file)
                files_removed += 1
        logger.info(f"Cleaned temporary directory, removed {files_removed} files")

        # RON: consider refactoring to a standalone method: self.disk_usage = calculate_disk_usage()
        # Calculate current disk usage
        self.disk_usage = 0
        for folder_path, _, files in os.walk(self.data_dir):
            for file in files:
                if file.endswith((".blob", ".headers")):
                    file_path = Path(folder_path) / file
                    size = await self.get_file_size(file_path)
                    self.disk_usage += size
        
        logger.info(f"Current disk usage: {self.disk_usage / (1024*1024):.2f} MB")
        
        # Check available disk space
        _, _, free = shutil.disk_usage(str(self.data_dir))
        required_space = config.MAX_DISK_QUOTA * 1.5
        if free < required_space:
            raise RuntimeError(f"Insufficient disk space. Need at least {required_space / (1024*1024*1024):.2f} GB free")

    def get_blob_path(self, blob_id: str) -> Tuple[Path, Path, Path]:
        """Get the paths where a blob should be stored based on its ID."""
        # Use first 2 chars of MD5 hash as directory name
        hash_prefix = hashlib.md5(blob_id.encode()).hexdigest()[:2]
        directory = self.data_dir / hash_prefix
        directory.mkdir(exist_ok=True)
        
        blob_path = directory / f"{blob_id}.blob"
        headers_path = directory / f"{blob_id}.headers"
        metadata_path = directory / f"{blob_id}.meta"  # New metadata file
        
        return blob_path, headers_path, metadata_path

    async def store_metadata(self, blob_id: str, original_filename: str):
        """Store metadata about the blob."""
        _, _, metadata_path = self.get_blob_path(blob_id)
        metadata = {
            "original_filename": original_filename
        }
        async with aiofiles.open(metadata_path, 'w') as f:
            await f.write(json.dumps(metadata))

    async def get_metadata(self, blob_id: str) -> dict:
        """Get metadata about the blob."""
        _, _, metadata_path = self.get_blob_path(blob_id)
        try:
            async with aiofiles.open(metadata_path, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        
    async def update_disk_usage(self, size_change: int):
        """Update the disk usage counter thread-safely."""
        async with self.disk_usage_lock:
            previous_usage = self.disk_usage
            self.disk_usage += size_change
            # RON: logging is not part of the critical code. consider moving the logging outside the lock's scope
            logger.debug(f"Disk usage updated. Previous: {previous_usage}, Change: {size_change}, New: {self.disk_usage}")
        
    # RON: returned value is not used. consider changing the signature.
    async def delete_blob(self, blob_id: str) -> bool:
        """Delete a blob and its associated files."""
        blob_path, headers_path, metadata_path = self.get_blob_path(blob_id)
        
        # Get sizes before deleting
        blob_size = await self.get_file_size(blob_path)
        headers_size = await self.get_file_size(headers_path)
        metadata_size = await self.get_file_size(metadata_path)
        total_size = blob_size + headers_size + metadata_size
        
        # Delete files if they exist
        if blob_size > 0:
            await aiofiles.os.unlink(blob_path)
        if headers_size > 0:
            await aiofiles.os.unlink(headers_path)
        if metadata_size > 0:
            await aiofiles.os.unlink(metadata_path)
            
        # Update disk usage if any files were deleted
        if total_size > 0:
            await self.update_disk_usage(-total_size)
            return True
        return False

    def check_disk_quota(self, additional_size: int) -> bool:
        """Check if storing additional data would exceed the disk quota.
        
        Args:
            additional_size: Size in bytes of new data to be stored
            
        Returns:
            bool: True if adding the data won't exceed quota, False otherwise
        """
        return (self.disk_usage + additional_size) <= config.MAX_DISK_QUOTA