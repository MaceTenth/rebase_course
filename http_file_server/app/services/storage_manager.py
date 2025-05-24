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
        
        # Clean temp directory at startup
        files_removed = 0
        for file in self.temp_dir.glob("*"):
            if file.is_file():
                await aiofiles.os.unlink(file)
                files_removed += 1
        logger.info(f"Cleaned temporary directory, removed {files_removed} files")
        
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

    def get_blob_path(self, blob_id: str) -> Tuple[Path, Path]:
        """Get the path where a blob should be stored based on its ID."""
        # Use first 2 chars of MD5 hash as directory name to avoid too many files in one directory
        hash_prefix = hashlib.md5(blob_id.encode()).hexdigest()[:2]
        directory = self.data_dir / hash_prefix
        directory.mkdir(exist_ok=True)
        
        blob_path = directory / f"{blob_id}.blob"
        headers_path = directory / f"{blob_id}.headers"
        
        return blob_path, headers_path
    
    def check_disk_quota(self, additional_size: int) -> bool:
        """Check if storing additional data would exceed the disk quota."""
        return (self.disk_usage + additional_size) <= config.MAX_DISK_QUOTA
    
    async def update_disk_usage(self, size_change: int):
        """Update the disk usage counter thread-safely."""
        async with self.disk_usage_lock:
            previous_usage = self.disk_usage
            self.disk_usage += size_change
            logger.debug(f"Disk usage updated. Previous: {previous_usage}, Change: {size_change}, New: {self.disk_usage}")
        
    async def delete_blob(self, blob_id: str) -> bool:
        """Delete a blob and its headers file."""
        blob_path, headers_path = self.get_blob_path(blob_id)
        
        # Get sizes before deleting
        blob_size = await self.get_file_size(blob_path)
        headers_size = await self.get_file_size(headers_path)
        total_size = blob_size + headers_size
        
        # Delete files if they exist
        if blob_size > 0:
            await aiofiles.os.unlink(blob_path)
        if headers_size > 0:
            await aiofiles.os.unlink(headers_path)
            
        # Update disk usage if any files were deleted
        if total_size > 0:
            await self.update_disk_usage(-total_size)
            return True
        return False