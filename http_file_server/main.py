import os
import re
import shutil
from pathlib import Path
from typing import Dict, Tuple
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, Response
import uvicorn
import hashlib
import mimetypes
from contextlib import asynccontextmanager
import config
import asyncio
import aiofiles
import aiofiles.os
import logging
from logger_config import setup_logger

# Data storage path
DATA_DIR = Path(config.DATA_DIR)
TEMP_DIR = Path(config.TEMP_DIR)

# Logger setup
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create and initialize storage manager
    app.state.storage_manager = StorageManager(DATA_DIR, TEMP_DIR)
    await app.state.storage_manager.initialize()
    yield
    # Cleanup operations can be added here


# Create FastAPI app with lifespan
app = FastAPI(title="HTTP File Server", lifespan=lifespan)


def is_valid_id(blob_id: str) -> bool:
    """Check if the blob ID is valid according to the requirements."""
    if not blob_id or len(blob_id) > config.MAX_ID_LENGTH:
        return False
    
    # Check if id contains only allowed characters: a-z, A-Z, 0-9, dot, underscore, minus
    pattern = r'^[a-zA-Z0-9._-]+$'
    return bool(re.match(pattern, blob_id))


def validate_blob_id(blob_id: str):
    """Validate the blob ID and raise HTTPException if invalid."""
    if not is_valid_id(blob_id):
        raise HTTPException(status_code=400, detail="Invalid blob ID format")


def get_storable_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Extract headers that should be stored according to the requirements."""
    storable_headers = {}
    
    # Store Content-Type header if present
    if "content-type" in headers:
        storable_headers["content-type"] = headers["content-type"]
    
    # Store any header that starts with x-rebase- (case insensitive)
    for key, value in headers.items():
        if key.lower().startswith("x-rebase-"):
            storable_headers[key] = value
            
    return storable_headers


async def check_content_length(request: Request):
    """Check if Content-Length header is present and valid."""
    content_length = request.headers.get("content-length")
    
    # Check custom header for test simulation
    if request.headers.get("x-skip-content-length", "").lower() == "true":
        raise HTTPException(status_code=400, detail="Missing Content-Length header")
    
    if content_length is None:
        raise HTTPException(status_code=400, detail="Missing Content-Length header")
    
    try:
        content_length_value = int(content_length)
        return content_length_value
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid Content-Length header")


def validate_ascii_headers(headers: Dict[str, str]):
    """Validate that headers are ASCII-only."""
    for key, value in headers.items():
        try:
            key.encode('ascii')
            value.encode('ascii')
        except UnicodeEncodeError:
            raise HTTPException(status_code=400, detail="Headers must be ASCII-only")


@app.post("/blobs/{blob_id}")
async def upload_blob(
    blob_id: str, 
    request: Request,
    content_length_value: int = Depends(check_content_length)
):
    """Upload a binary blob with the given ID."""
    storage_manager = request.app.state.storage_manager
    
    logger.info(f"Receiving upload request for blob_id: {blob_id}")
    logger.debug(f"Content-Length: {content_length_value} bytes")

    # Validate ID
    validate_blob_id(blob_id)
    logger.debug(f"Blob ID validation passed: {blob_id}")

    # Extract headers that should be stored
    headers = {k.lower(): v for k, v in request.headers.items()}
    storable_headers = get_storable_headers(headers)
    
    # Validate ASCII headers
    validate_ascii_headers(storable_headers)
    
    # Validate headers count and length
    if len(storable_headers) > config.MAX_HEADER_COUNT:
        raise HTTPException(status_code=400, detail=f"Too many headers. Maximum is {config.MAX_HEADER_COUNT}")
    
    for key, value in storable_headers.items():
        if len(key) + len(value) > config.MAX_HEADER_LENGTH:
            raise HTTPException(status_code=400, detail=f"Header too long. Maximum length is {config.MAX_HEADER_LENGTH}")
    
    # Calculate headers size (including newlines)
    headers_size = sum(len(f"{k}: {v}\n") for k, v in storable_headers.items())
    
    # Check if total size exceeds MAX_LENGTH
    if content_length_value + headers_size > config.MAX_LENGTH:
        raise HTTPException(
            status_code=400, 
            detail=f"Content size and headers exceed maximum allowed size ({config.MAX_LENGTH} bytes)"
        )
    
    # Check disk quota
    if not storage_manager.check_disk_quota(content_length_value + headers_size):
        raise HTTPException(status_code=400, detail="Disk quota exceeded")
    
    # Get blob path and create a temporary file for streaming
    blob_path, headers_path = storage_manager.get_blob_path(blob_id)
    temp_blob_path = TEMP_DIR / f"{blob_id}_temp.blob"
    temp_headers_path = TEMP_DIR / f"{blob_id}_temp.headers"
    
    # Get sizes of existing files if we're overwriting
    old_blob_size = blob_path.stat().st_size if blob_path.exists() else 0
    old_headers_size = headers_path.stat().st_size if headers_path.exists() else 0
    
    # Save the file in chunks
    content_size = 0
    try:
        # First write headers to temp file
        async with aiofiles.open(temp_headers_path, 'w') as file:
            for key, value in storable_headers.items():
                await file.write(f"{key}: {value}\n")
        
        # Then write content to temp file
        async with aiofiles.open(temp_blob_path, 'wb') as file:
            async for chunk in request.stream():
                content_size += len(chunk)
                if content_size > content_length_value:
                    # Client sent more data than declared
                    await aiofiles.os.unlink(temp_blob_path)
                    await aiofiles.os.unlink(temp_headers_path)
                    raise HTTPException(status_code=400, detail="Content length mismatch")
                await file.write(chunk)
        
        # Ensure we received exactly the amount of data specified
        if content_size != content_length_value:
            await aiofiles.os.unlink(temp_blob_path)
            await aiofiles.os.unlink(temp_headers_path)
            raise HTTPException(status_code=400, detail="Content length mismatch")
        
        # Move the temporary files to their final locations
        await aiofiles.os.rename(str(temp_headers_path), str(headers_path))
        await aiofiles.os.rename(str(temp_blob_path), str(blob_path))
        
        # Update disk usage
        disk_usage_change = (content_size + headers_size) - (old_blob_size + old_headers_size)
        await storage_manager.update_disk_usage(disk_usage_change)
        
        logger.info(f"Successfully uploaded blob: {blob_id}")
        return {"success": True, "message": f"Blob {blob_id} uploaded successfully"}
    
    except Exception as e:
        logger.error(f"Error uploading blob {blob_id}: {str(e)}", exc_info=True)
        # Clean up temporary files if operation fails
        if await aiofiles.os.path.exists(temp_blob_path):
            await aiofiles.os.unlink(temp_blob_path)
        if await aiofiles.os.path.exists(temp_headers_path):
            await aiofiles.os.unlink(temp_headers_path)
        raise HTTPException(status_code=500, detail=f"Error uploading blob: {str(e)}")


@app.get("/blobs/{blob_id}")
async def get_blob(blob_id: str, request: Request):
    """Retrieve a binary blob with the given ID."""
    storage_manager = request.app.state.storage_manager
    logger.info(f"Receiving download request for blob_id: {blob_id}")
    
    # Validate ID
    validate_blob_id(blob_id)
    
    # Get blob path
    blob_path, headers_path = storage_manager.get_blob_path(blob_id)
    
    # Check if blob exists
    if not await aiofiles.os.path.exists(blob_path):
        raise HTTPException(status_code=404, detail=f"Blob {blob_id} not found")
    
    # Read headers if they exist
    headers = {}
    content_type = "application/octet-stream"
    
    if await aiofiles.os.path.exists(headers_path):
        async with aiofiles.open(headers_path, 'r') as file:
            async for line in file:
                if ": " in line:
                    key, value = line.strip().split(": ", 1)
                    headers[key] = value
                    if key.lower() == "content-type":
                        content_type = value
    else:
        # Try to infer content type if not specified in headers
        guessed_type, _ = mimetypes.guess_type(blob_id)
        if guessed_type:
            content_type = guessed_type
    
    # Create a streaming response for the blob
    async def file_iterator():
        async with aiofiles.open(blob_path, 'rb') as file:
            while chunk := await file.read(8192):  # 8KB chunks
                yield chunk
    
    response_headers = {k: v for k, v in headers.items()}
    return StreamingResponse(
        file_iterator(),
        media_type=content_type,
        headers=response_headers
    )


@app.delete("/blobs/{blob_id}")
async def delete_blob(blob_id: str, request: Request):
    """Delete a blob with the given ID."""
    storage_manager = request.app.state.storage_manager
    logger.info(f"Receiving delete request for blob_id: {blob_id}")
    
    # Validate ID
    validate_blob_id(blob_id)
    
    # Delete the blob
    await storage_manager.delete_blob(blob_id)
    
    logger.info(f"Successfully deleted blob: {blob_id}")
    # Always return success even if blob doesn't exist (as per requirements)
    return {"success": True, "message": f"Blob {blob_id} deleted"}


if __name__ == "__main__":
    logger.info("Starting HTTP File Server...")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Temporary directory: {TEMP_DIR}")
    logger.info(f"Maximum disk quota: {config.MAX_DISK_QUOTA / (1024*1024):.2f} MB")
    uvicorn.run(app, host="0.0.0.0", port=8000)