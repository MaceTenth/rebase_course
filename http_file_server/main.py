import os
import re
import shutil
from pathlib import Path
from typing import Dict, Tuple, Optional, Annotated, Union
from fastapi import FastAPI, Request, HTTPException, Depends, Header, UploadFile, File, Form
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
from app.services.storage_manager import StorageManager

# Data storage path
DATA_DIR = Path(config.DATA_DIR)
TEMP_DIR = Path(config.TEMP_DIR)

# Logger setup
logger = setup_logger()


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
    file: UploadFile = File(...),
    request: Request = None
):
    """Upload a binary blob with the given ID.
    
    Args:
        blob_id: The ID to store the blob under
        file: The file to upload
    """
    storage_manager = request.app.state.storage_manager
    
    logger.info(f"Receiving upload request for blob_id: {blob_id}")
    
    # Get content length from the file
    file.file.seek(0, 2)  # Seek to end
    content_length_value = file.file.tell()  # Get file size
    file.file.seek(0)  # Reset to beginning
    
    logger.debug(f"Content-Length: {content_length_value} bytes")

    # Validate ID
    validate_blob_id(blob_id)
    logger.debug(f"Blob ID validation passed: {blob_id}")

    # Extract headers that should be stored
    headers = {k.lower(): v for k, v in request.headers.items()}
    storable_headers = get_storable_headers(headers)

    # Store original filename from the uploaded file
    original_filename = file.filename or blob_id
    
    # Add content type to storable headers if provided
    if file.content_type:
        storable_headers["content-type"] = file.content_type
    
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
    blob_path, headers_path, metadata_path = storage_manager.get_blob_path(blob_id)
    temp_blob_path = TEMP_DIR / f"{blob_id}_temp.blob"
    temp_headers_path = TEMP_DIR / f"{blob_id}_temp.headers"

    # Get sizes of existing files if we're overwriting
    old_blob_size = blob_path.stat().st_size if blob_path.exists() else 0
    old_headers_size = headers_path.stat().st_size if headers_path.exists() else 0
    
    try:
        # First write headers to temp file
        async with aiofiles.open(temp_headers_path, 'w') as f:
            for key, value in storable_headers.items():
                await f.write(f"{key}: {value}\n")
        
        # Save content using chunks for memory efficiency
        content_size = 0
        async with aiofiles.open(temp_blob_path, 'wb') as f:
            while chunk := await file.read(8192):  # 8KB chunks
                content_size += len(chunk)
                await f.write(chunk)

        # Store metadata including original filename
        await storage_manager.store_metadata(blob_id, original_filename)
        
        # Move files to final location
        await aiofiles.os.rename(str(temp_headers_path), str(headers_path))
        await aiofiles.os.rename(str(temp_blob_path), str(blob_path))
        
        # Update disk usage
        disk_usage_change = (content_size + headers_size) - (old_blob_size + old_headers_size)
        await storage_manager.update_disk_usage(disk_usage_change)
        
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
    
    # Get blob paths
    blob_path, headers_path, _ = storage_manager.get_blob_path(blob_id)
    
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

    # Get metadata to use original filename
    metadata = await storage_manager.get_metadata(blob_id)
    original_filename = metadata.get('original_filename', blob_id)
    
    # Try to infer content type from original filename if not specified in headers
    if content_type == "application/octet-stream":
        guessed_type, _ = mimetypes.guess_type(original_filename)
        if guessed_type:
            content_type = guessed_type
            
    # Add Content-Disposition header with original filename
    headers['content-disposition'] = f'attachment; filename="{original_filename}"'
    
    # Create streaming response
    async def file_iterator():
        async with aiofiles.open(blob_path, 'rb') as file:
            while chunk := await file.read(8192):  # 8KB chunks
                yield chunk
    
    return StreamingResponse(
        file_iterator(),
        media_type=content_type,
        headers=headers
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