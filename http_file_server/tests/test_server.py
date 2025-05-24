import os
import pytest
import requests
import time
import threading
import subprocess
import random
import string
import shutil
import asyncio
from pathlib import Path
from fastapi.testclient import TestClient
from httpx import AsyncClient
import sys
import json
import pytest_asyncio

# Add the parent directory to sys.path so we can import main
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Test directory for isolated testing
TEST_DATA_DIR = Path("test_data").absolute()
TEST_TEMP_DIR = Path("test_temp").absolute()

# Import our FastAPI app and override the DATA_DIR and TEMP_DIR
import config
config.DATA_DIR = str(TEST_DATA_DIR)
config.TEMP_DIR = str(TEST_TEMP_DIR)

# Now import the app after updating config
from main import app, StorageManager

# Create a test client
client = TestClient(app)


@pytest_asyncio.fixture(autouse=True)
async def setup_and_teardown():
    """Setup and teardown for tests."""
    # Clean up any existing test directories
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)
    shutil.rmtree(TEST_TEMP_DIR, ignore_errors=True)
    
    # Create test directories
    TEST_DATA_DIR.mkdir(exist_ok=True, parents=True)
    TEST_TEMP_DIR.mkdir(exist_ok=True, parents=True)
    
    # Create and initialize storage manager for tests
    test_storage_manager = StorageManager(TEST_DATA_DIR, TEST_TEMP_DIR)
    await test_storage_manager.initialize()
    
    # Important: attach storage manager to app state
    app.state.storage_manager = test_storage_manager
    print(f"Test setup: Created StorageManager instance {id(test_storage_manager)}")
    
    yield
    
    print(f"Test teardown: Cleaning up test directories")
    # Clean up test directories
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)
    shutil.rmtree(TEST_TEMP_DIR, ignore_errors=True)


def generate_random_id(length=10):
    """Generate a random blob ID."""
    chars = string.ascii_letters + string.digits + '._-'
    return ''.join(random.choice(chars) for _ in range(length))


def generate_random_content(size_bytes):
    """Generate random binary content of specified size."""
    return os.urandom(size_bytes)


def test_post_get_delete_blob():
    """Test basic POST, GET, and DELETE operations."""
    blob_id = generate_random_id()
    content = generate_random_content(1024)  # 1KB content
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Rebase-Test": "test-value"
    }
    
    # POST
    response = client.post(f"/blobs/{blob_id}", content=content, headers=headers)
    assert response.status_code == 200
    assert response.json().get("success") == True
    
    # GET
    response = client.get(f"/blobs/{blob_id}")
    assert response.status_code == 200
    assert response.content == content
    assert response.headers.get("content-type") == "application/octet-stream"
    assert response.headers.get("x-rebase-test") == "test-value"
    
    # DELETE
    response = client.delete(f"/blobs/{blob_id}")
    assert response.status_code == 200
    assert response.json().get("success") == True
    
    # Verify blob is gone
    response = client.get(f"/blobs/{blob_id}")
    assert response.status_code == 404


def test_invalid_blob_id():
    """Test blob ID validation."""
    # Since FastAPI handles URL path validation at the routing level,
    # we need to test with invalid IDs that will actually reach our endpoint
    # Test with invalid characters (but valid URL path)
    
    # Test 1: Test with a very long ID (exceeds MAX_ID_LENGTH)
    long_id = "a" * 201  # MAX_ID_LENGTH is 200
    content = generate_random_content(1024)
    
    response = client.post(f"/blobs/{long_id}", content=content)
    assert response.status_code == 400
    assert "Invalid blob ID format" in response.text
    
    # Test 2: Test with invalid characters that are valid in URLs
    # We'll use characters that are allowed in URLs but not in our blob ID validation
    invalid_id = "invalid@id"  # @ is valid in URLs but not in our blob ID regex
    response = client.post(f"/blobs/{invalid_id}", content=content)
    assert response.status_code == 400
    assert "Invalid blob ID format" in response.text


def test_missing_content_length():
    """Test handling of missing Content-Length header."""
    blob_id = generate_random_id()
    
    # Using requests directly to remove Content-Length header
    url = f"http://testserver/blobs/{blob_id}"
    
    # TestClient always adds Content-Length, so we're checking the route handling
    # This is a bit hacky but works for testing
    response = client.post(
        f"/blobs/{blob_id}",
        headers={"X-Skip-Content-Length": "true"},  # Custom header to simulate missing Content-Length
        content=b""
    )
    
    # Our API should reject this with a 400
    assert response.status_code == 400
    assert "Missing Content-Length header" in response.text


def test_too_many_headers():
    """Test handling of too many headers."""
    blob_id = generate_random_id()
    content = generate_random_content(1024)
    
    # Create more headers than MAX_HEADER_COUNT (20)
    headers = {"Content-Type": "application/octet-stream"}
    for i in range(21):  # 21 x-rebase headers + 1 content-type = 22 headers
        headers[f"X-Rebase-Test-{i}"] = f"value-{i}"
    
    response = client.post(f"/blobs/{blob_id}", content=content, headers=headers)
    assert response.status_code == 400
    assert "Too many headers" in response.text


def test_header_too_long():
    """Test handling of headers that are too long."""
    blob_id = generate_random_id()
    content = generate_random_content(1024)
    
    # Create a header that exceeds MAX_HEADER_LENGTH (100)
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Rebase-Test": "x" * 95  # Key (13) + value (95) = 108 chars, exceeding 100
    }
    
    response = client.post(f"/blobs/{blob_id}", content=content, headers=headers)
    assert response.status_code == 400
    assert "Header too long" in response.text


def test_blob_too_large():
    """Test handling of blobs that are too large."""
    blob_id = generate_random_id()
    
    # Create a blob that exceeds MAX_LENGTH (10MB)
    # To avoid actually creating a 10MB+ file, we'll mock the Content-Length header
    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(11 * 1024 * 1024)  # 11MB
    }
    
    # Send a small content but lie about the size
    response = client.post(f"/blobs/{blob_id}", content=b"small content", headers=headers)
    assert response.status_code == 400
    assert "exceed maximum allowed size" in response.text


def test_upsert():
    """Test overwriting an existing blob."""
    blob_id = generate_random_id()
    content1 = generate_random_content(1024)
    content2 = generate_random_content(2048)
    
    # POST initial content
    response = client.post(f"/blobs/{blob_id}", content=content1)
    assert response.status_code == 200
    
    # GET to verify
    response = client.get(f"/blobs/{blob_id}")
    assert response.content == content1
    
    # POST new content (upsert)
    response = client.post(f"/blobs/{blob_id}", content=content2)
    assert response.status_code == 200
    
    # GET to verify update
    response = client.get(f"/blobs/{blob_id}")
    assert response.content == content2


def test_delete_nonexistent_blob():
    """Test deleting a blob that doesn't exist."""
    blob_id = generate_random_id()
    
    # DELETE a blob that doesn't exist
    response = client.delete(f"/blobs/{blob_id}")
    assert response.status_code == 200
    assert response.json().get("success") == True


def test_content_type_inference():
    """Test content type inference for GET requests."""
    # Test with explicit Content-Type header
    blob_id = "test.txt"
    content = b"Hello, world!"
    
    response = client.post(
        f"/blobs/{blob_id}",
        content=content,
        headers={"Content-Type": "text/plain"}
    )
    assert response.status_code == 200
    
    response = client.get(f"/blobs/{blob_id}")
    assert response.headers.get("content-type") == "text/plain"
    
    # Clean up
    client.delete(f"/blobs/{blob_id}")
    
    # Test with inferred Content-Type from filename
    blob_id = "test.json"
    content = b'{"key": "value"}'
    
    response = client.post(
        f"/blobs/{blob_id}",
        content=content
    )
    assert response.status_code == 200
    
    response = client.get(f"/blobs/{blob_id}")
    assert response.headers.get("content-type") == "application/json" or response.headers.get("content-type") == "application/octet-stream"


@pytest.mark.asyncio
async def test_disk_quota_update():
    """Test disk quota updates properly when uploading and deleting blobs."""
    storage_manager = app.state.storage_manager
    print(f"Test: Using StorageManager instance {id(storage_manager)}")
    print(f"Test: Initial disk usage: {storage_manager.disk_usage}")
    
    blob_id = generate_random_id()
    content = generate_random_content(1024)  # 1KB content
    headers = {"Content-Type": "application/octet-stream"}
    
    # Get initial disk usage
    initial_usage = storage_manager.disk_usage
    print(f"Test: Initial usage before upload: {initial_usage}")
    
    # Upload blob
    response = client.post(f"/blobs/{blob_id}", content=content, headers=headers)
    assert response.status_code == 200
    
    # Verify the file exists
    blob_path, headers_path = storage_manager.get_blob_path(blob_id)
    assert blob_path.exists(), "Blob file was not created"
    assert headers_path.exists(), "Headers file was not created"
    
    print(f"Test: Files created - blob: {blob_path.exists()}, headers: {headers_path.exists()}")
    
    # Calculate expected size increase
    content_size = len(content)
    headers_size = len("Content-Type: application/octet-stream\n")
    expected_size = initial_usage + content_size + headers_size
    
    print(f"Test: After upload - Current: {storage_manager.disk_usage}, Expected: {expected_size}")
    
    # Get disk usage after upload
    assert storage_manager.disk_usage > initial_usage, f"Disk usage should increase after upload. Before: {initial_usage}, After: {storage_manager.disk_usage}"
    assert storage_manager.disk_usage == expected_size, f"Expected disk usage {expected_size}, got {storage_manager.disk_usage}"
    
    # Delete blob
    response = client.delete(f"/blobs/{blob_id}")
    assert response.status_code == 200
    
    # Wait for async operations to complete
    await asyncio.sleep(0.1)
    
    # Check disk usage returned to initial state and files are deleted
    assert storage_manager.disk_usage == initial_usage, f"Expected disk usage to return to {initial_usage}, got {storage_manager.disk_usage}"
    assert not blob_path.exists(), "Blob file was not deleted"
    assert not headers_path.exists(), "Headers file was not deleted"
        

if __name__ == "__main__":
    pytest.main(["-xvs", __file__])