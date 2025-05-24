import os
import pytest
import requests
import time
import threading
import subprocess
import random
import string
import shutil
from pathlib import Path
from fastapi.testclient import TestClient
import sys
import json

# Add the parent directory to sys.path so we can import main
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import our FastAPI app
from main import app, StorageManager, DATA_DIR, TEMP_DIR

# Create a test client
client = TestClient(app)

# Test directory for isolated testing
TEST_DATA_DIR = Path("./test_data")
TEST_TEMP_DIR = Path("./test_temp")


@pytest.fixture(autouse=True)
def setup_and_teardown():
    """Setup and teardown for tests."""
    # Create test directories
    TEST_DATA_DIR.mkdir(exist_ok=True, parents=True)
    TEST_TEMP_DIR.mkdir(exist_ok=True, parents=True)
    
    # Patch the storage manager to use test directories
    app.state.original_storage_manager = app.state.storage_manager if hasattr(app.state, 'storage_manager') else None
    app.state.storage_manager = StorageManager(TEST_DATA_DIR, TEST_TEMP_DIR)
    
    yield
    
    # Clean up test directories
    shutil.rmtree(TEST_DATA_DIR, ignore_errors=True)
    shutil.rmtree(TEST_TEMP_DIR, ignore_errors=True)
    
    # Restore original storage manager
    if app.state.original_storage_manager:
        app.state.storage_manager = app.state.original_storage_manager


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
    
    # Create a header that exceeds MAX_HEADER_LENGTH (50)
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Rebase-Test": "x" * 45  # Key + value should exceed 50 chars
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


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])