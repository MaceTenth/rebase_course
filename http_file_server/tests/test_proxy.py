import pytest
import httpx
from fastapi.testclient import TestClient
from main import app
from unittest.mock import patch, MagicMock

client = TestClient(app)

def test_proxy_get_request():
    """Test basic GET request proxying."""
    target_url = "https://example.com/api/data"
    expected_content = b"Hello from target server"
    
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "text/plain"}
        mock_response.aiter_bytes.return_value = [expected_content]
        mock_get.return_value = mock_response
        
        response = client.get(f"/proxy?url={target_url}")
        
        # Verify response
        assert response.status_code == 200
        assert response.content == expected_content
        assert response.headers["content-type"] == "text/plain"
        
        # Verify request was forwarded correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args[1]
        assert call_args["url"] == target_url

def test_proxy_forwards_headers():
    """Test that headers are forwarded to target."""
    target_url = "https://example.com/api/data"
    test_headers = {
        "Host": "example.com",
        "X-Custom-Header": "test-value",
        "Accept": "application/json"
    }
    
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "application/json"}
        mock_response.aiter_bytes.return_value = [b"{}"]
        mock_get.return_value = mock_response
        
        response = client.get(
            f"/proxy?url={target_url}",
            headers=test_headers
        )
        
        # Verify headers were forwarded
        call_args = mock_get.call_args[1]
        forwarded_headers = call_args["headers"]
        assert forwarded_headers["Host"] == "example.com"
        assert forwarded_headers["X-Custom-Header"] == "test-value"
        assert forwarded_headers["Accept"] == "application/json"

def test_proxy_forwards_query_params():
    """Test that query parameters in the target URL are preserved."""
    base_url = "https://example.com/api/data"
    query_params = "?key=value&foo=bar"
    target_url = base_url + query_params
    
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.aiter_bytes.return_value = [b""]
        mock_get.return_value = mock_response
        
        response = client.get(f"/proxy?url={target_url}")
        assert response.status_code == 200
        
        # Verify full URL with query params was used
        mock_get.assert_called_once()
        call_args = mock_get.call_args[1]
        assert call_args["url"] == target_url

def test_proxy_rejects_non_get_methods():
    """Test that only GET requests are allowed."""
    target_url = "https://example.com/api/data"
    
    for method in ["POST", "PUT", "DELETE", "PATCH"]:
        response = client.request(method, f"/proxy?url={target_url}")
        assert response.status_code == 405
        assert "Only GET requests are supported" in response.text

def test_proxy_missing_url_parameter():
    """Test error handling when URL parameter is missing."""
    response = client.get("/proxy")
    assert response.status_code == 422  # FastAPI validation error

def test_proxy_invalid_url_format():
    """Test error handling for invalid URLs."""
    invalid_urls = [
        "not-a-url",
        "ftp://example.com",  # Non-HTTP scheme
        "http://",  # Missing host
    ]
    
    for url in invalid_urls:
        response = client.get(f"/proxy?url={url}")
        assert response.status_code == 400
        assert "Invalid URL format" in response.text.lower()

def test_proxy_respects_10mb_limit():
    """Test that responses larger than 10MB are rejected."""
    target_url = "https://example.com/large-file"
    
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {
            "content-length": str(11 * 1024 * 1024)  # 11MB
        }
        mock_get.return_value = mock_response
        
        response = client.get(f"/proxy?url={target_url}")
        assert response.status_code == 413
        assert "too large" in response.text.lower()

def test_proxy_connection_error():
    """Test handling of connection errors to target server."""
    target_url = "https://nonexistent.example.com"
    
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_get.side_effect = httpx.RequestError("Connection failed")
        
        response = client.get(f"/proxy?url={target_url}")
        assert response.status_code == 502
        assert "error forwarding request" in response.text.lower()

def test_proxy_with_path():
    """Test proxying to URL with path components."""
    target_url = "https://example.com/api/v1/data/123"
    
    with patch('httpx.AsyncClient.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {}
        mock_response.aiter_bytes.return_value = [b""]
        mock_get.return_value = mock_response
        
        response = client.get(f"/proxy?url={target_url}")
        assert response.status_code == 200
        
        # Verify full path was preserved
        mock_get.assert_called_once()
        call_args = mock_get.call_args[1]
        assert call_args["url"] == target_url