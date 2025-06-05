from typing import Dict, Any, Optional
import httpx
from urllib.parse import urlparse
from fastapi import HTTPException
import logging
from logger_config import setup_logger

logger = setup_logger()

class ProxyService:
    def __init__(self, max_size: int = 10 * 1024 * 1024):
        self.max_size = max_size
        
    async def forward_request(self, url: str, headers: Dict[str, str]) -> Dict[str, Any]:
        """Forward a GET request and return response data."""
        parsed_url = urlparse(url)
        
        # Set up headers, excluding problematic ones
        forwarded_headers = {
            k: v for k, v in headers.items()
            if not k.lower() in ('content-length', 'content-type', 'host', 'connection')
        }
        # Set the correct Host header for the target
        forwarded_headers['Host'] = parsed_url.netloc
        
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.get(
                    url,
                    headers=forwarded_headers,
                    timeout=30.0
                )
                
                # Get content and size
                content = await response.aread()
                content_length = len(content)
                
                # Check size limit
                if content_length > self.max_size:
                    raise HTTPException(
                        status_code=413,
                        detail=f"Response too large (max {self.max_size} bytes)"
                    )
                
                # Clean response headers
                response_headers = dict(response.headers)
                response_headers.pop('content-encoding', None)
                response_headers.pop('transfer-encoding', None)
                response_headers['content-length'] = str(content_length)
                
                return {
                    'content': content,
                    'status_code': response.status_code,
                    'headers': response_headers,
                    'media_type': response.headers.get('content-type')
                }
                
        except httpx.RequestError as e:
            logger.error(f"Proxy request error for {url}: {str(e)}")
            raise HTTPException(status_code=502, detail=f"Error forwarding request: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected proxy error for {url}: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Proxy error: {str(e)}")

    def validate_url(self, url: str) -> str:
        """Validate the proxy target URL."""
        if not url:
            raise HTTPException(status_code=400, detail="URL parameter is required")
        
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError("Invalid URL format")
            if parsed.scheme not in ('http', 'https'):
                raise ValueError("Only HTTP(S) URLs are supported")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid URL format: {str(e)}")
        
        return url