# HTTP File Server

A FastAPI-based HTTP server for storing and managing blobs with headers.

## Features
- Upload, download, and delete blobs
- Custom headers support
- Disk quota management
- Content type inference
- Configurable size limits

## Installation

1. Clone the repository:
```bash
git clone https://github.com/MaceTenth/rebase_course.git
cd rebase_course/http_file_server
```

2. Create and activate a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The server can be configured through `config.py`. Default settings include:
- Maximum blob size: 10MB
- Maximum disk quota: 1GB
- Maximum header length: 100 characters
- Maximum number of headers: 20

## Running the Server

Start the server using uvicorn:
```bash
uvicorn main:app --reload
```

The server will start on `http://localhost:8000`

## Running Tests

The project uses pytest for testing. To run the tests:

```bash
pytest tests/test_server.py -v
```

For more detailed test output:
```bash
pytest tests/test_server.py -vv
```

## API Endpoints

### Upload a Blob
```
POST /blobs/{blob_id}
```

### Download a Blob
```
GET /blobs/{blob_id}
```

### Delete a Blob
```
DELETE /blobs/{blob_id}
```

## Project Structure
```
http_file_server/
├── config.py           # Server configuration
├── main.py            # FastAPI application
├── requirements.txt    # Project dependencies
├── app/               # Application modules
│   ├── api/          # API endpoints
│   ├── core/         # Core functionality
│   ├── models/       # Data models
│   └── services/     # Business logic
└── tests/            # Test suite
    └── test_server.py
```