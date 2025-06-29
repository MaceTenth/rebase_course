# logging_config.py
import logging
import json
from datetime import datetime
from logzio.handler import LogzioHandler
import os
import socket
import platform

# Logz.io configuration - EU Region
LOGZIO_TOKEN = os.getenv('LOGZIO_TOKEN', 'mFJpupXOLGfnACdjyGZlomwDdpxVfUFI')
LOGZIO_URL = os.getenv('LOGZIO_URL', 'https://listener-eu.logz.io:8071')
APP_ENV = os.getenv('APP_ENV', 'development')

class StructuredMessage:
    def __init__(self, message, **kwargs):
        self.message = message
        self.kwargs = kwargs

    def __str__(self):
        return '%s' % (self.message)

class StructuredLogzioFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.hostname = socket.gethostname()

    def format(self, record):
        # Get the original message
        if isinstance(record.msg, StructuredMessage):
            message = record.msg.message
            extra = record.msg.kwargs
        else:
            message = record.getMessage()
            extra = {}

        # Create the log structure
        log_data = {
            'message': f"[python-fastapi-app] {message}",
            'level': record.levelname,
            'timestamp': datetime.utcnow().isoformat(),
            'logger': record.name,
            'environment': APP_ENV,
            'application': 'users-microservice',
            'hostname': self.hostname,
            'platform': platform.platform(),
            'python_version': platform.python_version(),
            'function': record.funcName,
            'line_number': record.lineno,
            'filename': record.filename,
        }

        # Add any extra fields from the StructuredMessage
        log_data.update(extra)

        return json.dumps(log_data)

def setup_logging():
    # Create and configure the Logz.io handler
    logzio_handler = LogzioHandler(
        token=LOGZIO_TOKEN,
        url=LOGZIO_URL,
        logs_drain_timeout=5,
        debug=True,
        network_timeout=10.0
    )
    
    # Set the formatter
    logzio_handler.setFormatter(StructuredLogzioFormatter())

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    root_logger.addHandler(logzio_handler)

    # Create a logger specific to the users microservice
    logger = logging.getLogger("users_microservice")
    logger.setLevel(logging.INFO)
    
    return logger

# Initialize the logger
logger = setup_logging()

# Helper function to create structured logs
def structured_log(message, **kwargs):
    return StructuredMessage(message, **kwargs)
