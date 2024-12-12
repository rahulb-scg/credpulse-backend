# Standard library imports
import os
from io import BytesIO

# Third-party imports
import pytest

# Local imports
from backend.app import app

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_upload_valid_file(client):
    # Create a valid test file
    data = {
        'files': (BytesIO(b'{"key": "value"}'), 'test_file.json')
    }

    # Upload the file
    response = client.post('/upload', data=data)

    # Assertions
    assert response.status_code == 201
    assert b'Files uploaded successfully' in response.data

def test_upload_invalid_file_type(client):
    # Create an invalid file type (e.g., .txt)
    data = {
        'files': (BytesIO(b'This is a test document.'), 'test_document.txt')
    }

    response = client.post('/upload', data=data)
    
    # Assertions
    assert response.status_code == 400
    assert b'File type not allowed' in response.data
