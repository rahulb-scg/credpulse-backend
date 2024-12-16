import os
import pytest
from backend.app import app

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

@pytest.fixture
def upload_test_file(client):
    # Ensure the uploads directory exists
    upload_dir = './uploads'
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)

    # Create a file to upload
    file_path = os.path.join(upload_dir, 'test_file.json')
    
    with open(file_path, 'w') as f:
        f.write('{"test": "data"}')

    # Simulate the file upload
    with open(file_path, 'rb') as f:
        response = client.post('/upload', data={'files': f})
    
    # Ensure the upload was successful (status code 201)
    assert response.status_code == 201
    assert b'Files uploaded successfully' in response.data

    yield file_path  # Return the path to the uploaded file for testing download

    # Clean up after the test
    os.remove(file_path)


def test_download_valid_file(client, upload_test_file):
    filename = 'test_db.json'
    response = client.get(f'/download/{filename}')
    
    # Check if the file is being downloaded successfully
    assert response.status_code == 200
    assert b'{"test": "data"}' in response.data

def test_download_nonexistent_file(client):
    # Attempt to download a nonexistent file
    response = client.get('/download/nonexistent_file.json')

    # Assertions
    assert response.status_code == 404
    assert b'File not found' in response.data
