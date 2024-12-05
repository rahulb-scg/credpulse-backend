import pytest
from backend.app import app

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_api_data(client):
    # Test the /api/data endpoint
    response = client.get('/api/data')

    # Assertions
    assert response.status_code == 200
    assert b'{"message":"Hello from the Python backend!"}\n' in response.data
