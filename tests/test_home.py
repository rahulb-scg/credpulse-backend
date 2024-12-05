import pytest, json
from backend.app import app

@pytest.fixture
def client():
    with app.test_client() as client:
        yield client

def test_home(client):
    # Test the homepage endpoint
    response = client.get('/')

    # Assertions
    data = json.loads(response.data)
    assert response.data == b'{"message":"Hello from the Python backend!"}\n'
