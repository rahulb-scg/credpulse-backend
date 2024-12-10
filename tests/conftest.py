import os
import shutil
import pytest

@pytest.fixture(scope='function', autouse=True)
def setup_upload_folder():
    """Fixture to clean up and prepare the uploads folder before and after each test."""
    upload_folder = './uploads'

    # Set up: clean and prepare the uploads folder before each test
    if os.path.exists(upload_folder):
        shutil.rmtree(upload_folder)
    os.makedirs(upload_folder)

    yield

    # Tear down: clean up after the test
    if os.path.exists(upload_folder):
        shutil.rmtree(upload_folder)
