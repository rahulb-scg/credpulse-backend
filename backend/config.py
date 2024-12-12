from configparser import ConfigParser
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_credpulse_db_config():
    """Get database configuration from environment variables"""
    return {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'credpulse'),
        'user': os.getenv('DB_USER', 'credpulse'),
        'password': os.getenv('DB_PASSWORD', 'credpulse'),
        'engine': 'postgresql'
    }

def get_mongo_config():
    """Get MongoDB configuration from environment variables"""
    return {
        'host': os.getenv('MONGO_DB_HOST', 'localhost'),
        'port': os.getenv('MONGO_DB_PORT', '27017'),
        'user': os.getenv('MONGO_DB_USER', 'credpulse'),
        'password': os.getenv('MONGO_DB_PASSWORD', 'credpulse'),
        'database': os.getenv('MONGO_DB_NAME', 'credpulse'),
        'collection': os.getenv('MONGO_DB_COLLECTION', 'reports'),
        'auth_source': os.getenv('MONGO_DB_AUTH_SOURCE', 'admin')
    }