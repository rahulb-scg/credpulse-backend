from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from backend import config
import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongo_client():
    """Get MongoDB client using configuration"""
    mongo_config = config.get_mongo_config()
    
    connection_string = f"mongodb://{mongo_config['host']}:{mongo_config['port']}/"
    if mongo_config.get('username') and mongo_config.get('password'):
        connection_string = f"mongodb://{mongo_config['username']}:{mongo_config['password']}@{mongo_config['host']}:{mongo_config['port']}/"

    # Replace with your actual connection string
    client = MongoClient(connection_string)
        
    return client


def save_report(report_data):
    """Save report data to MongoDB"""
    client = None
    try:
        # Get MongoDB configuration
        mongo_config = config.get_mongo_config()
        
        # Get client
        client = get_mongo_client()
        
        # Get database and collection
        db = client[mongo_config['database']]
        collection = db[mongo_config['collection']]
        
        # Add metadata
        report_data['created_at'] = datetime.datetime.now(datetime.UTC)
        
        # Insert document
        result = collection.insert_one(report_data)
        
        logger.info(f"Report saved successfully with ID: {result.inserted_id}")
        return str(result.inserted_id)
        
    except Exception as e:
        logger.error(f"Failed to save report to MongoDB: {str(e)}")
        raise
    
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")
