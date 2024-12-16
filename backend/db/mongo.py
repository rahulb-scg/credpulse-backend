# Standard library imports
import datetime
import logging

# Third-party imports
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Local imports
from backend import config

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def get_mongo_client():
    """Get MongoDB client using configuration"""
    mongo_config = config.get_mongo_config()
    
    connection_string = f"mongodb://{mongo_config['host']}:{mongo_config['port']}/"
    if mongo_config.get('username') and mongo_config.get('password'):
        connection_string = f"mongodb://{mongo_config['username']}:{mongo_config['password']}@{mongo_config['host']}:{mongo_config['port']}/"

    logger.debug(f"MongoDB connection string: {connection_string}")
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
        report_data['type'] = "tmas"
        report_data['rejected_at'] = ""
        report_data['rejected_reason'] = ""
        report_data['processed_at'] = datetime.datetime.now(datetime.UTC)
        
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


def get_report(report_id):
    """Retrieve report data from MongoDB by ID"""
    client = None
    try:
        # Get MongoDB configuration
        mongo_config = config.get_mongo_config()
        
        # Get client
        client = get_mongo_client()
        
        # Get database and collection
        db = client[mongo_config['database']]
        collection = db[mongo_config['collection']]
        
        # Find the report by ID
        report = collection.find_one({"_id": ObjectId(report_id)})
        
        if report:
            # Convert ObjectId to string for JSON serialization
            report['_id'] = str(report['_id'])
            
            # Handle datetime fields only if they're datetime objects
            for field in ['created_at', 'processed_at']:
                if isinstance(report.get(field), datetime.datetime):
                    report[field] = report[field].isoformat()

            logger.info(f"Report retrieved successfully with ID: {report_id}")
            return report
        else:
            logger.warning(f"Report not found with ID: {report_id}")
            return None
            
    except Exception as e:
        logger.error(f"Failed to retrieve report from MongoDB: {str(e)}")
        raise
        
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")


def list_reports(page=1, page_size=20):
    """Retrieve paginated reports from MongoDB"""
    client = None
    try:
        # Get MongoDB configuration
        mongo_config = config.get_mongo_config()
        
        # Get client
        client = get_mongo_client()
        
        # Get database and collection
        db = client[mongo_config['database']]
        collection = db[mongo_config['collection']]
        
        # Calculate skip value for pagination
        skip = (page - 1) * page_size
        
        # Get total count of documents
        total_reports = collection.count_documents({})
        
        # Find the reports with pagination
        reports = list(collection.find(
            {},
            {
                'report_name': 1,
                'processed_at': -1,
                'type': 1,
                'status': 1,
                '_id': 1
            }
        ).sort('processed_at', -1).skip(skip).limit(page_size))
        
        # Convert ObjectId to string for JSON serialization
        for report in reports:
            report['_id'] = str(report['_id'])
            if isinstance(report.get('processed_at'), datetime.datetime):
                report['processed_at'] = report['processed_at'].isoformat()
        
        # Calculate pagination metadata
        total_pages = (total_reports + page_size - 1) // page_size
        has_next = page < total_pages
        has_prev = page > 1
        
        logger.info(f"Retrieved {len(reports)} reports for page {page}")
        return {
            'reports': reports,
            'pagination': {
                'total_reports': total_reports,
                'total_pages': total_pages,
                'current_page': page,
                'page_size': page_size,
                'has_next': has_next,
                'has_prev': has_prev
            }
        }
            
    except Exception as e:
        logger.error(f"Failed to retrieve reports from MongoDB: {str(e)}")
        raise
        
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")

