from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from bson.objectid import ObjectId

from backend.config import config

class MongoDBClient:
    """MongoDB client for handling report data."""
    
    def __init__(self):
        """Initialize MongoDB client with configuration."""
        self.mongo_config = config.mongodb
        self.client = self._get_client()
        self.db = self.client[self.mongo_config['database']]
        self.collection = self.db[self.mongo_config['collection']]

    def _get_client(self) -> MongoClient:
        """Create and return MongoDB client."""
        try:
            # Build connection string with proper URL encoding
            credentials = f"{self.mongo_config['user']}:{self.mongo_config['password']}"
            host = f"{self.mongo_config['host']}:{self.mongo_config['port']}"
            auth_source = self.mongo_config.get('auth_source', 'admin')
            
            # First try connecting without auth to check if auth is required
            try:
                no_auth_client = MongoClient(
                    f"mongodb://{host}/",
                    serverSelectionTimeoutMS=2000,
                    connectTimeoutMS=2000
                )
                no_auth_client.admin.command('ping')
                logging.info("Connected to MongoDB without authentication")
                return no_auth_client
            except Exception:
                logging.info("Attempting connection with authentication...")
            
            # Create the connection URI with authentication
            mongo_uri = f"mongodb://{credentials}@{host}/?authSource={auth_source}"
            
            # Create client with proper options
            client = MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=5000,
                retryWrites=True,
                w='majority'
            )
            
            # Test connection and authentication
            client.admin.command('ping')
            
            # Initialize database and collection
            db = client[self.mongo_config['database']]
            collection = db[self.mongo_config['collection']]
            
            # Verify we can access the collection
            collection.find_one()
            
            logging.info("Successfully connected to MongoDB")
            return client
            
        except ConnectionFailure as e:
            logging.error(f"Failed to connect to MongoDB: {str(e)}", exc_info=True)
            raise
        except OperationFailure as e:
            logging.error(f"MongoDB authentication failed: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Unexpected error connecting to MongoDB: {str(e)}", exc_info=True)
            raise

    def insert_report(self, report_data: Dict[str, Any]) -> str:
        """
        Insert a new report into MongoDB.
        Args:
            report_data: Dictionary containing report data
        Returns:
            str: ID of inserted document
        """
        try:
            if 'created_at' not in report_data:
                report_data['created_at'] = datetime.utcnow()
            result = self.collection.insert_one(report_data)
            logging.info(f"Successfully inserted report with ID: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            logging.error(f"Error inserting report: {e}")
            raise

    def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a report by its ID.
        Args:
            report_id: ID of the report to retrieve
        Returns:
            Optional[Dict]: Report data if found, None otherwise
        """
        try:
            report = self.collection.find_one({'_id': ObjectId(report_id)})
            if report:
                # Convert ObjectId to string for JSON serialization
                report['_id'] = str(report['_id'])
                # Handle datetime fields
                for field in ['created_at', 'processed_at']:
                    if isinstance(report.get(field), datetime):
                        report[field] = report[field].isoformat()
                logging.info(f"Successfully retrieved report: {report_id}")
            else:
                logging.info(f"No report found with ID: {report_id}")
            return report
        except Exception as e:
            logging.error(f"Error retrieving report: {e}")
            raise

    def list_reports(self, page: int = 1, page_size: int = 20) -> Dict[str, Any]:
        """
        List reports with pagination.
        Args:
            page: Page number (1-based)
            page_size: Number of items per page
        Returns:
            Dict containing reports and pagination info
        """
        try:
            # Calculate skip value for pagination
            skip = (page - 1) * page_size
            
            # Get total count of documents
            total_reports = self.collection.count_documents({})
            
            # Find the reports with pagination
            reports = list(self.collection.find(
                {},
                {
                    'report_name': 1,
                    'description': 1,
                    'type': 1,
                    'status': 1,
                    'created_at': 1,
                    'processed_at': 1,
                    'files.config_name': 1,
                    'files.data_name': 1,
                    'result.summary': 1,
                    '_id': 1
                }
            ).sort('created_at', -1).skip(skip).limit(page_size))
            
            # Convert ObjectId to string and format datetime
            for report in reports:
                report['_id'] = str(report['_id'])
                for field in ['created_at', 'processed_at']:
                    if isinstance(report.get(field), datetime):
                        report[field] = report[field].isoformat()
            
            # Calculate pagination metadata
            total_pages = (total_reports + page_size - 1) // page_size
            has_next = page < total_pages
            has_prev = page > 1
            
            logging.info(f"Retrieved {len(reports)} reports for page {page}")
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
            logging.error(f"Error listing reports: {e}")
            raise

    def close(self):
        """Close MongoDB connection."""
        if hasattr(self, 'client'):
            self.client.close()
            logging.info("MongoDB connection closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

# Function-based interface for backward compatibility
_mongo_client = None

def _get_mongo_client() -> MongoDBClient:
    """Get or create a MongoDB client instance."""
    global _mongo_client
    try:
        if _mongo_client is None:
            _mongo_client = MongoDBClient()
        return _mongo_client
    except Exception as e:
        logging.error(f"Failed to get MongoDB client: {str(e)}", exc_info=True)
        raise

def save_report(report_data: Dict[str, Any]) -> str:
    """Backward-compatible function to save a report."""
    client = _get_mongo_client()
    try:
        return client.insert_report(report_data)
    except Exception as e:
        logging.error(f"Error in save_report: {e}")
        raise

def get_report(report_id: str) -> Optional[Dict[str, Any]]:
    """Backward-compatible function to get a report."""
    client = _get_mongo_client()
    try:
        return client.get_report(report_id)
    except Exception as e:
        logging.error(f"Error in get_report: {e}")
        raise

def list_reports(page: int = 1, page_size: int = 20) -> Dict[str, Any]:
    """Backward-compatible function to list reports."""
    client = _get_mongo_client()
    try:
        return client.list_reports(page, page_size)
    except Exception as e:
        logging.error(f"Error in list_reports: {e}")
        raise

