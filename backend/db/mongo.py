from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

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
            mongo_uri = f"mongodb://{self.mongo_config['user']}:{self.mongo_config['password']}@{self.mongo_config['host']}:{self.mongo_config['port']}/?authSource={self.mongo_config['auth_source']}"
            client = MongoClient(mongo_uri)
            # Verify connection
            client.admin.command('ping')
            logging.info("Successfully connected to MongoDB")
            return client
        except ConnectionFailure as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            raise
        except OperationFailure as e:
            logging.error(f"Authentication failed: {e}")
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
            report = self.collection.find_one({'_id': report_id})
            if report:
                logging.info(f"Successfully retrieved report: {report_id}")
            else:
                logging.info(f"No report found with ID: {report_id}")
            return report
        except Exception as e:
            logging.error(f"Error retrieving report: {e}")
            raise

    def get_all_reports(self) -> List[Dict[str, Any]]:
        """
        Retrieve all reports.
        Returns:
            List[Dict]: List of all reports
        """
        try:
            reports = list(self.collection.find())
            logging.info(f"Successfully retrieved {len(reports)} reports")
            return reports
        except Exception as e:
            logging.error(f"Error retrieving reports: {e}")
            raise

    def close(self):
        """Close MongoDB connection."""
        if hasattr(self, 'client'):
            self.client.close()
            logging.info("MongoDB connection closed")

