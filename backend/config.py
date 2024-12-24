"""
Deprecated: This module is maintained for backward compatibility.
Please use the new config package instead:
    from backend.config import config
"""

import logging
from .config import config

def get_credpulse_db_config():
    """Get database configuration from environment variables"""
    logging.debug("Fetching database configuration.")
    return config.database

def get_mongo_config():
    """Get MongoDB configuration from environment variables"""
    logging.debug("Fetching MongoDB configuration.")
    return config.mongodb