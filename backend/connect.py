import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import sys
from typing import Dict, Any, Optional

from backend.config import config

class DatabaseConnection:
    """Database connection manager for PostgreSQL databases."""
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None, config_name: str = 'default'):
        """
        Initialize database connection with configuration.
        Args:
            db_config: Optional database configuration dictionary. If not provided,
                      will try to get configuration by name.
            config_name: Name of the configuration to use if db_config not provided.
        """
        self.db_config = db_config or config.get_database_config(config_name)
        if not self.db_config:
            raise ValueError(f"No database configuration found for name: {config_name}")
        
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        """Establish database connection."""
        try:
            logging.info(f"Connecting to PostgreSQL database: {self.db_config['database']}")
            self.connection = psycopg2.connect(
                **self.db_config,
                cursor_factory=RealDictCursor
            )
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(f"Error connecting to PostgreSQL database: {error}")
            raise

    def get_connection(self):
        """Get the database connection."""
        if not self.connection or self.connection.closed:
            self._connect()
        return self.connection

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            logging.info("Database connection closed")

def get_db_connection(config_name: str = 'default') -> DatabaseConnection:
    """
    Create a database connection using configuration parameters.
    Args:
        config_name: Name of the database configuration to use
    Returns:
        DatabaseConnection object
    """
    return DatabaseConnection(config_name=config_name)