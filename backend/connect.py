import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import sys

from backend.config import config

def get_db_connection():
    """
    Create a database connection using configuration parameters
    Returns:
        connection: psycopg2 connection object
    """
    try:
        params = config.database
        logging.info(f"Connecting to PostgreSQL database: {params['database']}")
        connection = psycopg2.connect(**params, cursor_factory=RealDictCursor)
        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error connecting to PostgreSQL database: {error}")
        sys.exit(1)