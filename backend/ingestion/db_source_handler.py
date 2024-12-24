# Third-party imports
import oracledb
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import logging
import time  # Add this import at the top of the file

def db_handler(connection_params):
    """
    Connects to the client's database, executes a query, and fetches data as a DataFrame.
    
    Parameters:
    - connection_params: Dictionary of connection parameters (host, port, username, password, database_name, engine, etc.)
    
    Returns:
    - Data as a pandas DataFrame
    """
    # Define the engine format for each database type
    engine_format = {
        "postgresql": "postgresql://{user}:{password}@{host}:{port}/{database}",
        "mysql": "mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
        "oracle": "oracle+oracledb://{user}:{password}@{host}:{port}/{database}",
        "mssql": "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
        "sqlite": "sqlite:///{database}"  # SQLite does not require host/port/user/password
    }

    db_type = connection_params['engine']
    logging.debug(f"Database type detected: {db_type}")

    try:
        if db_type not in engine_format:
            logging.error(f"Unsupported database type: {db_type}")
            raise ValueError(f"Unsupported database type: {db_type}")

        # Format the engine string based on connection parameters
        engine_string = engine_format[db_type].format(
            user=connection_params.get('username', ''),
            password=connection_params.get('password', ''),
            host=connection_params.get('host', ''),
            port=connection_params.get('port', ''),
            database=connection_params.get('database_name', ''),
            ssl_mode=connection_params.get('ssl_mode', 'disable'),
            connect_timeout=connection_params.get('connect_timeout', 10)
        )
        logging.debug(f"Engine string created: {engine_string}")

        # Create the SQLAlchemy engine
        engine = create_engine(engine_string)
        logging.info("SQLAlchemy engine created successfully.")

        # Execute the query and fetch data into a DataFrame
        query = connection_params.get('query', f"SELECT * FROM {connection_params['table']}")
        logging.debug(f"Executing query: {query}")
        start_time = time.time()  # Start timing
        data = pd.read_sql(query, engine)
        duration = time.time() - start_time  # Calculate duration
        num_records = data.shape
        logging.info(f"Data fetched successfully into DataFrame in {duration:.2f} seconds.")
        logging.info(f"Number of records fetched: {num_records}")
        # Log the first few rows of data for debugging
        logging.debug(f"First few rows of fetched data:\n{data.head(5)}")
        return data

    except Exception as e:
        logging.error(f"An error occurred while fetching data from {db_type}: {e}")
        return None
