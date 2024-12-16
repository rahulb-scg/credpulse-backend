import pandas as pd
import logging
import json
import argparse
from sqlalchemy import create_engine
import time

# Hardcoded connection parameters
DB_USERNAME = 'rahul'
DB_PASSWORD = '00000'
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'credpulse_test'

# Default configuration values
DEFAULT_CSV_PATH = 'test_data.csv'
DEFAULT_DB_CONNECTION = f'postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
DEFAULT_TABLE_NAME = 'test_data'
DEFAULT_IF_EXISTS = 'replace'
DEFAULT_CHUNK_SIZE = 1000

def csv_to_db(csv_file_path, db_connection_string, table_name, if_exists='replace', chunk_size=1000):
    """
    Reads a CSV file and saves it to a database table.
    
    Parameters:
        csv_file_path (str): Path to the CSV file
        db_connection_string (str): SQLAlchemy database connection string 
        table_name (str): Name of the table to create/update
        if_exists (str): How to behave if table exists ('fail', 'replace', 'append')
        chunk_size (int): Number of rows to write at a time
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logging.info(f"Reading CSV file: {csv_file_path}")
        start_time = time.time()
        df = pd.read_csv(csv_file_path)
        processing_time = time.time() - start_time
        logging.info(f"CSV processing took {processing_time:.2f} seconds")
        
        logging.info(f"Creating database engine with connection string: {db_connection_string}")
        engine = create_engine(db_connection_string)
        
        logging.info(f"Writing {len(df)} rows to table '{table_name}'")
        start_time = time.time()
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunk_size
        )
        processing_time = time.time() - start_time
        logging.info(f"Database write took {processing_time:.2f} seconds")
        
        logging.info("Successfully wrote data to database")
        return True
        
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_file_path}")
        return False
    except pd.errors.EmptyDataError:
        logging.error("CSV file is empty")
        return False
    except Exception as e:
        logging.error(f"Error writing to database: {str(e)}")
        return False

def load_config(config_file_path):
    """Load configuration from a JSON file."""
    try:
        with open(config_file_path) as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in config file: {config_file_path}")
        return None
    except FileNotFoundError:
        logging.error(f"Config file not found: {config_file_path}")
        return None

def get_user_input():
    """Prompt the user for input to set custom values."""
    print("\nWould you like to customize any default values? (Enter 'y' for yes, any other key to use defaults)")
    customize = input().lower() == 'y'
    
    preferences = {
        'csv_file_path': DEFAULT_CSV_PATH,
        'db_connection_string': DEFAULT_DB_CONNECTION,
        'table_name': DEFAULT_TABLE_NAME,
        'if_exists': DEFAULT_IF_EXISTS,
        'chunk_size': DEFAULT_CHUNK_SIZE
    }
    
    if customize:
        print("\nPress Enter to keep the default value, or enter a new value:")
        
        csv_path = input(f"CSV file path [{DEFAULT_CSV_PATH}]: ")
        preferences['csv_file_path'] = csv_path if csv_path else DEFAULT_CSV_PATH
        
        db_conn = input(f"Database connection string [{DEFAULT_DB_CONNECTION}]: ")
        preferences['db_connection_string'] = db_conn if db_conn else DEFAULT_DB_CONNECTION
        
        table = input(f"Table name [{DEFAULT_TABLE_NAME}]: ")
        preferences['table_name'] = table if table else DEFAULT_TABLE_NAME
        
        if_exists = input(f"If table exists action (fail/replace/append) [{DEFAULT_IF_EXISTS}]: ")
        preferences['if_exists'] = if_exists if if_exists else DEFAULT_IF_EXISTS
        
        try:
            chunk_size = input(f"Chunk size [{DEFAULT_CHUNK_SIZE}]: ")
            preferences['chunk_size'] = int(chunk_size) if chunk_size else DEFAULT_CHUNK_SIZE
        except ValueError:
            logging.warning("Invalid chunk size entered, using default")
            preferences['chunk_size'] = DEFAULT_CHUNK_SIZE
            
    return preferences

def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='Load CSV data into a database.')
    parser.add_argument('--config_file', type=str, help='Path to JSON config file')
    parser.add_argument('--csv_file_path', type=str, help='Path to the CSV file')
    parser.add_argument('--db_connection_string', type=str, help='SQLAlchemy database connection string')
    parser.add_argument('--table_name', type=str, help='Name of the table to create/update')
    parser.add_argument('--if_exists', type=str, default='replace', help='How to behave if table exists (default: replace)')
    parser.add_argument('--chunk_size', type=int, default=1000, help='Number of rows to write at a time (default: 1000)')

    args = parser.parse_args()

    if args.config_file:
        config = load_config(args.config_file)
        if config:
            csv_file_path = config.get('csv_file_path')
            db_connection_string = config.get('db_connection_string')
            table_name = config.get('table_name')
            if_exists = config.get('if_exists', 'replace')
            chunk_size = config.get('chunk_size', 1000)
        else:
            logging.error("Failed to load configuration from file.")
            return  # Exit if config loading failed
    else:
        # If no command-line arguments are provided, prompt for user input
        preferences = get_user_input()
        csv_file_path = preferences['csv_file_path']
        db_connection_string = preferences['db_connection_string']
        table_name = preferences['table_name']
        if_exists = preferences['if_exists']
        chunk_size = preferences['chunk_size']

    # Check for required parameters
    if not all([csv_file_path, db_connection_string, table_name]):
        logging.error("Missing required parameters. Please provide csv_file_path, db_connection_string, and table_name.")
        return

    csv_to_db(csv_file_path, db_connection_string, table_name, if_exists, chunk_size)

if __name__ == "__main__":
    main()
