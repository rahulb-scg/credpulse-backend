# Standard library imports
import os
import json
from datetime import datetime
import logging

# Third-party imports
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Local imports
from backend import config
from backend.ingestion import csv_source_handler, db_source_handler, df_to_db

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("backend/logs/app.log"),
        logging.StreamHandler()
    ]
)

# Function to resolve file paths correctly
def get_absolute_filepath(relative_path_to_target, script_path=os.path.dirname(__file__)):
    full_path = os.path.join(script_path, relative_path_to_target)
    logging.debug(f"Resolved absolute file path: {full_path}")
    return full_path

# Function to get test report configuration
def get_test_report_config(config_type='db'):
    """Get test report configuration from environment variables based on the type of configuration."""
    logging.debug(f"Getting test report configuration for type: {config_type}")
    if config_type == 'csv':
        config = {
            'report_name': os.getenv('TEST_REPORT_NAME', 'TMM1_REPORT_'),
            'description': os.getenv('TEST_REPORT_DESCRIPTION', 'TMM1_REPORT_DESCRIPTION'),
            'model': os.getenv('TEST_REPORT_MODEL', 'TMM1'),
            'config_file': os.getenv('TEST_CONFIG_FILE_CSV', 'test/test_data/test_data.json')
        }
    elif config_type == 'db':
        config = {
            'report_name': os.getenv('TEST_REPORT_NAME', 'TMM1_REPORT_'),
            'description': os.getenv('TEST_REPORT_DESCRIPTION', 'TMM1_REPORT_DESCRIPTION'),
            'model': os.getenv('TEST_REPORT_MODEL', 'TMM1'),
            'config_file': os.getenv('TEST_CONFIG_FILE_DB', 'test/test_data/test_db.json')
        }
    else:
        logging.error("Invalid config_type provided.")
        raise ValueError("Invalid config_type. Use 'csv' or 'db'.")
    
    logging.debug(f"Configuration loaded: {config}")
    return config

# Function to process the file based on its extension
def file_type_handler(configFilePath, dataFilePath=None):
    try:
        logging.debug(f"Opening configuration file: {configFilePath}")
        with open(configFilePath, 'r') as f:
            data_config = json.load(f)
        
        source_type = data_config['configuration']['source'].lower()
        logging.info(f"Source type detected: {source_type}")
        
        if source_type == 'csv':
            df, data_config = csv_source_handler.csv_handler(configFilePath, dataFilePath)
            logging.info("CSV data ingested successfully.")
            return df, data_config
        elif source_type == 'db':
            logging.info("Parsing Database Configuration file..")
            connection_params = data_config['configuration']['attributes']['connection_details']
            logging.debug(f"Connection parameters: {connection_params}")
            df = db_source_handler.db_handler(connection_params=connection_params)
            if df is not None:
                logging.info("Database data ingested successfully.")
            else:
                logging.error("Failed to ingest data from the database.")
            return df, data_config
        else:
            logging.error(f"Invalid source type '{source_type}' specified in configuration")
            return None, None
                
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {configFilePath}")
        return None, None
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in configuration file: {configFilePath}")
        return None, None
    except KeyError as e:
        logging.error(f"Missing key in configuration: {str(e)}")
        return None, None
    except Exception as e:
        logging.error(f"Unexpected error processing configuration: {str(e)}")
        return None, None

def data_source_handler(config_file, data_file_path, read_rows=None):
    """
    Handles data source based on the configuration file provided.

    Parameters:
    - config_file: Path to the configuration file (JSON format).
    - data_file_path: Path to the data file (CSV, Excel, etc.).
    - read_rows: Number of rows to read (optional).

    Returns:
    - DataFrame and data configuration.
    """
    data_config = {}
    df = None

    try:
        logging.debug(f"Loading configuration file: {config_file}")
        with open(config_file, 'r') as config:
            data_config = json.load(config)

        logging.info("Config file loaded successfully.")
        logging.debug(f"Data configuration: {data_config}")

        # Determine the source type from the configuration
        source_type = data_config.get('source_type').lower()
        logging.debug(f"Detected source type: {source_type}")

        if source_type == 'csv':
            df, data_config = csv_source_handler.csv_handler(config_file, data_file_path, read_rows)
        elif source_type == 'excel':
            df = pd.read_excel(data_file_path, nrows=read_rows)
            logging.info(f"Dataset imported successfully from Excel. Imported {read_rows if read_rows else 'all'} rows")
        elif source_type == 'db':
            connection_params = data_config['configuration']['attributes']['connection_details']
            df = db_source_handler.db_handler(connection_params=connection_params)
            logging.info("Dataset imported successfully from Database.")
        else:
            logging.error(f"Unsupported source type: {source_type}")
            return None, None

    except FileNotFoundError as e:
        logging.error(f"Error: The file {data_file_path} was not found.")
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing data file: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Error: Failed to parse the configuration file. Invalid JSON: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if df is not None:
            logging.debug(f"DataFrame shape: {df.shape}")
        else:
            logging.warning("DataFrame was not created due to an error.")
        logging.info("Finished data import process.")

    return df, data_config 

def export_output(data: dict, file_name_prefix='', file_name_suffix='', file_path='./', save_to_mongodb=True):
    """
    Exports a dictionary containing Pandas Series, DataFrames, and plots/images to JSON and image files.
    Optionally saves to MongoDB.

    Parameters:
        data_dict (dict): The input dictionary containing Series, DataFrames, and plots.
        file_name_prefix (str): Optional prefix for filenames.
        file_name_suffix (str): Optional suffix for filenames.
        file_path (str): The path where the output folder will be created.
        save_to_mongodb (bool): Whether to also save the data to MongoDB.

    Returns:
        dict: The exported data
    """
    # Create a timestamped folder for the export
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    export_folder = os.path.join(file_path, f'export_{timestamp}')

    try:
        os.makedirs(export_folder, exist_ok=True)
        logging.info(f"Export folder created: {export_folder}")
        
        # Initialize a dictionary to hold the JSON-compatible data
        json_export_data = {}

        for key, value in data.items():
            if isinstance(value, (pd.Series, pd.DataFrame)):
                # First convert to dictionary
                temp_dict = value.to_dict()
                
                # If it's a DataFrame, we need to handle nested dictionaries
                if isinstance(value, pd.DataFrame):
                    for col in temp_dict:
                        for idx in temp_dict[col]:
                            if pd.isna(temp_dict[col][idx]):
                                temp_dict[col][idx] = 0
                # If it's a Series, handle single level dictionary
                else:
                    for idx in temp_dict:
                        if pd.isna(temp_dict[idx]):
                            temp_dict[idx] = 0
                
                json_export_data[key] = temp_dict

            elif isinstance(value, plt.Figure):
                # Save the plot as an image
                image_file_name = f"{file_name_prefix}{key}{file_name_suffix}.png"
                image_file_path = os.path.join(export_folder, image_file_name)
                value.savefig(image_file_path)
                plt.close(value)  # Close the figure to free up memory
                value = image_file_path
            else:
                # Handle other types of data (e.g., strings, numbers)
                json_export_data[key] = value
        
        # Export to JSON file
        json_file_name = f"{file_name_prefix}export{file_name_suffix}.json"
        json_file_path = os.path.join(export_folder, json_file_name)
        
        with open(json_file_path, 'w') as json_file:
            json.dump(json_export_data, json_file, indent=4, default=str)
        
        logging.info(f"Export completed successfully! Files are saved in: {export_folder}")

        return json_export_data
    
    except Exception as e:
        print(f"An error occurred during export: {e}")
        return None

def export_to_mongodb(data: dict, collection_name: str = 'outputs') -> bool:
    """
    Exports data to a MongoDB collection.

    Parameters:
        data (dict): The data to be stored in MongoDB
        collection_name (str): Name of the collection to store the data (default: 'outputs')

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get MongoDB configuration from environment
        mongo_config = config.get_mongo_config()
        
        # Build connection string from config
        connection_string = f"mongodb://{mongo_config['host']}:{mongo_config['port']}/"
        if mongo_config.get('username') and mongo_config.get('password'):
            connection_string = f"mongodb://{mongo_config['username']}:{mongo_config['password']}@{mongo_config['host']}:{mongo_config['port']}/"

        # Connect to MongoDB using config
        client = MongoClient(connection_string)
        db = client[mongo_config['database']]  # Get database name from config
        collection = db[collection_name]

        # Add metadata to the document
        document = {
            'timestamp': datetime.now(),
            'data': data
        }

        # Insert the document
        result = collection.insert_one(document)
        
        print(f"Document inserted successfully with ID: {result.inserted_id}")
        client.close()
        return True

    except Exception as e:
        print(f"An error occurred while exporting to MongoDB: {e}")
        return False

