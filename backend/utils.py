# Standard library imports
import os
import json
from datetime import datetime

# Third-party imports
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# Local imports
from backend import config
from backend.ingestion import csv_source_handler, db_source_handler

# Load environment variables
load_dotenv()

# Function to resolve file paths correctly
def get_absolute_filepath(relative_path_to_target, script_path=os.path.dirname(__file__)):
    script_path = script_path
    relative_path = relative_path_to_target
    full_path = os.path.join(script_path, relative_path)
    
    return full_path

# Function to get test report configuration
def get_test_report_config():
    """Get test report configuration from environment variables"""
    return {
        'report_name': os.getenv('TEST_REPORT_NAME', 'TMM1_REPORT_'),
        'description': os.getenv('TEST_REPORT_DESCRIPTION', 'TMM1_REPORT_DESCRIPTION'),
        'model': os.getenv('TEST_REPORT_MODEL', 'TMM1'),
        'config_file_csv': os.getenv('TEST_CONFIG_FILE_CSV', 'test/test_data/test_data.json'),
        'config_file_db': os.getenv('TEST_CONFIG_FILE_DB', 'test/test_data/test_data.ini')
    }

# Function to process the file based on its extension
def file_type_handler(file_path, dataFilePath):
    # Get the file extension (lowercased for consistency)
    file_extension = os.path.splitext(file_path)[1].lower()
    print('File Extension is:', file_extension)

    if file_extension == '.json':
        return csv_source_handler.csv_handler(file_path, dataFilePath)
    elif file_extension == '.ini':
        print('Parsing Database Configuration file..')
        source_db_config = config.parser(file_path)
        return db_source_handler.db_handler(connection_params=source_db_config)
    elif file_extension == 3:
        return "Option 3 selected"
    else:
        return "Invalid option"

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
        
        print(f"Export completed successfully! Files are saved in: {export_folder}")

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

