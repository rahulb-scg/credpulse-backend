import os, json
from backend import config
import pandas as pd
from datetime import datetime
from backend.ingestion import csv_source_handler, db_source_handler
import matplotlib.pyplot as plt

# Function to resolve file paths correctly
def get_absolute_filepath(relative_path_to_target, script_path=os.path.dirname(__file__)):
    script_path = script_path
    relative_path = relative_path_to_target
    full_path = os.path.join(script_path, relative_path)
    
    return full_path


# Function to process the file based on its extension
def file_type_handler(file_path):
    # Get the file extension (lowercased for consistency)
    file_extension = os.path.splitext(file_path)[1].lower()
    print('File Extension is:', file_extension)

    if file_extension == '.json':
        return csv_source_handler.csv_handler(file_path)
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
                # Convert Series/DataFrame to a dictionary format instead of JSON string
                json_export_data[key] = value.to_dict()

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

        # Save to MongoDB if requested
        if save_to_mongodb:
            export_to_mongodb(json_export_data)

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
        from pymongo import MongoClient
        from datetime import datetime

        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['credpulse']  # Using the same database name as our PostgreSQL for consistency
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

