import os, json, config
import pandas as pd
from datetime import datetime
from ingestion import csv_source_handler, db_source_handler
import matplotlib.pyplot as plt

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

def export_output(data: dict, file_name_prefix='', file_name_suffix='', file_path='./'):
    """
    Exports a dictionary containing Pandas Series, DataFrames, and plots/images to JSON and image files.

    Parameters:
        data_dict (dict): The input dictionary containing Series, DataFrames, and plots.
        file_name_prefix (str): Optional prefix for filenames.
        file_name_suffix (str): Optional suffix for filenames.
        file_path (str): The path where the output folder will be created.

    Returns:
        None
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

                # Convert Series/DataFrame to JSON-compatible format
                json_export_data[key] = value.to_json()

            elif isinstance(value, plt.Figure):

                # Save the plot as an image
                image_file_name = f"{file_name_prefix}{key}{file_name_suffix}.png"
                image_file_path = os.path.join(export_folder, image_file_name)
                value.savefig(image_file_path)

                plt.close(value)  # Close the figure to free up memory

                value = image_file_path
            else:

                # Handle other types of data if necessary (e.g., strings, numbers)
                json_export_data[key] = value
        
        # Export the entire dictionary to a single JSON file
        json_file_name = f"{file_name_prefix}export{file_name_suffix}.json"
        json_file_path = os.path.join(export_folder, json_file_name)
        
        with open(json_file_path, 'w') as json_file:
            json.dump(json_export_data, json_file, indent=4)
        
        print(f"Export completed successfully! Files are saved in: {export_folder}")
    
    except Exception as e:
        print(f"An error occurred during export: {e}")
