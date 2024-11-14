import os
import json
import pandas as pd
from datetime import datetime
import config
import ingestion.csv_source_handler
import ingestion.db_source_handler


# Function to process the file based on its extension
def file_type_handler(file_path):
    # Get the file extension (lowercased for consistency)
    file_extension = os.path.splitext(file_path)[1].lower()
    print('File Extension is:', file_extension)

    if file_extension == '.json':
        return ingestion.csv_source_handler.csv_handler(file_path)
    elif file_extension == '.ini':
        print('Parsing Database Configuration file..')
        source_db_config = config.parser(file_path)
        return ingestion.db_source_handler.db_handler(connection_params=source_db_config)
    elif file_extension == 3:
        return "Option 3 selected"
    else:
        return "Invalid option"

def export_dict_to_json(data, file_name='', file_path='./'):
    """
    Exports a dictionary to a JSON file. If the dictionary contains pandas DataFrames or Series,
    they will be converted to a format that can be written to JSON.

    Parameters:
    - data (dict): The dictionary to export
    - file_name (str): The name of the output JSON file (without extension)
    - file_path (str): The path to the folder where the JSON file will be saved
    """

    try:
        # Ensure the directory exists
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        # Helper function to convert DataFrames or Series to JSON-compatible formats
        def convert_value(value):
            if isinstance(value, pd.DataFrame):
                return value.to_dict(orient='records')  # Convert DataFrame to list of dicts
            elif isinstance(value, pd.Series):
                return value.to_dict()  # Convert Series to dict
            return value  # If it's not a DataFrame or Series, leave it unchanged

        # Apply conversion to each value in the dictionary
        converted_data = {key: convert_value(value) for key, value in data.items()}

        # Get the current date and time, formatted as YYYYMMDD_HHMMSS
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create the full path to the output JSON file
        file_name_with_timestamp = f"{file_name}_{timestamp}.json"
        file_full_path = os.path.join(file_path, file_name_with_timestamp)

        # Export the converted dictionary to a JSON file
        with open(file_full_path, 'w') as json_file:
            json.dump(converted_data, json_file, indent=4)

        print(f"Dictionary has been successfully exported to {file_full_path}")

    except Exception as e:
        # Handle any exceptions that occur during the process
        print(f"An error occurred while exporting the dictionary: {str(e)}")

    finally:
        # This block runs regardless of success or failure
        print("Export attempt has completed.")