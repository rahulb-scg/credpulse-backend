# Standard library imports
import json

# Third-party imports
import pandas as pd

# CSV to DataFrame
def csv_handler(config_file, dataFilePath, read_rows=None):
    df = None
    data_config = {}

    try:
        # Open and load the configuration JSON file
        with open(config_file, 'r') as config:
            data_config = json.load(config)

        # print("Config file loaded successfully..\n", data_config)
        print("Config file loaded successfully..\n")
        print("Reading CSV..")

        # Check if column names are provided in the configuration
        column_names = data_config['configuration']['attributes']['names']
        
        # If column names are provided, assume the CSV has no header
        if column_names != "None":
            df = pd.read_csv(filepath_or_buffer=dataFilePath, 
                             delimiter=data_config['configuration']['attributes']['delimiter'],
                             names=column_names,
                             header=None,
                             skipinitialspace=True,
                             nrows=read_rows)
        else:
            # If no column names are provided, infer them from the CSV file
            df = pd.read_csv(filepath_or_buffer=dataFilePath,
                             delimiter=data_config['configuration']['attributes']['delimiter'],
                             skipinitialspace=True,
                             nrows=read_rows)

        print(f"Dataset imported successfully. Imported {read_rows if read_rows else 'all'} rows")

    except FileNotFoundError as e:
        print(f"Error: The file {dataFilePath} was not found.")
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV file: {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse the configuration file. Invalid JSON: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        if df is not None:
            print("DataFrame shape:", df.shape)
        else:
            print("DataFrame was not created due to an error.")
        print("Finished CSV import process.")

    return df, data_config
