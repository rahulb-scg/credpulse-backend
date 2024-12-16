# Standard library imports
import json
import logging
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Third-party imports
import pandas as pd

# Local imports

# CSV to DataFrame
def csv_handler(config_file, dataFilePath='test/test_data/test_data.csv', read_rows=None):
    df = None
    data_config = {}

    from backend.utils import get_absolute_filepath

    try:
        # Open and load the configuration JSON file
        logging.debug("Attempting to open configuration file: %s", config_file)
        with open(config_file, 'r') as config:
            data_config = json.load(config)

        logging.info("Config file loaded successfully.")
        logging.debug("Config data: %s", data_config)
        # If no data file path provided, get it from config
        if dataFilePath is None:
            dataFilePath = data_config['configuration']['attributes']['filepath']
            logging.debug("Using filepath from config: %s", dataFilePath)
        
        # Get absolute path
        
        dataFilePath = get_absolute_filepath(dataFilePath)
        # Check and fix path if needed
        if dataFilePath == "E:\\CredPulse_Backend\\backend\\test_data.csv":
            dataFilePath = "E:\\CredPulse_Backend\\backend\\test\\test_data\\test_data.csv"
        logging.debug("Resolved absolute filepath: %s", dataFilePath)
        logging.info("Reading CSV... %s", dataFilePath)

        # Check if column names are provided in the configuration
        column_names = data_config['configuration']['attributes']['names']
        
        # If column names are provided, assume the CSV has no header
        start_time = time.time()
        
        if column_names != "None":
            logging.debug("Column names provided: %s", column_names)
            df = pd.read_csv(filepath_or_buffer=dataFilePath, 
                             delimiter=data_config['configuration']['attributes']['delimiter'],
                             names=column_names,
                             header=None,
                             skipinitialspace=True,
                             nrows=read_rows)
        else:
            logging.debug("No column names provided, inferring from CSV.")
            df = pd.read_csv(filepath_or_buffer=dataFilePath,
                             delimiter=data_config['configuration']['attributes']['delimiter'],
                             skipinitialspace=True,
                             nrows=read_rows)
        
        processing_time = time.time() - start_time
        logging.info(f"CSV processing took {processing_time:.2f} seconds")

        logging.info("Dataset imported successfully. Imported %s rows", read_rows if read_rows else 'all')

    except FileNotFoundError as e:
        logging.error("Error: The file %s was not found.", dataFilePath)
    except pd.errors.ParserError as e:
        logging.error("Error parsing CSV file: %s", e)
    except json.JSONDecodeError as e:
        logging.error("Error: Failed to parse the configuration file. Invalid JSON: %s", e)
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
    finally:
        if df is not None:
            logging.debug("DataFrame shape: %s", df.shape)
        else:
            logging.warning("DataFrame was not created due to an error.")
        logging.info("Finished CSV import process.")

    return df, data_config
