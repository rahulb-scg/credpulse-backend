import os
from datetime import datetime
import logging

# Local imports
from backend import connect, config
from backend.utils import get_absolute_filepath, file_type_handler, export_output, get_test_report_config
from backend.data_handler import preprocessor
from backend.ingestion import df_to_db
from backend.models import tmm1
from backend.db.mongo import save_report

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level for granular logging
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("backend/logs/app.log"),
        logging.StreamHandler()
    ]
)

def main(configFilePath = None, dataFilePath = None, config_type='db'):
    logging.info("Starting the main function.")
    
    # Get test configuration based on the specified type
    test_config = get_test_report_config(config_type)
    logging.info(f"Test configuration loaded: {test_config}")

    # Use provided config file path or default from test config
    if configFilePath is None:
        configFilePath = get_absolute_filepath(test_config['config_file'])
    
    logging.info(f"Using configuration file: {configFilePath}")

    # Check file for type of source, and import it into a dataframe
    df, data_config = file_type_handler(configFilePath, dataFilePath)
    if df is None:
        logging.error("DataFrame is None. Exiting main function.")
        return "Error"
    
    logging.info("DataFrame loaded successfully.")

    # Creating a connection with the credpulse database
    engine, conn = connect.connect()
    logging.info("CredPulse Database connection established.")

    # Saving the df to db
    # logging.info('Saving DataFrame to database...')
    # df_to_db.df_to_db(df, engine, tableName='test_table')

    # Data Preprocessing
    preprocessed_data = preprocessor.preprocess(df, data_config)
    logging.info("Data preprocessing completed.")

    # Running Model
    data = tmm1.run_model(preprocessed_data, data_config)
    logging.info("Model run completed.")

    # Save local files if needed
    output = export_output(
        data=data, 
        file_name_prefix=test_config['report_name'], 
        file_path=get_absolute_filepath('test/outputs')
    )
    logging.info("Output files saved successfully.")

    # Return both the MongoDB ID and the output data
    return {
        "data": output
    }

if __name__ == "__main__":
    main()

