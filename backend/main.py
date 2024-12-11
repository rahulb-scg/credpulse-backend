import os
from backend import connect, config
from backend.utils import get_absolute_filepath, file_type_handler, export_output, get_test_report_config
from backend import data_handler
from backend.data_handler import preprocessor
from backend.ingestion import df_to_db
from backend.models import tmm1
from backend.db.mongo import save_report
from datetime import datetime

def main(configFilePath = None):
    # Get test configuration
    test_config = get_test_report_config()
    
    # Use provided config file path or default from test config
    if configFilePath is None:
        configFilePath = test_config['config_file_csv']

    # Greet user
    print("Welcome to CredPulse!\n")
    
    configFile = get_absolute_filepath(configFilePath)

    # Check file for type of source, and import it into a dataframe
    df, data_config = file_type_handler(configFile)
    if df is None:
        return "Error"
    
    # Creating a connection with the database
    engine, conn = connect.connect()

    # Saving the df to db
    print('Saving to database')
    # df_to_db.df_to_db(df, engine, tableName='test_table')

    # Data Preprocessing
    preprocessed_data = preprocessor.preprocess(df, data_config)

    # Running Model
    data = tmm1.run_model(preprocessed_data, data_config)

    # Create timestamp for this run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    

    # Save local files if needed
    output = export_output(
        data=data, 
        file_name_prefix=test_config['report_name'], 
        file_path=get_absolute_filepath('test/outputs')
    )

    # Prepare report data for MongoDB using test configuration
    report_data = {
        "report_name": f"{test_config['report_name']}_{timestamp}",
        "description": test_config['description'],
        "result": output,
        "config_file": configFile,
        "created_at": datetime.now(),
        "model": test_config['model']
    }

    # Save to MongoDB
    report_id = save_report(report_data)
    print(f"Report saved to MongoDB with ID: {report_id}")

    # Return both the MongoDB ID and the output data
    return {
        "report_id": report_id,
        "data": output
    }

if __name__ == "__main__":
    main()

