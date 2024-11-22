import connect, config, os
from utils import get_absolute_filepath, file_type_handler, export_output
import data_handler
from ingestion import df_to_db
from models import tmm1
import data_handler.preprocessor

def main():
    # Greet user
    print("Welcome to CredPulse!\n")
    
    # Asking user to input path to configration file
    print("Enter the path to data configuration file", '(Leave blank for default)',sep='\n', end='\n')
    # configFile = input("File Path: ")
    configFile = get_absolute_filepath('test/test_data/test_data.json')    # Defaulting to test file to save time
    # configFile = 'test/test_data/test_db.ini'    # Defaulting to test file to save time

    # Check file for type of source, and import it into a dataframe
    df, data_config = file_type_handler(configFile)
    if df is None:
        return "Error"
    
    # Creating a connection with the database
    engine, conn = connect.connect()

    # Saving the df to db
    print('Saving to database')
    df_to_db.df_to_db(df, engine, tableName='test_table')

    # Data Preprocessing
    preprocessed_data = data_handler.preprocessor.preprocess(df, data_config)

    # Running Model
    data = tmm1.run_model(preprocessed_data)

    # Saving output to a json
    export_output(data=data, file_name_prefix='tmm1', file_path=get_absolute_filepath('test/outputs'))

if __name__ == "__main__":
    main()

