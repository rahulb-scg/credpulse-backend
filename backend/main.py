import connect, config, os
from utils import file_type_handler
from ingestion import df_to_db

def main():
    # Greet user
    print("Welcome to CredPulse!\n")
    
    # Ask for data
    print("Enter the path to data configuration file", end='\n')
    print("(Leave blank for default)")
    # configFile = input("File Path: ")
    configFile = 'test/test_data/test_data.json'
    
    # Check file for type of source, and import it into a dataframe
    df = file_type_handler(configFile)
    
    engine, conn = connect.connect()

    print('Saving to database')

    df_to_db.df_to_db(df, engine, tableName='test_table')

if __name__ == "__main__":
    main()
