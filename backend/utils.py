import os
from ingestion import csv_source_handler
# from ingestion.db_source_handler
# from ingestion.df_to_db import df_to_db


# Function to process the file based on its extension
def file_type_handler(file_path):
    # Get the file extension (lowercased for consistency)
    file_extension = os.path.splitext(file_path)[1].lower()
    

    if file_extension == '.json':
        return csv_source_handler.csv_handler(file_path)
    elif file_extension == '.ini':
        return ".ini"
    elif file_extension == 3:
        return "Option 3 selected"
    else:
        return "Invalid option"
