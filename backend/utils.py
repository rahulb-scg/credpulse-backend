import os
import json
import pandas as pd
from datetime import datetime
import ingestion, config, connect


# Function to process the file based on its extension
def file_type_handler(file_path):
    # Get the file extension (lowercased for consistency)
    file_extension = os.path.splitext(file_path)[1].lower()

    if file_extension == '.json':
        return ingestion.csv_source_handler.csv_handler(file_path)
    elif file_extension == '.ini':
        source_db_config = config(file_path)
        return ingestion.db_source_handler.db_handler(source_db_config)
    elif file_extension == 3:
        return "Option 3 selected"
    else:
        return "Invalid option"

