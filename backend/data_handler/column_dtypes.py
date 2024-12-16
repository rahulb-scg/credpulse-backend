# Third-party imports
import pandas as pd

# Local imports
from backend.data_handler.date_handler import convert_date_columns

def convert_columns_dtype(df, data_config):
    """
    Converts DataFrame columns to specified data types based on configuration.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    data_config (dict): Configuration dictionary containing date column information
                       and data type mapping.

    Returns:
    pd.DataFrame: A DataFrame with converted column data types.
    """
    # Get dtype mapping from config
    dtype_map = data_config['configuration']['attributes']['dtype']

    print("Handling dates...")
    df_dates_converted = convert_date_columns(df, data_config)

    temp = pd.DataFrame(None)
    
    try:
        # Try to convert the whole DataFrame at once
        df_dates_converted = df_dates_converted.astype(dtype_map)
    except Exception as e:
        print(f"Error during DataFrame conversion: {e}")
        print("Attempting to identify problematic columns...")
        
        # If error occurs, try per-column conversion
        for column, dtype in dtype_map.items():
            try:
                df_dates_converted[column] = df_dates_converted[column].astype(dtype)
            except Exception as column_error:
                print(f"Error in column '{column}': {column_error}")

    temp = df_dates_converted
    
    return temp