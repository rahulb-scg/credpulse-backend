# Third-party imports
import pandas as pd

def convert_columns_dtype(df, dtype_map):
    """
    Converts DataFrame columns to specified data types based on a dictionary map.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    dtype_map (dict): A dictionary where the keys are column names, 
                      and the values are the data types to convert the columns to.

    Returns:
    pd.DataFrame: A DataFrame with converted column data types.
    """

    # Custom function to convert unusual date formats (including 5 and 6 digits)
    def convert_unusual_date(date_value):
        # Handle missing or NaN values
        if pd.isna(date_value):
            return pd.NaT
        try:
            # Convert any non-string values (like int or float) to string
            date_value = str(date_value)
            month = date_value[:-4]   # Extract single-digit month
            year = date_value[-4:]   # Extract year
            return pd.Timestamp(f"{year}-{month.zfill(2)}-28")  # Return as YYYY-MM-DD (with leading zero for month)

        except:
            # Return NaT (Not a Time) for invalid formats
            return pd.NaT

    # Combined function to check and convert columns based on multiple keywords
    def convert_date_columns(df):
        # Define keywords to search for in column names
        date_columns = ['ORIG_DATE', 'ACT_PERIOD']
        # Check unique values in each date column before conversion
        for col in date_columns:
            unique_values = df[col].unique()
            print(f"Unique values in '{col}' before conversion:\n{unique_values}\n")

        # Applying Conversion
        df[date_columns] = df[date_columns].apply(lambda col: col.apply(convert_unusual_date))

        # Check unique values in each column after conversion
        for col in date_columns:
            unique_values = df[col].unique()
            print(f"Unique values in '{col}' after conversion:\n{unique_values}\n")

        return df

    print("Handling dates...")
    df_dates_converted = convert_date_columns(df)

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