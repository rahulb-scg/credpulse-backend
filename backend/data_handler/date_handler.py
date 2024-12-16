# Third-party imports
import pandas as pd

# Desired date format for output
DESIRED_DATE_FORMAT = "%Y-%m-%d"

# List of common date formats recognized by pandas
stock_date_formats = (
    # ISO formats
    'ISO8601',                  # e.g. '2023-12-31' or '2023-12-31T23:59:59'
    '%Y-%m-%d',                 # e.g. '2023-12-31'
    '%Y%m%d',                   # e.g. '20231231'
    
    # American formats
    '%m/%d/%Y',                 # e.g. '12/31/2023'
    '%m-%d-%Y',                 # e.g. '12-31-2023'
    '%m.%d.%Y',                 # e.g. '12.31.2023'
    '%b %d, %Y',               # e.g. 'Dec 31, 2023'
    '%B %d, %Y',               # e.g. 'December 31, 2023'
    
    # European formats
    '%d/%m/%Y',                 # e.g. '31/12/2023'
    '%d-%m-%Y',                 # e.g. '31-12-2023'
    '%d.%m.%Y',                 # e.g. '31.12.2023'
    '%d %b %Y',                # e.g. '31 Dec 2023'
    '%d %B %Y',                # e.g. '31 December 2023'
    
    # Year first formats
    '%Y/%m/%d',                 # e.g. '2023/12/31'
    '%Y.%m.%d',                 # e.g. '2023.12.31'
    
    # With time components
    '%Y-%m-%d %H:%M:%S',        # e.g. '2023-12-31 23:59:59'
    '%Y-%m-%d %H:%M:%S.%f',     # e.g. '2023-12-31 23:59:59.999999'
    '%Y-%m-%dT%H:%M:%S',        # e.g. '2023-12-31T23:59:59'
    '%Y-%m-%dT%H:%M:%S.%f',     # e.g. '2023-12-31T23:59:59.999999'
    
    # Two digit year formats
    '%y-%m-%d',                 # e.g. '23-12-31'
    '%d/%m/%y',                 # e.g. '31/12/23'
    '%m/%d/%y',                 # e.g. '12/31/23'
    
    # Month name formats
    '%b-%d-%Y',                # e.g. 'Dec-31-2023'
    '%B-%d-%Y',                # e.g. 'December-31-2023'
)

# Custom date format processors
custom_date_formats = {
    'XMYYYY': lambda x: pd.to_datetime(f"{x[-4:]}-{x[:-4].zfill(2)}-28", format='%Y-%m-%d'),
    'XDXMYYYY': lambda x: pd.to_datetime(f"{x[-4:]}-{x[-5:-4].zfill(2)}-{x[:-5].zfill(2) or '28'}", format='%Y-%m-%d'),
    'XMXDYYYY': lambda x: pd.to_datetime(f"{x[-4:]}-{x[:-5].zfill(2)}-{x[-5:-4].zfill(2) or '28'}", format='%Y-%m-%d'),
    'DDMMYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[2:4]}-{x[:2] or '28'}", format='%Y-%m-%d'),
    'MMDDYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[:2]}-{x[2:4] or '28'}", format='%Y-%m-%d'),
    'XDXMYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[-3:-2].zfill(2)}-{x[:-3].zfill(2) or '28'}", format='%Y-%m-%d'),
    'XMXDYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[:-3].zfill(2)}-{x[-3:-2].zfill(2) or '28'}", format='%Y-%m-%d')
}


def strip_separators(value, separators):
    """
    Strips specified separators from a value and returns only the numeric portion.
    
    Parameters:
    value: Input value that may contain separators
    separators (list): List of separator characters to remove
    
    Returns:
    str: Value with all separators removed
    """
    if pd.isna(value):
        return value
        
    value_str = str(value)
    for sep in separators:
        value_str = value_str.replace(sep, '')
    return value_str

def convert_date_columns(df, data_config):
    """
    Converts date columns in DataFrame based on configuration.
    
    Parameters:
    df (pd.DataFrame): The input DataFrame
    data_config (dict): Configuration dictionary containing date column information
    
    Returns:
    pd.DataFrame: DataFrame with converted date columns
    """
    # Get date columns configuration
    date_columns = data_config['configuration']['data_specific_functions']['date_columns']
    
    for col_name, col_config in date_columns.items():
        print(f"\nProcessing column: {col_name}")
        format_type = col_config.get('date_format')
        
        # Handle stock date formats
        if format_type in stock_date_formats:
            print(f"Using stock date format: {format_type}")
            try:
                df[col_name] = pd.to_datetime(df[col_name], format=format_type)
                df[col_name] = df[col_name].dt.strftime(DESIRED_DATE_FORMAT)
            except Exception as e:
                print(f"Error converting column {col_name}: {str(e)}")
                
        # Handle custom date formats
        elif format_type in custom_date_formats:
            print(f"Using custom date format: {format_type}")
            try:
                # Strip separators if specified
                if 'separator' in col_config:
                    df[col_name] = df[col_name].apply(
                        lambda x: strip_separators(x, col_config['separator'])
                    )
                
                # Apply custom format processor
                df[col_name] = df[col_name].apply(custom_date_formats[format_type])
                df[col_name] = df[col_name].dt.strftime(DESIRED_DATE_FORMAT)
            except Exception as e:
                print(f"Error converting column {col_name}: {str(e)}")
        
        else:
            print(f"Warning: Unrecognized date format '{format_type}' for column {col_name}")
    
        print(df[col_name].unique().tolist())

    return df

def test_date_handler():
    """
    Test function to verify date handling functionality.
    """
    # Create a sample DataFrame with different date formats, including missing days
    test_data = {
        'stock_date1': ['2023-12-31', '2023-01-15', '2023-06-30'],
        'stock_date2': ['12/31/2023', '01/15/2023', '06/30/2023'],
        'custom_date1': ['122023', '012023', '062023'],  # XMYYYY format (should default to 28th)
        'custom_date2': ['311223', '150123', '300623'],  # DDMMYY format
        'custom_date3': ['12023', '12023', '62023'],     # XMYYYY format with missing values
    }
    df = pd.DataFrame(test_data)

    # Create sample configuration
    test_config = {
        'configuration': {
            'data_specific_functions': {
                'date_columns': {
                    'stock_date1': {
                        'format': '%Y-%m-%d'
                    },
                    'stock_date2': {
                        'format': '%m/%d/%Y'
                    },
                    'custom_date1': {
                        'format': 'XMYYYY',
                        'separator': ['-', '/', '.']
                    },
                    'custom_date2': {
                        'format': 'DDMMYY',
                        'separator': ['-', '/', '.']
                    },
                    'custom_date3': {
                        'format': 'XMYYYY',
                        'separator': ['-', '/', '.']
                    }
                }
            }
        }
    }

    # Process the dates
    try:
        result_df = convert_date_columns(df, test_config)
        
        # Print results
        print("\nTest Results:")
        print("-" * 50)
        print("Original DataFrame:")
        print(df)
        print("\nConverted DataFrame:")
        print(result_df)
        
        # Verify results
        expected_dates = {
            'stock_date1': ['31 December 2023', '15 January 2023', '30 June 2023'],
            'stock_date2': ['31 December 2023', '15 January 2023', '30 June 2023'],
            'custom_date1': ['28 December 2023', '28 January 2023', '28 June 2023'],
            'custom_date2': ['31 December 2023', '15 January 2023', '30 June 2023'],
            'custom_date3': ['28 January 2023', '28 January 2023', '28 June 2023']
        }
        
        for col in result_df.columns:
            print(f"\nTesting column: {col}")
            for i, date in enumerate(result_df[col]):
                assert date == expected_dates[col][i], f"Mismatch in {col} row {i}: Expected {expected_dates[col][i]}, got {date}"
            print(f"✓ {col} passed all tests")
        
        print("\n✓ All tests passed successfully!")
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_date_handler()