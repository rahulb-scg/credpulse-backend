import pandas as pd
import logging
from typing import Dict, Any, Optional, Callable

# Desired date format for output
DESIRED_DATE_FORMAT = "%Y-%m-%d"

# List of common date formats recognized by pandas
STOCK_DATE_FORMATS = [
    'ISO8601',                  # e.g. '2023-12-31' or '2023-12-31T23:59:59'
    '%Y-%m-%d',                 # e.g. '2023-12-31'
    '%Y%m%d',                   # e.g. '20231231'
    '%m/%d/%Y',                 # e.g. '12/31/2023'
    '%m-%d-%Y',                 # e.g. '12-31-2023'
    '%m.%d.%Y',                 # e.g. '12.31.2023'
    '%b %d, %Y',               # e.g. 'Dec 31, 2023'
    '%B %d, %Y',               # e.g. 'December 31, 2023'
    '%d/%m/%Y',                 # e.g. '31/12/2023'
    '%d-%m-%Y',                 # e.g. '31-12-2023'
    '%d.%m.%Y',                 # e.g. '31.12.2023'
    '%d %b %Y',                # e.g. '31 Dec 2023'
    '%d %B %Y',                # e.g. '31 December 2023'
    '%Y/%m/%d',                 # e.g. '2023/12/31'
    '%Y.%m.%d',                 # e.g. '2023.12.31'
    '%Y-%m-%d %H:%M:%S',        # e.g. '2023-12-31 23:59:59'
    '%Y-%m-%d %H:%M:%S.%f',     # e.g. '2023-12-31 23:59:59.999999'
    '%Y-%m-%dT%H:%M:%S',        # e.g. '2023-12-31T23:59:59'
    '%Y-%m-%dT%H:%M:%S.%f',     # e.g. '2023-12-31T23:59:59.999999'
    '%y-%m-%d',                 # e.g. '23-12-31'
    '%d/%m/%y',                 # e.g. '31/12/23'
    '%m/%d/%y',                 # e.g. '12/31/23'
    '%b-%d-%Y',                # e.g. 'Dec-31-2023'
    '%B-%d-%Y',                # e.g. 'December-31-2023'
]

# Custom date format processors
CUSTOM_DATE_FORMATS: Dict[str, Callable] = {
    'XMYYYY': lambda x: pd.to_datetime(f"{x[-4:]}-{x[:-4].zfill(2)}-28", format='%Y-%m-%d'),
    'XDXMYYYY': lambda x: pd.to_datetime(f"{x[-4:]}-{x[-5:-4].zfill(2)}-{x[:-5].zfill(2) or '28'}", format='%Y-%m-%d'),
    'XMXDYYYY': lambda x: pd.to_datetime(f"{x[-4:]}-{x[:-5].zfill(2)}-{x[-5:-4].zfill(2) or '28'}", format='%Y-%m-%d'),
    'DDMMYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[2:4]}-{x[:2] or '28'}", format='%Y-%m-%d'),
    'MMDDYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[:2]}-{x[2:4] or '28'}", format='%Y-%m-%d'),
    'XDXMYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[-3:-2].zfill(2)}-{x[:-3].zfill(2) or '28'}", format='%Y-%m-%d'),
    'XMXDYY': lambda x: pd.to_datetime(f"20{x[-2:]}-{x[:-3].zfill(2)}-{x[-3:-2].zfill(2) or '28'}", format='%Y-%m-%d')
}

def strip_separators(value: Any, separators: list) -> Optional[str]:
    """
    Strip specified separators from a value.

    Args:
        value: Input value that may contain separators
        separators: List of separator characters to remove

    Returns:
        Value with all separators removed, or None if input is NA
    """
    if pd.isna(value):
        return value
        
    value_str = str(value)
    for sep in separators:
        value_str = value_str.replace(sep, '')
    return value_str

def convert_date_columns(df: pd.DataFrame, data_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert date columns in DataFrame based on configuration.

    Args:
        df: The input DataFrame
        data_config: Configuration dictionary containing date column information

    Returns:
        DataFrame with converted date columns
    """
    try:
        # Get date columns configuration
        date_columns = data_config['configuration']['data_specific_functions']['date_columns']
        
        for col_name, col_config in date_columns.items():
            logging.info(f"Processing date column: {col_name}")
            format_type = col_config.get('date_format')
            
            try:
                # Handle stock date formats
                if format_type in STOCK_DATE_FORMATS:
                    logging.info(f"Using stock date format: {format_type}")
                    df[col_name] = pd.to_datetime(df[col_name], format=format_type)
                    df[col_name] = df[col_name].dt.strftime(DESIRED_DATE_FORMAT)
                    
                # Handle custom date formats
                elif format_type in CUSTOM_DATE_FORMATS:
                    logging.info(f"Using custom date format: {format_type}")
                    
                    # Strip separators if specified
                    if 'separator' in col_config:
                        df[col_name] = df[col_name].apply(
                            lambda x: strip_separators(x, col_config['separator'])
                        )
                    
                    # Apply custom format processor
                    df[col_name] = df[col_name].apply(CUSTOM_DATE_FORMATS[format_type])
                    df[col_name] = df[col_name].dt.strftime(DESIRED_DATE_FORMAT)
                
                else:
                    logging.warning(f"Unrecognized date format '{format_type}' for column {col_name}")
                    continue
                
                logging.debug(f"Unique values in {col_name}: {df[col_name].unique().tolist()}")
                
            except Exception as e:
                logging.error(f"Error converting column {col_name}: {str(e)}")
                continue

        return df
        
    except KeyError as e:
        logging.warning(f"Missing configuration key for date conversion: {e}")
        return df
    except Exception as e:
        logging.error(f"Error in date conversion: {e}")
        raise

# For backward compatibility
convert_date = convert_date_columns