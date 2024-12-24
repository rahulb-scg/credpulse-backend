import pandas as pd
import logging
from typing import Dict, Any

from .date_handler import convert_date_columns

def convert_columns_dtype(df: pd.DataFrame, data_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert DataFrame columns to specified data types based on configuration.

    Args:
        df: The input DataFrame
        data_config: Configuration dictionary containing date column information
                    and data type mapping

    Returns:
        DataFrame with converted column data types
    """
    try:
        # Get dtype mapping from config
        dtype_map = data_config['configuration']['attributes']['dtype']
        
        logging.info("Converting date columns...")
        df = convert_date_columns(df, data_config)

        try:
            # Try to convert the whole DataFrame at once
            df = df.astype(dtype_map)
            logging.info("All columns converted successfully")
        except Exception as e:
            logging.warning(f"Bulk conversion failed: {e}. Attempting column-by-column conversion...")
            
            # If bulk conversion fails, try per-column conversion
            for column, dtype in dtype_map.items():
                try:
                    df[column] = df[column].astype(dtype)
                    logging.info(f"Column '{column}' converted to {dtype}")
                except Exception as column_error:
                    logging.error(f"Failed to convert column '{column}': {column_error}")

        return df
        
    except KeyError as e:
        logging.warning(f"Missing configuration key for data type conversion: {e}")
        return df
    except Exception as e:
        logging.error(f"Error in data type conversion: {e}")
        raise

# For backward compatibility
column_dtypes = convert_columns_dtype