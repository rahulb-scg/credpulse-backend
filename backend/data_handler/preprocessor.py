import logging
import pandas as pd
from typing import Dict, Any

from .duplicate_handler import handle_duplicates
from .column_dtypes import convert_columns_dtype
from .missing_value_handler import handle_missing_values

def replace_values(df: pd.DataFrame, data_config: Dict[str, Any]) -> pd.DataFrame:
    """
    Replace values in a dataset that may cause errors with other operations.

    Args:
        df: The input DataFrame
        data_config: The input dataset's configuration

    Returns:
        DataFrame with replaced values
    """
    try:
        # Get replacement configuration
        replace_config = data_config['configuration']['data_specific_functions']['replace_values'][0]
        column_name = replace_config['column_name']
        value_to_replace = replace_config['values_to_replace'][0]
        value_to_replace_with = replace_config['values_to_replace_with'][0]

        logging.info(f"Replacing value '{value_to_replace}' with '{value_to_replace_with}' in column: {column_name}")
        df[column_name] = df[column_name].replace(value_to_replace, value_to_replace_with)
        
        return df
    except KeyError as e:
        logging.warning(f"Missing configuration key for value replacement: {e}")
        return df
    except Exception as e:
        logging.error(f"Error in value replacement: {e}")
        raise

def preprocess(df: pd.DataFrame, data_config: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Preprocess the input DataFrame with various data handling steps.

    Args:
        df: Input DataFrame to preprocess
        data_config: Configuration dictionary for preprocessing steps

    Returns:
        Preprocessed DataFrame
    """
    logging.info("Starting data preprocessing pipeline")
    
    try:
        # Handle duplicates
        logging.info("Handling duplicates")
        df = handle_duplicates(df)
        
        if data_config:
            # Replace values if config is provided
            logging.info("Handling value replacements")
            df = replace_values(df, data_config)
            
            # Convert column data types
            logging.info("Converting column data types")
            df = convert_columns_dtype(df, data_config)
            
            # Handle missing values
            logging.info("Handling missing values")
            df = handle_missing_values(df, data_config)
        
        logging.info(f"Preprocessing complete. Final shape: {df.shape}")
        return df
        
    except Exception as e:
        logging.error(f"Error in preprocessing pipeline: {e}")
        raise