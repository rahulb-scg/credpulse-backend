import pandas as pd
import logging

# Configure logger
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def handle_missing_values(
    df,
    config_file=None       # Configuration file dictionary
):
    """
    Function to handle missing values in a dataset based on different strategies.
    """
    try:
        # To check and display missing values
        logger.debug("\n--- Missing Values Summary ---")
        missing_values = df.isnull().sum()
        missing_percentages = (missing_values / len(df)) * 100
        Missing_summary = pd.DataFrame({
            'Null Count': missing_values,
            'Percentage': missing_percentages
        })
        logger.debug(Missing_summary)

        # To list columns with missing values
        Missing_Values_columns = Missing_summary[Missing_summary['Null Count'] > 0].index.tolist()
        logger.debug("\nColumns with missing values:")
        logger.debug(Missing_Values_columns)

        # Update parameters from configuration file
        method = config_file['configuration']['data_specific_functions']['missing_values']['method']
        subset = config_file['configuration']['data_specific_functions']['missing_values']['subset']
        drop_all_nulls = config_file['configuration']['data_specific_functions']['missing_values']['drop_all_nulls']
        null_threshold = config_file['configuration']['data_specific_functions']['missing_values']['null_threshold']

        # Drop columns with all null values if required
        if drop_all_nulls:
            try:
                dropped_cols = df.columns[df.isnull().all()].tolist()
                df = df.dropna(axis=1, how='all')
                logger.debug(f"Dropped columns with all null values: {dropped_cols}")
            except Exception as e:
                logger.error(f"Error while dropping columns with all null values: {e}")

        # Drop columns with nulls greater than the specified threshold
        if null_threshold:
            try:
                null_percentages = (df.isnull().sum() / len(df)) * 100
                cols_to_drop = null_percentages[null_percentages > null_threshold].index
                df = df.drop(columns=cols_to_drop)
                logger.debug(f"Dropped columns with null percentages greater than {null_threshold}: {cols_to_drop}")
            except Exception as e:
                logger.error(f"Error while dropping columns with null percentages greater than {null_threshold}: {e}")

        # Impute missing values for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        integer_cols = df.select_dtypes(include=['int64', 'int32', 'int16', 'int8', 'uint64', 'uint32', 'uint16', 'uint8']).columns

        logger.debug(f"Numeric columns identified: {numeric_cols.tolist()}")
        if method in ['mean', 'median']:
            for col in numeric_cols:
                try:
                    if method == 'mean':
                        mean_value = df[col].mean()
                        if pd.notna(mean_value):  # Check if mean_value is not NaN
                            fill_value = round(mean_value) if df[col].dtype == 'int' else mean_value
                            df[col] = df[col].fillna(fill_value)  # Avoid inplace=True
                            logger.debug(f"Filled missing values in column '{col}' with mean: {fill_value}")
                    else:
                        median_value = df[col].median()
                        if pd.notna(median_value):  # Check if median_value is not NaN
                            df[col] = df[col].fillna(median_value)  # Avoid inplace=True
                            logger.debug(f"Filled missing values in column '{col}' with median: {median_value}")
                except Exception as e:
                    logger.error(f"Error while imputing missing values in column '{col}': {e}")

        # Forward fill or backward fill for time series data
        if method in ['ffill', 'bfill']:
            for col in numeric_cols:
                try:
                    df[col] = df[col].fillna(method=method)  # Avoid inplace=True
                    logger.debug(f"Applied '{method}' fill in column '{col}'")
                except Exception as e:
                    logger.error(f"Error while applying '{method}' fill in column '{col}': {e}")

        # Subset-based imputation (if specified)
        if subset:
            for col in subset:
                try:
                    fill_value = df[col].mean() if df[col].dtype == 'number' else df[col].mode()[0]
                    df[col] = df[col].fillna(fill_value)  # Avoid inplace=True
                    logger.debug(f"Imputed missing values in subset column '{col}' with fill value: {fill_value}")
                except Exception as e:
                    logger.error(f"Error while imputing missing values in subset column '{col}': {e}")


    except Exception as e:
        logger.error(f"An error occurred in handle_missing_values: {e}")

    return df