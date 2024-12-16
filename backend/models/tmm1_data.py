import pandas as pd
import logging

def prepare(df, data_config: dict):
    logging.debug("Preparing data with configuration: %s", data_config)

    bucket_config = data_config['configuration']['loan_buckets']
    bucket_count = bucket_config['bucket_count']
    bucket_map = bucket_config['bucket_map']

    logging.info('Bucket Configuration Loaded...')
    logging.info(f'No of buckets: {bucket_count}')
    logging.info(f'Bucket Map: {bucket_map}')

    # Checking if no. of loan buckets provided are also mapped
    if bucket_count == len(bucket_map):
        logging.info(f"No. of buckets {bucket_config['bucket_count']} matches the mapping.")
    else:
        logging.error("Buckets not mapped correctly. Please check.")
        return None
    
    # Filtering columns as required by the model
    def filter_columns(df):
        df = df[data_config['configuration']['required_cols']]
        logging.debug("Filtered columns: %s", df.columns)
        return df

    # Filtering buckets based on the bucket map
    def filter_buckets(df_group):
        try:
            # Sort the group by ACT_PERIOD
            group_sorted = df_group.sort_values(by='ACT_PERIOD')
            # Keep only rows where DLQ_STATUS is in bucket_map keys
            valid_statuses = [int(k) for k in bucket_map.keys()]
            filtered_df = group_sorted[group_sorted['DLQ_STATUS'].isin(valid_statuses)]
            return filtered_df

        except Exception as e:
            logging.error(f"Error in filter_buckets: {e}")
            logging.debug(f"Group data: {df_group}")
            raise e

    logging.info("Filtering columns as required by the model...")
    df = filter_columns(df)
    logging.info("Columns filtered: %s", df.columns)

    logging.info("Filtering Buckets...")
    try:
        logging.debug("Unique DLQ_STATUS values: %s", df['DLQ_STATUS'].unique())
        df_processed = df.groupby('LOAN_ID').apply(filter_buckets).reset_index(drop=True)
        logging.debug("Processed DLQ_STATUS values: %s", df_processed['DLQ_STATUS'].unique())
    
        logging.info("Buckets Filtered: %s", df_processed['DLQ_STATUS'].unique())
        
        return df_processed
    
    except Exception as e:
        logging.error(f"Error in prepare function: {e}")
        raise e