import pandas as pd

def prepare(df, data_config: dict):
    
    bucket_config = data_config['configuration']['loan_buckets']
    bucket_count = bucket_config['bucket_count']
    bucket_map = bucket_config['bucket_map']

    print('Bucket Configuration Loaded...')
    print(f'No of buckets: {bucket_count}')
    print(f'Bucket Map: {bucket_map}')

    # Checking if no. of loan buckets provided are also mapped
    if(bucket_count == len(bucket_map)):
        print(f"No. of buckets {bucket_config['bucket_count']} matches the mapping.")
    else:
        return print("Buckets not mapped correctly. Please check.")
    
    # Filtering columns as required by the model
    def filter_columns(df):
        df = df[data_config['configuration']['required_cols']]
        return df

    # Filtering buckets based on the bucket map
    def filter_buckets(df_group):
        try:
            # Sort the group by ACT_PERIOD
            group_sorted = df_group.sort_values(by='ACT_PERIOD')
            # Keep only rows where DLQ_STATUS is in bucket_map keys
            # Convert bucket_map keys to integers since DLQ_STATUS is numerical
            valid_statuses = [int(k) for k in bucket_map.keys()]
            filtered_df = group_sorted[group_sorted['DLQ_STATUS'].isin(valid_statuses)]

            return filtered_df

        except Exception as e:
            print(f"Error in filter_buckets: {e}")
            print(f"Group data: {df_group}")
            raise e

    print("Filtering columns as required by the model...", "\n", data_config['configuration']['required_cols'])
    df = filter_columns(df)
    print(f"Columns filtered..\n {df.columns}")

    print("Filtering Buckets..")
    try:
        print(df['DLQ_STATUS'].unique())
        df_processed = df.groupby('LOAN_ID').apply(filter_buckets).reset_index(drop=True)
        print(df_processed['DLQ_STATUS'].unique())
    
        # Map DLQ_STATUS directly to bucket names
        # bucket_name_map = {k: v for k, v in bucket_map.items()}
        # df_processed['DLQ_STATUS'] = df_processed['DLQ_STATUS'].astype(str).map(bucket_name_map)
        
        print(f"Buckets Filtered...\n{df_processed['DLQ_STATUS'].unique()}")
        
        return df_processed
    
    except Exception as e:
        print(f"Error in prepare function: {e}")
        raise e