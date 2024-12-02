import pandas as pd

def prepare(df, data_config: dict):
    
    bucket_config = data_config['configuration']['loan_buckets']
    bucket_count = bucket_config['bucket_count']
    bucket_map = bucket_config['bucket_map']

    print('Bucket Configuration Loaded...')
    print(f'No of buckets: {bucket_count}')
    print(f'Bucket Map: {bucket_map}')

    # Checking if no of loan buckets provided are also mapped
    if(bucket_count == len(bucket_map)):
        print(f"No of buckets {bucket_config['bucket_count']} is the same.")
    else:
        return print("Buckets not mapped correctly. Please check.")
    

    def filter_columns(df):
        df = df[data_config['configuration']['required_cols']]
        return df

    def filter_buckets(df_group):

        # Sort the group by ACT_PERIOD
        group_sorted = df_group.sort_values(by='ACT_PERIOD')
        
        # Check for 'bucket_count' and drop subsequent rows if found
        bucket_count_index = group_sorted[group_sorted['DLQ_STATUS'] >= bucket_count].index

        if not bucket_count_index.empty:
            
        # Keep only rows up to the first occurrence of 'bucket_count'
            if group_sorted[group_sorted['DLQ_STATUS'] == bucket_count].__len__():
                first_bucket_count_idx = bucket_count_index[0]
            else:
                first_bucket_count_idx = bucket_count_index[0] -1

            return group_sorted.loc[:first_bucket_count_idx]

        return group_sorted  # Return the whole group if 'bucket_count' is not found

    def filter_terms(df):
        pass
    
    print("Filtering columns as required by the model...", "\n", data_config['configuration']['required_cols'])
    df = filter_columns(df)
    print(f"Columns filtered..\n {df.columns}")

    print("Filtering Buckets..")
    # Group by 'LOAN_ID', apply the filtering function, and reset index
    df_processed  = df.groupby('LOAN_ID').apply(filter_buckets).reset_index(drop=True)
    print(f"Buckets Filtered...\n{df_processed['DLQ_STATUS'].unique()}")

    return df_processed