from backend.data_handler import duplicate_handler, column_dtypes

def replace_values(df, data_config):
    """
    Function to replace values in a dataset such that it may cause errors with other operations.

    Parameters:
    df (pd.DataFrame): The input DataFrame to check for duplicates.
    data_config (file): The input dataset's configuration file
    """

    # Accessing column to perform the replace operation on
    column_name = data_config['configuration']['data_specific_functions']['replace_values'][0]['column_name']
    print(f"Replacing values in column: {column_name}")

    # Accessing value to replace
    value_to_replace = data_config['configuration']['data_specific_functions']['replace_values'][0]['values_to_replace'][0]
    print(f"Replacing value: {value_to_replace}")
    
    # Accessing value to replace with
    value_to_replace_with = data_config['configuration']['data_specific_functions']['replace_values'][0]['values_to_replace_with'][0]
    print(f"Replacing value with: {value_to_replace_with}")

    # Replacing Value
    df[column_name] = df[column_name].replace(value_to_replace, value_to_replace_with)
    print(df[column_name].unique())

    return df


def preprocess(df, data_config):
    # Handling duplicates in the data
    df_duplicate_handled = duplicate_handler.handle_duplicates(df)
    print("Duplicate Handling Complete")

    # Performing data-type specific replacement operations before coverting column dtypes
    print("Handling value replacements")
    df_values_replaced = replace_values(df_duplicate_handled, data_config)
    print("Handling value replacements Complete")

    # Converting Column dtypes
    print("Converting column dtypes")
    df_column_dtype_set = column_dtypes.convert_columns_dtype(
        df_values_replaced,
        data_config
    )
    print("Conversion complete")

    # Saving last operated dataset with a new name
    preprocessed_dataset = df_column_dtype_set
    print(preprocessed_dataset.shape)
    return(preprocessed_dataset)