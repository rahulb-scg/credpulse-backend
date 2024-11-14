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
    temp = df.astype(dtype_map)
    return temp