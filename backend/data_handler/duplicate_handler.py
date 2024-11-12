def handle_duplicates(df, case="remove", subset=None, keep='first', inplace=False):
    """
    Function to check and handle duplicates in a dataset based on different cases.

    Parameters:
    df (pd.DataFrame): The input DataFrame to check for duplicates.
    case (str): Defines how to handle duplicates. Options are:
        - "remove": Removes duplicate records (default).
        - "mark": Marks duplicate records in a new column called 'is_duplicate'.
        - "count": Returns the number of duplicate records.
        - "keep_last": Keeps the last occurrence of duplicates and removes others.
    subset (list): List of columns to consider for duplicate checking (default is all columns).
    keep (str): Specifies which duplicates to keep. Options are:
        - 'first' (default): Keeps the first occurrence.
        - 'last': Keeps the last occurrence.
        - False: Drops all duplicates.
    inplace (bool): If True, modifies the DataFrame in place. Otherwise, returns a new DataFrame.

    Returns:
    pd.DataFrame or int: Depending on the case, returns a DataFrame or an integer.
    """

    # Check for duplicates
    duplicate_exists = df.duplicated(subset=subset, keep=keep).any()

    if not duplicate_exists:
        print("No duplicates found in the dataset.")
        return df if not inplace else None

    # Handle cases
    if case == "remove":
        print(f"{duplicate_exists.shape[0]} duplicates found")
        print("Removing duplicates...")
        return df.drop_duplicates(subset=subset, keep='keep', inplace=inplace)

    elif case == "mark":
        print("Marking duplicates...")
        df['is_duplicate'] = df.duplicated(subset=subset, keep=keep)
        return df

    elif case == "keep_last":
        print("Keeping the last occurrence of duplicates...")
        return df.drop_duplicates(subset=subset, keep='last', inplace=inplace)

    else:
        print("Invalid case. Choose from: 'remove', 'mark', 'count', 'keep_last'.")
        return df