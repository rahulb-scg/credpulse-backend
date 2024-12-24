import pandas as pd
import logging

def handle_duplicates(df: pd.DataFrame, case: str = "remove", subset: list = None, 
                     keep: str = 'first', inplace: bool = False) -> pd.DataFrame:
    """
    Handle duplicates in a dataset based on different cases.

    Args:
        df: The input DataFrame to check for duplicates
        case: How to handle duplicates ('remove', 'mark', 'count', 'keep_last')
        subset: List of columns to consider for duplicate checking
        keep: Which duplicates to keep ('first', 'last', False)
        inplace: If True, modifies the DataFrame in place

    Returns:
        DataFrame with duplicates handled according to specified case
    """
    logging.info("Checking for duplicates")
    duplicate_exists = df.duplicated(subset=subset, keep=keep).any()

    if not duplicate_exists:
        logging.info("No duplicates found in the dataset")
        return df

    if case == "remove":
        logging.info("Removing duplicates...")
        if inplace:
            df.drop_duplicates(subset=subset, keep=keep, inplace=True)
            return df
        return df.drop_duplicates(subset=subset, keep=keep)

    elif case == "mark":
        logging.info("Marking duplicates...")
        df['is_duplicate'] = df.duplicated(subset=subset, keep=keep)
        return df

    elif case == "keep_last":
        logging.info("Keeping last occurrence of duplicates...")
        if inplace:
            df.drop_duplicates(subset=subset, keep='last', inplace=True)
            return df
        return df.drop_duplicates(subset=subset, keep='last')

    else:
        logging.warning("Invalid case. Choose from: 'remove', 'mark', 'count', 'keep_last'")
        return df

# For backward compatibility
duplicate_handler = handle_duplicates
