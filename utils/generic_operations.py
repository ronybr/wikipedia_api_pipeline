import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_duplicates(df: pd.DataFrame, subset: list = None) -> pd.DataFrame:
    """
    Counts and removes duplicate rows in a Pandas DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        subset (list, optional): List of column names to check for duplicates.
                                 If None, all columns are considered.

    Returns:
        pd.DataFrame: A cleaned DataFrame with duplicates removed.
    """
    try:
        # Check if subset is provided
        if subset is None:
            subset = df.columns.tolist()
        else:
            # Check if subset columns exist in the DataFrame
            for column in subset:
                if column not in df.columns:
                    raise ValueError(f"Column '{column}' not found in DataFrame.")

        # Convert any lists or other unhashable types into strings
        logger.info("Checking for lists in the DataFrame...")
        df = df.applymap(lambda x: str(x) if isinstance(x, list) else x)

        # Count duplicates
        num_duplicates = df.duplicated(subset=subset).sum()
        logger.info(f"Number of duplicate rows: {num_duplicates}")

        # Remove duplicates
        cleaned_df = df.drop_duplicates(subset=subset, keep='first').reset_index(drop=True)

        # Replace NaN values with empty strings
        cleaned_df.fillna("", inplace=True)
        logger.info("Data was cleaned successfully.")
        logger.info(f"Number of rows after cleaning: {cleaned_df.shape[0]}")

        return cleaned_df
    except Exception as error:
        logger.error(f"Error occurred to clean data: {error}")
        raise error
