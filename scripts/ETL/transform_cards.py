import pandas as pd
import logging

logger = logging.getLogger(__name__)

#check for duplicates
def cards_check_duplicates(df: pd.DataFrame)-> None:
    """
    Check for duplicate rows in the DataFrame based on specified columns.
    Args:
        df (pd.DataFrame): The DataFrame to check for duplicates.
    Returns:
        None
    """
    try:
        
        critical_columns = ['id', 'client_id']
        duplicates = df.duplicated(subset=critical_columns).sum()
        
        if duplicates > 0:
            logger.info(f"There are {duplicates} duplicate records in the data based on columns {critical_columns}.")
        else:
            logger.info(f"No duplicate records found in the data based on columns {critical_columns}.")

    except Exception as e:
        logger.error(f"An error occurred while checking for duplicates: {e}")
        raise

# Check for missing values
def cards_check_missing_values(df: pd.DataFrame) -> None:
    """
    Check for missing values in the DataFrame and log the results.
    Args:
        df (pd.DataFrame): The DataFrame to check for missing values.
    Returns:
        None
    """
    try:
        missing_values = df.isnull().sum()
        missing_columns = missing_values[missing_values > 0]
        
        if not missing_columns.empty:
            for column, count in missing_columns.items():
                logger.info(f"Column '{column}' has {count} missing values in CARDS data.")
            
            if all(col in missing_columns for col in ['id', 'client_id']):
                logger.error("Critical columns 'id' and 'client_id' in CARDS data contain missing values, which may affect data integrity.")
        else:
            logger.info("No missing values found in CARDS data.")
    
    except Exception as e:
        logger.error(f"An error occurred while checking for missing values in CARDS data: {e}")
        raise

# Remove and rename columns

def cards_remove_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove and rename specified columns from the DataFrame.
    Args:
        df (pd.DataFrame): The DataFrame from which to remove columns.
    Returns:
        pd.DataFrame: The DataFrame with specified columns removed.
    """
    try:
        columns_to_remove = ['client_id', 'card_number', 'cvv']
        columns_to_rename = {'id': 'card_id'}
        df_cleaned = df.copy()
        df_cleaned = df.drop(columns=columns_to_remove, errors='ignore')
        df_cleaned = df_cleaned.rename(columns=columns_to_rename)
        logger.info(f"Removed columns {columns_to_remove} from CARDS data.")
        return df_cleaned
    
    except Exception as e:
        logger.error(f"An error occurred while removing columns from CARDS data: {e}")
        raise