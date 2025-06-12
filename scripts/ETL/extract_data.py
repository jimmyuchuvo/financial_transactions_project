# File to extract data by loading CSV files into DataFrames

import pandas as pd
import logging
import ETL.config as config

logger = logging.getLogger(__name__)

def extract_cards_data() -> pd.DataFrame:
    """
    This function extracts card data from a CSV file and returns it as a DataFrame.
    It logs the number of records loaded or any errors encountered during the process.
    The function uses the path defined in the config module to locate the CSV file.

    Args:
        None
    Returns:
        pd.DataFrame: DataFrame containing card data if successful, None otherwise.
    """
    try:
        df = pd.read_csv(config.CARDS_FILE)
        logger.info(f"✅ Extracted cards_data.csv with {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"❌ Error loading cards_data.csv: {e}")
        raise 


def extract_transactions_data() -> pd.DataFrame:
    """
    This function extracts transaction data from a CSV file and returns it as a DataFrame.
    It logs the number of records loaded or any errors encountered during the process.
    The function uses the path defined in the config module to locate the CSV file.
    Args:
        None
    Returns:
        pd.DataFrame: DataFrame containing transaction data if successful, None otherwise.
    """
    try:
        df = pd.read_csv(config.TRANSACTIONS_FILE)
        logger.info(f"✅ Extracted transactions_data.csv with {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"❌ Error loading transactions_data.csv: {e}")
        return None


def extract_users_data() -> pd.DataFrame:
    """
    This function extracts user data from a CSV file and returns it as a DataFrame.
    It logs the number of records loaded or any errors encountered during the process.
    The function uses the path defined in the config module to locate the CSV file.
    Args:
        None
    Returns:
        pd.DataFrame: DataFrame containing user data if successful, None otherwise.
    """
    try:
        df = pd.read_csv(config.USERS_FILE)
        logger.info(f"✅ Extracted users_data.csv with {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"❌ Error loading users_data.csv: {e}")
        return None
