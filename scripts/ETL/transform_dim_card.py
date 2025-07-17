import pandas as pd
import logging
import ETL.config as config


logger = logging.getLogger(__name__)


def cards_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tranform the DataFrame to create dim_card.
    Args:
        df (pd.DataFrame): The DataFrame to transform.
    Returns:
        pd.DataFrame: The DataFrame with specified transformations.
    """
    # Define the critical columns, columns to remove, and columns to rename
    critical_columns = ['card_id']
    columns_to_remove = ['client_id', 'card_number', 'cvv']
    columns_to_rename = {'id': 'card_id','acct_open_date': 'account_open_date'}

    try:
        logger.info("Starting transformation of CARDS data.")


        df_cleaned = df.copy()# Create a copy to avoid modifying the original DataFrame
        df_cleaned = df_cleaned.drop(columns=columns_to_remove) # Remove specified columns
        df_cleaned = df_cleaned.rename(columns=columns_to_rename) # Rename specified columns


        df_cleaned['credit_limit'] = df_cleaned['credit_limit'].fillna(0).str.extract(r'(\d+)').astype(int)
        df_cleaned['has_chip'] = df_cleaned['has_chip'].str.lower()
        df_cleaned['card_on_dark_web'] = df_cleaned['card_on_dark_web'].str.lower()
        df_cleaned['card_id'] = df_cleaned['card_id'].astype(int)
        df_cleaned['expires'] = pd.to_datetime('01/' + df_cleaned['expires'].astype(str), format='%d/%m/%Y', errors='raise')
        df_cleaned['account_open_date'] = pd.to_datetime('01/' + df_cleaned['account_open_date'].astype(str), format='%d/%m/%Y', errors='raise')

        df_cleaned = df_cleaned.drop_duplicates()

        logger.info('✅Succesfully finished transformation of CARDS data.')

        return df_cleaned
    
    except Exception as e:
        logger.error(f'❌An error occurred while transforming CARDS data: {e}')
        raise

