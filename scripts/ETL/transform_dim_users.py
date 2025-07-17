import pandas as pd
import logging
import ETL.config as config


logger = logging.getLogger(__name__)

#etl_users.py
import pandas as pd
import logging
import ETL.config as config
logger = logging.getLogger(__name__)


def users_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the users DataFrame.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to transform.
        
    Returns:
        pd.DataFrame: The transformed DataFrame.
    """
    critical_columns = ['user_id']
    columns_to_remove = []
    columns_to_rename = {'id': 'user_id', 'address': 'user_address'}

    try:
        logger.info('Starting transformation of users DataFrame')

        df_cleaned = df.copy()  # Create a copy to avoid modifying the original DataFrame
        df_cleaned = df_cleaned.drop(columns=columns_to_remove) # Remove specified columns
        df_cleaned = df_cleaned.rename(columns=columns_to_rename) # Rename specified columns

        duplicates_count = df_cleaned.duplicated(subset=critical_columns).sum()
        if duplicates_count > 0:
            duplicates = df.loc[df_cleaned.duplicated(subset=critical_columns), critical_columns]
            logger.info(f"Found {duplicates_count} duplicate records based on columns {critical_columns}.")
            logger.info(f"Duplicate records:\n{duplicates}")
            raise ValueError(f'{duplicates_count} duplicate records found in CARDS data {critical_columns} columns.')

        #clean up values from columns
        df_cleaned['gender'] = df_cleaned['gender'].str.lower() 
        df_cleaned['per_capita_income'] = df_cleaned['per_capita_income'].fillna(0).str.extract(r'(\d+)').astype(int)
        df_cleaned['yearly_income'] = df_cleaned['yearly_income'].fillna(0).str.extract(r'(\d+)').astype(int)
        df_cleaned['total_debt'] = df_cleaned['total_debt'].fillna(0).str.extract(r'(\d+)').astype(int)
        df_cleaned['user_address'] = df_cleaned['user_address'].fillna('n/a').str.replace(r'\d+', '', regex=True).str.strip().astype(str)

        logger.info('✅Transformation of users DataFrame completed successfully')
        return df_cleaned
    
    except Exception as e:
        logger.error(f'❌Error during transformation of users DataFrame: {e}')
        raise 


