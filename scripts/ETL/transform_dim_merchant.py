
import pandas as pd
import logging
import ETL.config as config
import ETL.extract_data as extract

logger = logging.getLogger(__name__)



def transform_dim_merchant(df: pd.DataFrame, data_name: str) -> pd.DataFrame:
    """
    Transforms the merchant data by merging with MCC codes and adding a merchant key.
    
    Parameters:
    df (pd.DataFrame): DataFrame containing the merchant data.
    data_name (str): data name of the dim
    
    Returns:
    pd.DataFrame: Transformed DataFrame with additional columns.
    """
    columns_to_rename = {'zip': 'merchant_zip'}

    try:
        logger.info(f'Starting transformation of {data_name} DataFrame')

        df = df.copy()
        df = df.drop_duplicates()
        df['merchant_key'] = df.index
        df.loc[(df['merchant_state'].isna()) & (df['merchant_city'] == 'ONLINE'), ['merchant_state']] = 'ONLINE'
        df.loc[(df['merchant_state'] == 'ONLINE') & (df['merchant_city'] == 'ONLINE'), 'zip'] = 0   
        df['zip'] = df['zip'].fillna(0)
        mcc_df = extract.extract_json_mcc_data(config.MCC_FILE, 'MCC')
        dim_merchant_mcc = df.merge(mcc_df, left_on='mcc', right_on='mcc', how='left').reset_index(drop=True)
        
        dim_merchant_mcc = dim_merchant_mcc.rename(columns=columns_to_rename)
        
        return dim_merchant_mcc
        logger.info(f'✅ Successfully transformed {data_name} data with {len(dim_merchant_mcc)} records')
    except Exception as e:
        logger.error(f'❌Error transforming data: {e}')
        raise

 