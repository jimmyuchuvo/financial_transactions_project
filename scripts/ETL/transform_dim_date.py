import pandas as pd
import logging
import ETL.config as config

logger = logging.getLogger(__name__)


def create_dim_date(df: pd.DataFrame, data_name: str) -> pd.DataFrame:
    '''
    Creates a date dimension table from a given DataFrame containing date-related data.

    Parameters:
    df : pd.DataFrame
        Input DataFrame that includes at least one column with datetime or date information.
    
    data_name : str
        A label or identifier used to name or tag the generated date dimension table.

    Returns:
    pd.DataFrame
        A transformed DataFrame structured as a date dimension, with additional time-related columns such as:
        - year
        - quarter
        - month
        - day
        - day of week
        - flags for holiday or weekend
        - custom hierarchy levels (if needed)
    '''
    bins   = [-1, 15, 30, 45, 59] #use it to create ['minute_block']
    labels = ['0015', '1630', '3145', '4659'] #use it to create ['minute_block']
    

    try:
        logger.info(f'Starting ETL {data_name} data')
        
        df = df.drop_duplicates()
        df['date'] = pd.to_datetime(df['date'], errors='raise')
        dt = df['date'].dt

        df['year'] = dt.year
        df['quarter'] = dt.quarter
        df['month'] = dt.month
        df['month_name'] = dt.month_name()
        df['day'] = dt.day
        df['day_name'] = dt.day_name()
        df['day_of_week'] = dt.day_of_week
        df['day_type'] = (dt.weekday > 5).map({True: 'Weekend', False: 'Weekday'})
        df['hour'] = dt.hour
        df['minute_block'] = pd.cut(pd.to_datetime(df['date']).dt.minute, bins=bins, labels=labels).astype(str)
        #df['date_key'] = df['date'].astype(str).str.replace(r'[^A-Za-z0-9]','',regex=True).str[:8] + df['minute_block'].astype(str)
        
        df['date_key'] = (
        df['year'].astype(str) +
        df['month'].astype(str).str.zfill(2) +
        df['day'].astype(str).str.zfill(2) +
        df['hour'].astype(str).str.zfill(2) +
        df['minute_block']
        ).astype(int)
        

        return df.drop(columns='date').drop_duplicates()

    except Exception as e:
        logger.info(f'Error in creating date dimension for {data_name}: {e}')
        raise