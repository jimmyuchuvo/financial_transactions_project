# File to extract data by loading CSV files into DataFrames
import pandas as pd
import logging
import ETL.config as config

logger = logging.getLogger(__name__)

def extract_data(file_path: str, data_type: str) -> pd.DataFrame:
    """
    Generic function to extract data from CSV files and return as DataFrame.
    Logs the number of records loaded or any errors encountered during the process.
    
    Args:
        file_path (str): Path to the CSV file to extract
        data_type (str): Name of the data type for logging purposes (e.g., 'cards', 'transactions', 'users')
    
    Returns:
        pd.DataFrame: DataFrame containing the extracted data
        
    Raises:
        Exception: Re-raises any exception encountered during CSV reading
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"✅ Extracted {data_type}_data.csv with {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"❌ Error extracting {data_type}_data.csv: {e}")
        raise