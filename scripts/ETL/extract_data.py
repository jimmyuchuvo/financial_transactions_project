# File to extract data by loading CSV files into DataFrames
import pandas as pd
import logging
import ETL.config as config
import json


logger = logging.getLogger(__name__)

def extract_data(file_path: str, data_type: str, columns: list=None) -> pd.DataFrame:
    """
    Generic function to extract data from CSV files and return as DataFrame.
    Logs the number of records loaded or any errors encountered during the process.
    
    Args:
        file_path (str): Path to the CSV file to extract
        data_type (str): Name of the data type for logging purposes (e.g., 'cards', 'transactions', 'users')
        columns (List[str]): Optional if you need to filter columns in the extracting
    Returns:
        pd.DataFrame: DataFrame containing the extracted data
        
    Raises:
        Exception: Re-raises any exception encountered during CSV reading
    """
    try:

        if columns is not None:
            df = pd.read_csv(file_path, usecols=columns, low_memory=True)
        else:
            df = pd.read_csv(file_path, low_memory=True)

        logger.info(f"✅ Extracted {data_type} data.csv with {len(df)} records")
        return df
    
    except Exception as e:
        logger.error(f"❌ Error extracting {data_type}_data.csv: {e}")
        raise

# Function to extract JSON data and return as DataFrame
def extract_json_mcc_data(json_path:str, data_name:str) -> pd.DataFrame:
    """
    Extracts data from a JSON file and returns it as a pandas DataFrame.
    
    Parameters:
    json_path (str): The path to the JSON file.
    data_name (str): The name of the data to be extracted.
    
    Returns:
    pd.DataFrame: DataFrame containing the extracted data.
    """
    try:
        
        with open(json_path, 'r') as f:
            mcc_dict = json.load(f)

        # Convert dictionary to DataFrame
        df = pd.DataFrame(list(mcc_dict.items()), columns=['mcc', 'mcc_description'])
        df['mcc'] = df['mcc'].astype(int)

        return df
    
        logger.info(f'✅ Extracted {data_name} data with {len(df)} records')
                
    except Exception as e:
        logger.error(f'❌Error extracting data from {data_name}: {e}')
        raise
