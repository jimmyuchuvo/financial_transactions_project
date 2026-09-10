# utils.py
#define a utility module for ETL processes
import pandas as pd
import os
import logging
from logging.handlers import RotatingFileHandler
import re

# create a function to explore a column in a DataFrame
def explore_column(df, col):
    """
    Displays a comprehensive summary of a column in a styled DataFrame,
    including special characters and their counts.

    Parameters:
        df  : pandas DataFrame
        col : str, name of the column to explore
    """
    series = df[col]
    str_series = series.dropna().astype(str)

    # --- Value counts with % ---
    vc = series.value_counts(dropna=False)
    vc_pct = series.value_counts(dropna=False, normalize=True).mul(100).round(2)
    vc_cumulative_pct = vc_pct.cumsum().round(2)

    value_counts_df = pd.DataFrame({
        'count': vc,
        'percentage (%)': vc_pct,
        'cumulative (%)': vc_cumulative_pct,
    })
    value_counts_df.index.name = col

    # --- String length stats ---
    lengths = str_series.str.len()

    # --- Whitespace / casing issues ---
    has_leading_trailing = str_series.str.strip().ne(str_series).sum()
    has_mixed_case = str_series.apply(
        lambda x: x != x.lower() and x != x.upper()
    ).sum()
    has_digits = str_series.str.contains(r'\d', regex=True).sum()

    # --- Special characters ---
    special_char_mask = str_series.str.contains(r'[^a-zA-Z0-9\s]', regex=True)
    has_special_chars = special_char_mask.sum()

    # Count each special character across the column
    special_chars_counter = (
        str_series[special_char_mask]
        .str.findall(r'[^a-zA-Z0-9\s]')
        .explode()
        .value_counts()
        .to_dict()
    )

    # --- Duplicates ---
    duplicated_count = series.duplicated().sum()

    # --- Summary stats ---
    summary = {
        # Volume
        'total_rows': len(series),
        'non_missing_values': series.notnull().sum(),
        'missing_values': series.isnull().sum(),
        'non_missing_pct (%)': round(series.notnull().mean() * 100, 2),
        'missing_pct (%)': round(series.isnull().mean() * 100, 2),
        # Cardinality
        'unique_values': series.nunique(dropna=True),
        'is_unique': series.is_unique,
        'duplicated_values': duplicated_count,
        # Frequency
        'most_frequent': series.mode()[0] if not series.mode().empty else None,
        'most_frequent_count': series.value_counts().iloc[0] if len(series.value_counts()) > 0 else 0,
        'most_frequent_pct (%)': round(series.value_counts(normalize=True).iloc[0] * 100, 2) if len(series.value_counts()) > 0 else 0,
        'least_frequent': series.value_counts().index[-1] if len(series.value_counts()) > 0 else None,
        'least_frequent_count': series.value_counts().iloc[-1] if len(series.value_counts()) > 0 else 0,
        # String length
        'min_length': int(lengths.min()) if len(lengths) > 0 else None,
        'max_length': int(lengths.max()) if len(lengths) > 0 else None,
        'mean_length': round(lengths.mean(), 2) if len(lengths) > 0 else None,
        'median_length': round(lengths.median(), 2) if len(lengths) > 0 else None,
        'std_length': round(lengths.std(), 2) if len(lengths) > 0 else None,
        # Quality flags
        'leading/trailing whitespace': has_leading_trailing,
        'mixed_case_values': has_mixed_case,
        'values_with_digits': has_digits,
        'values_with_special_chars': has_special_chars,
    }

    summary_df = pd.DataFrame.from_dict(summary, orient='index', columns=['value'])
    summary_df.index.name = 'metric'

    # --- Display ---
    print(f"{'='*55}")
    print(f"  Column exploration: '{col}'  |  dtype: {series.dtype}")
    print(f"{'='*55}\n")

    print("[ Summary ]")
    display(summary_df) 

    print("\n[ Value Counts ]")
    display(value_counts_df)  

    if has_special_chars:
        print("\n[ Special Characters Count ]")
        # Convert Counter to DataFrame for display
        special_chars_df = pd.DataFrame.from_dict(special_chars_counter, orient='index', columns=['count'])
        special_chars_df.index.name = 'special_char'
        special_chars_df = special_chars_df.sort_values('count', ascending=False)
        display(special_chars_df)





# Define a function to set up logging for the ETL process
def setup_logging(
                  log_dir=r"C:\Users\jimmy\Documents\Data Projects\Project\Financial Transactions Project\logs", 
                  log_file='Financial_ETL.log',
                  max_bytes=1*1024*1024,
                  backup_count=3)-> logging.Logger:
    """
    Set up logging for the ETL process.
    Args:
        log_dir (str): The directory where log files will be stored.
        log_file (str): The name of the log file.
        max_bytes (int): Maximum size in bytes for a log file before rotation.
        backup_count (int): Number of backup log files to keep
    Returns:
        logger (logging.Logger): Configured logger object.
    """
    
    os.makedirs(log_dir, exist_ok=True) # Ensure the log directory exists; create it if it doesn't
    log_path = os.path.join(log_dir, log_file) # Build the full path to the log file

    # Get a logger object
    logger = logging.getLogger()  # Use a specific name for the logger

    if  logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.DEBUG)  # Set minimum level to capture all messages
    
    # Create a rotating file handler
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
        ) 
    file_handler.setLevel(logging.DEBUG) 


    # Define the log message format
    formatter = logging.Formatter( '%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)  # Apply format to the file handler
    logger.addHandler(file_handler) # Add the file handler to the logger

    return logger


# Compile regex patterns
REG_EXTRACT_CARD_CREDIT_LIMIT = re.compile(r'(\d+)') # Extract credit limit from card information (dim_card)

REG_CREATE_DATE_KEY = re.compile(r'[^A-Za-z0-9]')  # Follow best practices for creating date keys (dim_date)
