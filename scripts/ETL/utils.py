# utils.py
#define a utility module for ETL processes
import os
import logging
from logging.handlers import RotatingFileHandler
import ETL.config as config

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

# define a function to create the sqlserver connection string
def create_sqlserver_connection_string(
    server: str, 
    database: str, 
    username: str, 
    password: str
) -> str:
    """
    Create a SQL Server connection string.
    
    Args:
        server (str): The name or IP address of the SQL Server.
        database (str): The name of the database to connect to.
        username (str): The username for authentication.
        password (str): The password for authentication.
        
    Returns:
        str: A formatted connection string for SQL Server.
    """
    return f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
