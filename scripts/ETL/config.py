import os

# Base directory for data files
DATA_DIR = r"C:\Users\jimmy\Documents\Data Projects\Project\Financial Transactions Project\data"

# Individual file paths
CARDS_FILE = '/opt/airflow/data/cards_data.parquet'  # Path to cards data file in Airflow environment
CARDS_FILE_LOCAL = os.path.join(DATA_DIR, "cards_data.parquet") #use to test in local
USERS_FILE = '/opt/airflow/data/users_data.parquet'  # Path to users data file in Airflow environment
USERS_FILE_LOCAL = os.path.join(DATA_DIR, "users_data.parquet")
TRANSACTIONS_FILE = '/opt/airflow/data/transactions_data.parquet'  # Path to transactions data file in Airflow environment
TRANSACTIONS_FILE_LOCAL = os.path.join(DATA_DIR, "transactions_data.parquet")
MCC_FILE_LOCAL = os.path.join(DATA_DIR,'mcc_codes.json')
MCC_FILE = '/opt/airflow/data/mcc_codes.json'  # Path to MCC codes JSON file
FILTERED_TRANSACTIONS_FILE = '/opt/airflow/data/filtered_transactions_data.parquet' #filtered transactions since 2018
FILTERED_TRANSACTIONS_FILE_LOCAL = os.path.join(DATA_DIR,"filtered_transactions_data.parquet")

# SQL Server credentials and connection settings
DB_USER = "sa"
DB_LOCAL_MACHINE_USER = 'local_machine_user' 
DB_PASS = "Enero1072."
DB_SERVER = "localhost"  
DB_NAME = "FinancialDW"
DOCKER_DB_SERVER = "host.docker.internal"  # Use this for Docker container access to host
DB_SERVER_LOCAL = r"Jimmy\SQLEXPRESS01"

