import os

# Base directory for data files
DATA_DIR = r"C:\Users\jimmy\Documents\Data Projects\Project\Financial Transactions Project\data"

# Individual file paths
CARDS_FILE = '/opt/airflow/data/cards_data.csv'
CARDS_FILE_LOCAL = os.path.join(DATA_DIR, "cards_data.csv") #use to test in local
USERS_FILE = '/opt/airflow/data/users_data.csv'
USERS_FILE_LOCAL = os.path.join(DATA_DIR, "users_data.csv")
TRANSACTIONS_FILE = '/opt/airflow/data/transactions_data.csv'
TRANSACTIONS_FILE_LOCAL = os.path.join(DATA_DIR, "transactions_data.csv")
MCC_FILE_LOCAL = os.path.join(DATA_DIR,'mcc_codes.json')
MCC_FILE = '/opt/airflow/data/mcc_codes.json'  # Path to MCC codes JSON file
FILTERED_TRANSACTIONS_FILE = '/opt/airflow/data/filtered_transactions_data.csv' #filtered transactions since 2018
FILTERED_TRANSACTIONS_FILE_LOCAL = os.path.join(DATA_DIR,"filtered_transactions_data.csv")
# SQL Server credentials and connection settings
DB_USER = "sa"
DB_PASS = "Enero1072."
DB_SERVER = "localhost"  
DB_NAME = "FinancialDW"
DOCKER_DB_SERVER = "host.docker.internal"  # Use this for Docker container access to host
