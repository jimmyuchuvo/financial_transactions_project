import os

# Base directory for data files
DATA_DIR = r"C:\Users\jimmy\Documents\Data Projects\Project\Financial Transactions Project\data"

# Individual file paths
CARDS_FILE = '/opt/airflow/data/cards_data.csv'
USERS_FILE = '/opt/airflow/data/users_data.csv'
CARDS_FILE_LOCAL = os.path.join(DATA_DIR, "cards_data.csv") #use to test in local
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions_data.csv")
USERS_FILE_LOCAL = os.path.join(DATA_DIR, "users_data.csv")

# SQL Server credentials and connection settings
DB_USER = "sa"
DB_PASS = "Enero1072."
DB_SERVER = "localhost"  
DB_NAME = "FinancialDW"
DOCKER_DB_SERVER = "host.docker.internal"  # Use this for Docker container access to host
