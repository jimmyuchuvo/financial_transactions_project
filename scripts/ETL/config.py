import os

# Base directory for data files
DATA_DIR = r"C:\Users\jimmy\Documents\Data Projects\Project\Financial Transactions Project\data"

# Individual file paths
CARDS_FILE = os.path.join(DATA_DIR, "cards_data.csv")
TRANSACTIONS_FILE = os.path.join(DATA_DIR, "transactions_data.csv")
USERS_FILE = os.path.join(DATA_DIR, "users_data.csv")

# SQL Server credentials and connection settings
DB_USER = "sa"
DB_PASS = "Enero1072."
DB_SERVER = "localhost"  
DB_NAME = "FinancialDW"