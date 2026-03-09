├── catalog/                         # Architecture & lineage diagrams
│   ├── data_architecture.svg
│   ├── data_lineage.svg
│   └── star_model.png
│
├── data/                            # Input datasets (raw/reference)
│   ├── cards_data.csv
│   ├── mcc_codes.json
│   ├── transactions_data.csv
│   └── users_data.csv
│
├── docker/
│   └── airflow/                     # Airflow environment
│       ├── dags/                    # ETL DAG definitions
│       │   ├── etl_dim_card_dag.py
│       │   ├── etl_dim_date_dag.py
│       │   ├── etl_dim_merchant_dag.py
│       │   ├── etl_dim_users_dag.py
│       │   ├── etl_fact_transactions_dag.py
│       │   └── test_group_dag.py
│       ├── .dockerignore
│       ├── Dockerfile
│       ├── docker-compose.yaml
│       └── requirements.txt
│
├── scripts/
│   ├── ETL/                         # Transformation logic
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── extract_data.py
│   │   ├── load_data.py
│   │   ├── transform_dim_card.py
│   │   ├── transform_dim_date.py
│   │   ├── transform_dim_merchant.py
│   │   ├── transform_dim_users.py
│   │   ├── transform_fact_transactions.py
│   │   └── utils.py
│   │
│   ├── EDA_cards_data.ipynb
│   ├── EDA_date_data.ipynb
│   ├── EDA_merchant_data.ipynb
│   ├── EDA_transactions_data.ipynb
│   └── EDA_users_data.ipynb
│
├── sql/                             # Data warehouse DDL & quality checks
│   ├── Create FinancialDW.sql
│   ├── dim_cards_data_quality_check.sql
│   ├── dim_date_data_quality_check.sql
│   ├── dim_merchant_data_quality_checkk.sql
│   ├── dim_users_data_quality_check.sql
│   ├── fact_transactions_data_quality_check.sql
│   ├── index_strategy.sql
│   └── partitioning_strategy.sql
│
├── visualization_PBI/               # Power BI semantic model & report
│   ├── financial_transactions_BI.Report/
│   ├── financial_transactions_BI.SemanticModel/
│   └── financial_transactions_BI.pbip
│
├── .gitattributes
├── .gitignore
├── README.md
└── requirements.txt
