FROM apache/airflow:2.8.1-python3.8

USER root

# Install system dependencies and Microsoft ODBC Driver
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    freetds-dev \
    unixodbc-dev \
    libffi-dev \
    libssl-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies AFTER system build tools are installed
RUN pip install --no-cache-dir \
    pyodbc==4.0.39 \
    pandas==2.0.3 \
    apache-airflow-providers-microsoft-mssql==3.9.1

USER airflow

USER root

RUN echo "[ODBC Driver 18 for SQL Server]\n\
Description=Microsoft ODBC Driver 18 for SQL Server\n\
Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.so\n\
UsageCount=1" > /etc/odbcinst.ini

RUN echo "[my_mssql_dsn]\n\
Driver=ODBC Driver 18 for SQL Server\n\
Server=host.docker.internal\n\
Database=financialDW\n\
Port=1433\n\
UID=sa\n\
PWD=Enero1072.\n\
Trusted_Connection=No" > /etc/odbc.ini

USER airflow
RUN pip install apache-airflow[odbc] apache-airflow-providers-odbc
