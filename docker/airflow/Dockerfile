FROM apache/airflow:2.8.1-python3.8

USER root

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
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install core packages first
RUN pip install --no-cache-dir \
    pyodbc==4.0.39 \
    pandas==2.0.3

# Try to install apache-airflow-providers-microsoft-mssql without pymssql dependency
RUN pip install --no-cache-dir apache-airflow-providers-microsoft-mssql==3.4.0 || \
    pip install --no-cache-dir --no-deps apache-airflow-providers-microsoft-mssql==3.4.0