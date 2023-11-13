# Use the Airflow image as the base
FROM apache/airflow:2.7.3

# Switch back to the airflow user
USER airflow

# Install a new Python library
RUN pip install yfinance