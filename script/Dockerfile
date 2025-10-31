FROM apache/airflow:2.6.0-python3.9

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    apt-get clean

USER airflow

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --user -r /requirements.txt
