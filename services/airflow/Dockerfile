FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# On repart de la racine du projet
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
