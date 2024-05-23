FROM apache/airflow:2.9.1

USER airflow
COPY requirements.txt .

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt --no-cache-dir