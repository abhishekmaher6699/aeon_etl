FROM apache/airflow:2.10.3-python3.8

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt