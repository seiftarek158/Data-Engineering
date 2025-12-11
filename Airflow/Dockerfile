FROM apache/airflow:2.10.3
USER airflow 
COPY docker-compose-app.yml /opt/airflow/docker-compose-app.yml
COPY requirements.txt /
RUN pip install -r /requirements.txt
