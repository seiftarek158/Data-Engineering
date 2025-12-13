FROM apache/airflow:2.10.3

# Switch to root to install Java and download JDBC driver
USER root

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create jars directory and download PostgreSQL JDBC driver
RUN mkdir -p /opt/airflow/jars && \
    curl -o /opt/airflow/jars/postgresql-42.7.1.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.1.jar

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow

# Install Python dependencies with increased timeout and retries
COPY requirements.txt /
RUN pip install --default-timeout=100 --retries 5 -r /requirements.txt
