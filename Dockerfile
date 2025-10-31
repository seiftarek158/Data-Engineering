FROM python:3.12-slim

# Install PostgreSQL development packages
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --default-timeout=1000 --no-cache-dir -r requirements.txt


ENTRYPOINT ["python", "src/app.py"]