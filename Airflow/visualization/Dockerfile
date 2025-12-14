FROM python:3.12-slim

WORKDIR /app

# Install system dependencies including curl for health checks
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the dashboard script
COPY visualization_dashboard.py .

# Expose Streamlit port
EXPOSE 8501

# Run Streamlit (will be controlled by restart policy)
CMD ["streamlit", "run", "visualization_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
