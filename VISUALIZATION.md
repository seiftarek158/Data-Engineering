# Dashboard Access Guide

## How to Access the Dashboard

**Dashboard URL:** `http://localhost:8502`

---

## Prerequisites

1. **Start Docker Services:**
   ```powershell
   docker-compose up -d
   ```

2. **Run the Pipeline:**
   - Open Airflow UI: `http://localhost:8085`
   - Trigger DAG: `stock_portfolio_pipeline`
   - Wait for Stage 4 and Stage 5 to complete

---

## Access Methods

### Method 1: Docker Compose (Recommended)

```powershell
# Start dashboard with all services
docker-compose up -d

# Verify dashboard is running
docker-compose ps | Select-String "dashboard"
```

Then open in browser: `http://localhost:8502`

### Method 2: Local Python

```powershell
# Navigate to dags directory
cd c:\Users\Omar-Harridy\coding\Data-Engineering\Airflow\dags

# Run dashboard
streamlit run dashboard.py --server.port 8502
```

Then open in browser: `http://localhost:8502`

---

**Note:** Ensure the Airflow pipeline has completed successfully before accessing the dashboard.

