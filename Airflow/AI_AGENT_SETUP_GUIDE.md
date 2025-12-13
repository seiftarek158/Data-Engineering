# AI Agent Setup & Testing Guide

## Overview
This guide explains how to set up, run, and test the Stage 6 AI Agent for natural language SQL querying using Google Gemini.

---

## Prerequisites

### 1. Get Gemini API Key
1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Sign in with your Google account
3. Click "Create API Key"
4. Copy the generated API key

### 2. Configure Environment Variables
Edit `Airflow/dags/.env` and add your Gemini API key:

```env
GEMINI_API_KEY=your-actual-api-key-here
```

---

## Installation & Setup

### Step 1: Rebuild Docker Containers

Since we added new dependencies (`langchain-google-genai` and `google-generativeai`), you need to rebuild:

```powershell
# Navigate to Airflow directory
cd "e:\Semester9\Data engineering\Milestone_1\Airflow"

# Stop existing containers
docker-compose down

# Rebuild containers with new dependencies
docker-compose build

# Start all services
docker-compose up -d
```

**Wait 2-3 minutes** for all services to initialize properly.

### Step 2: Verify Services

Check that all services are running:

```powershell
docker-compose ps
```

You should see:
- ✅ airflow-webserver (port 8085)
- ✅ airflow-scheduler
- ✅ airflow-worker
- ✅ pgdatabase (port 5454)
- ✅ kafka (port 9092)
- ✅ spark-master (port 8080)

---

## Running Stage 6 AI Agent

### Option 1: Via Airflow Web UI (Recommended)

1. **Access Airflow UI**
   - Open browser: http://localhost:8085
   - Login: `airflow` / `airflow`

2. **Enable the DAG**
   - Find `stage_6_ai_agent` in the DAG list
   - Toggle the switch to enable it
   - Tags: `stage_6`, `ai_agent`, `langchain`, `gemini`

3. **Trigger the DAG**
   - Click the "Play" button (▶️) on the right
   - Select "Trigger DAG"
   - Watch the task progression in real-time

4. **Monitor Execution**
   - Click on the DAG name to see the Graph view
   - Tasks should execute in order:
     1. `setup_agent_volume` (creates directories and test queries)
     2. `process_with_ai_agent` (executes 15 natural language queries)
     3. `log_agent_responses` (saves results to JSON and PostgreSQL)

5. **View Logs**
   - Click on any task box
   - Select "Log" to see detailed execution output
   - Look for:
     - ✅ Gemini LLM initialization
     - 📝 Query processing status
     - 📊 Generated SQL queries
     - ✅ Success/failure counts

### Option 2: Via Airflow CLI

```powershell
# Access Airflow container
docker exec -it <airflow-webserver-container-id> bash

# Trigger the DAG
airflow dags trigger stage_6_ai_agent

# Check DAG status
airflow dags list-runs -d stage_6_ai_agent
```

---

## Verifying Results

### 1. Check Log Files

**Location**: `/opt/airflow/dags/agent_logs/2025/agent_volume_<timestamp>.json`

**From Host Machine**:
The logs are inside the Docker container. To access them:

```powershell
# List containers
docker ps

# Access airflow-webserver container
docker exec -it <airflow-webserver-container-id> bash

# Navigate to logs
cd /opt/airflow/dags/agent_logs/2025/

# View the latest log file
cat agent_volume_*.json | tail -100
```

**Expected JSON Structure**:
```json
{
  "execution_date": "2025-12-12T...",
  "dag_run_id": "manual__2025-12-12T...",
  "total_queries": 15,
  "queries": [
    {
      "query_number": 1,
      "user_query": "What was the total trading volume for technology stocks last month?",
      "sql_generated": "SELECT SUM(quantity) FROM cleaned_trades WHERE ...",
      "row_count": 5,
      "agent_response": "[{...}]",
      "status": "success",
      "timestamp": "2025-12-12T..."
    }
  ]
}
```

### 2. Check PostgreSQL Table

The agent also saves results to a PostgreSQL table for easy querying.

**Using pgAdmin** (http://localhost:8090):
1. Login: `postgres@postgres.com` / `postgres`
2. Connect to server:
   - Host: `pgdatabase`
   - Port: `5432`
   - Database: `Trades_Database`
3. Run query:

```sql
SELECT 
    query_number,
    user_query,
    sql_generated,
    row_count,
    status,
    timestamp
FROM agent_query_results
ORDER BY timestamp DESC
LIMIT 20;
```

**Using Adminer** (http://localhost:8070):
1. System: PostgreSQL
2. Server: `pgdatabase:5432`
3. Username: `postgres`
4. Password: `postgres`
5. Database: `Trades_Database`
6. Execute the same query above

### 3. Check Test Queries File

The agent creates a file with predefined test queries:

**Location**: `/opt/airflow/dags/agents/user_query_test.txt`

**15 Test Queries**:
1. "What was the total trading volume for technology stocks last month?"
2. "Show me the top 10 customers by trade amount"
3. "What is the average trade size for retail accounts?"
4. "How many transactions occurred on weekends?"
5. "Which stock sector has the highest liquidity?"
6. "Show me all transactions for customer with ID 4747"
7. "What are the total buy versus sell transaction amounts?"
8. "Which day of the week has the most trading activity?"
9. "Show me all stocks in the high liquidity tier"
10. "What is the transaction count by customer account type?"
11. "Which stock ticker had the highest trade volume?"
12. "What percentage of trades happened during holidays?"
13. "Show me the average stock price by sector"
14. "How many unique customers made transactions?"
15. "What is the total trade amount by transaction type?"

---

## Testing the Dashboard with Gemini

The dashboard now supports Google Gemini alongside OpenAI and Ollama!

### Step 1: Start Dashboard (if not already in docker-compose)

**Note**: The dashboard service is not yet in `docker-compose.yaml`. You can either:

**Option A: Add to docker-compose.yaml**

Add this service to `Airflow/docker-compose.yaml`:

```yaml
  dashboard:
    build:
      context: ./dashboard
      dockerfile: Dockerfile
    container_name: streamlit-dashboard
    ports:
      - "8501:8501"
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - DB_HOST=pgdatabase
      - DB_PORT=5432
      - POSTGRES_DB=Trades_Database
    depends_on:
      - pgdatabase
    restart: unless-stopped
```

Then rebuild:
```powershell
docker-compose down
docker-compose build
docker-compose up -d
```

**Option B: Run Dashboard Locally**

```powershell
# Navigate to dashboard directory
cd "e:\Semester9\Data engineering\Milestone_1\Airflow\dashboard"

# Install requirements
pip install -r requirements.txt

# Set environment variables for local connection
$env:DB_HOST="localhost"
$env:DB_PORT="5454"
$env:POSTGRES_DB="Trades_Database"
$env:POSTGRES_USER="postgres"
$env:POSTGRES_PASSWORD="postgres"

# Run streamlit
streamlit run dashboard.py
```

### Step 2: Use Dashboard

1. **Open Dashboard**: http://localhost:8501

2. **Configure AI Settings** (Left Sidebar):
   - Select LLM Provider: **Google Gemini**
   - Enter your Gemini API Key
   - Select Model: `gemini-2.5-flash` (recommended for speed)
   - You should see: ✅ gemini-2.5-flash initialized

3. **Ask Questions**:
   - In the "AI Analyst" section on the right
   - Type a natural language question, e.g.:
     - "What is the total trading volume by sector?"
     - "Show me the top 5 customers by trade amount"
   - Click "Analyze"

4. **View Results**:
   - Generated SQL will be displayed
   - Query results shown as a dataframe

---

## Troubleshooting

### Error: "GEMINI_API_KEY not found"

**Solution**: 
- Check `.env` file has the correct API key
- Restart containers: `docker-compose restart`
- Verify environment variables inside container:
  ```powershell
  docker exec -it <container-id> env | grep GEMINI
  ```

### Error: "Database Connection Failed"

**Solution**:
- Ensure `pgdatabase` service is running: `docker-compose ps`
- Check database logs: `docker-compose logs pgdatabase`
- Verify database is accessible:
  ```powershell
  docker exec -it <pgdatabase-container-id> psql -U postgres -d Trades_Database -c "\dt"
  ```

### Error: "No module named 'langchain_google_genai'"

**Solution**:
- Requirements not installed properly
- Rebuild containers:
  ```powershell
  docker-compose down
  docker-compose build --no-cache
  docker-compose up -d
  ```

### Agent Returns No Results

**Solution**:
- Check if `cleaned_trades` table has data:
  ```sql
  SELECT COUNT(*) FROM cleaned_trades;
  ```
- Ensure earlier stages (1-4) have run successfully
- Run the complete pipeline first:
  - `stock_portfolio_pipeline_55_0654` DAG

### Rate Limit Errors from Gemini API

**Solution**:
- Free tier has limits: 15 requests per minute
- The agent includes 1-second delays between queries
- If still hitting limits, increase delay in `Stage_6.py`:
  ```python
  time.sleep(2)  # Change from 1 to 2 seconds
  ```

---

## Success Criteria

Your Stage 6 AI Agent is working correctly if:

✅ All 3 tasks complete successfully in Airflow UI (green boxes)
✅ Log file created in `/opt/airflow/dags/agent_logs/2025/`
✅ 15 queries processed (check log file)
✅ At least 12/15 queries have `status: "success"` (80% success rate)
✅ `agent_query_results` table contains records in PostgreSQL
✅ Generated SQL queries are syntactically correct
✅ Dashboard can execute natural language queries with Gemini

---

## Next Steps

### Integration with Main Pipeline

To run Stage 6 automatically after Stage 4, update `stock_portfolio_pipeline_55_0654.py`:

```python
from airflow.sensors.external_task import ExternalTaskSensor

# Add this task
wait_for_stage_6 = ExternalTaskSensor(
    task_id='wait_for_stage_6',
    external_dag_id='stage_6_ai_agent',
    external_task_id=None,  # Wait for entire DAG
    timeout=600,
    poke_interval=30,
)

# Update dependencies
run_stage_4 >> wait_for_stage_6
```

### Custom Test Queries

To add your own test queries, modify `Stage_6.py`:

```python
test_queries = [
    "Your custom query here",
    "Another custom query",
    # ... existing queries ...
]
```

---

## Assignment Deliverables Checklist

For Milestone 3 submission:

- [ ] **Stage_6.py** - AI agent DAG implementation
- [ ] **agent_volume.json** - Log file with agent responses
- [ ] **agent_query_results** - PostgreSQL table with results
- [ ] **Screenshots**:
  - [ ] Airflow DAG view showing successful Stage 6 run
  - [ ] Task logs showing Gemini initialization and query processing
  - [ ] pgAdmin query showing `agent_query_results` table
  - [ ] Dashboard with Gemini configured and working query
- [ ] **Documentation**: This guide explaining setup and testing

---

## Bonus Features (Extra Credit)

If implementing bonus features from the assignment:

### 1. Task Groups for Each Query
Convert the single `process_with_ai_agent` task into a TaskGroup with 15 individual tasks.

### 2. Chronological Prompt Connections
Chain queries so each query's results inform the next query's prompt.

### 3. Access Spark Analytics Tables
Modify the agent to include Spark analytics tables (`spark_analytics_1-5`) for enhanced context.

---

## Support

If you encounter issues:
1. Check Airflow task logs (most detailed information)
2. Review container logs: `docker-compose logs <service-name>`
3. Verify all prerequisites are met
4. Ensure previous stages (1-4) have run successfully
5. Test Gemini API key separately: https://makersuite.google.com/app/prompts/new_freeform

---

**Last Updated**: December 12, 2025
