# Streamlit Dashboard Testing Guide

## 🚀 Accessing the Dashboard

The Streamlit dashboard is now running at:
**http://localhost:8501**

Open this URL in your web browser.

---

## 📋 Testing the AI Agent Flow

### Step 1: Configure the LLM
1. In the sidebar, you'll see "AI Configuration"
2. Select **"Google Gemini"** from the dropdown
3. Enter your Gemini API Key: `AIzaSyBDxqVxxnLSSoslgtICMM7odWBi9fLKYyM`
4. Select Model: **gemini-2.5-flash** (recommended for speed)
5. You should see "✅ gemini-2.5-flash initialized"

### Step 2: View Data
The main dashboard shows:
- **Recent Trades** table (from PostgreSQL)
- **Trade Price vs Quantity** scatter plot
- Real-time data from your `Trades_Database`

### Step 3: Test AI Natural Language Queries

In the "🤖 AI Analyst" section on the right, try these queries:

#### Basic Queries:
```
What is the total trading volume for all stocks?
```

```
Show me the top 5 customers by trade amount
```

```
What is the average trade size for retail accounts?
```

#### Complex Queries:
```
Which stock sector has the highest total transaction value?
```

```
Show me all transactions that occurred on weekends
```

```
What percentage of trades were buy orders versus sell orders?
```

#### Customer Analysis:
```
Show me all transactions for customer ID 4747
```

```
How many unique customers made transactions?
```

### Step 4: Review Results

For each query, the dashboard will:
1. **Generate SQL** - Shows the SQL query created by Gemini
2. **Execute Query** - Runs it against your PostgreSQL database
3. **Display Results** - Shows the data in a table or as text

---

## 🔄 Complete Testing Workflow

### Method 1: Dashboard Testing (Interactive)
1. Open http://localhost:8501
2. Enter API key in sidebar
3. Type natural language questions
4. See SQL generation + results in real-time

### Method 2: Airflow DAG Testing (Automated)
1. Open http://localhost:8085
2. Enable `stage_6_ai_agent` DAG
3. Trigger manually
4. Check logs at: `Airflow/dags/agent_logs/2025/`
5. Results saved to:
   - JSON files: `agent_volume_*.json`
   - PostgreSQL: `agent_query_results` table

---

## 📊 Viewing Saved Agent Results

### From Airflow DAG Execution:
```sql
-- Connect to PostgreSQL and run:
SELECT * FROM agent_query_results 
ORDER BY timestamp DESC 
LIMIT 10;
```

### Access via pgAdmin:
1. Open http://localhost:5050
2. Login: admin@admin.com / admin
3. Add Server:
   - Host: `pgdatabase`
   - Port: `5432`
   - Database: `Trades_Database`
   - Username: `postgres`
   - Password: `postgres`
4. Navigate to: Trades_Database → Schemas → public → Tables → agent_query_results

---

## 🎯 Assignment Requirements Checklist

✅ **Stage 6 DAG** - Automated AI agent processing 15 test queries
✅ **Streamlit Dashboard** - Interactive natural language SQL interface  
✅ **Gemini Integration** - Using Google Gemini API instead of OpenAI
✅ **Database Connection** - Queries PostgreSQL `Trades_Database`
✅ **Result Storage** - Saves to JSON + PostgreSQL table
✅ **Documentation** - Complete setup and testing guides

---

## 🐛 Troubleshooting

### Dashboard won't load
```bash
# Check if container is running
docker ps | grep streamlit-dashboard

# View logs
docker logs streamlit-dashboard
```

### Database connection failed
- Ensure `pgdatabase` container is running
- Check connection string in dashboard logs
- Verify `final_stocks` table exists in Trades_Database

### API Key Issues
- Gemini free tier: 5 requests per minute
- If you see rate limit errors, wait 60 seconds between queries
- Or upgrade to paid tier at: https://ai.google.dev/pricing

### No data in dashboard
```bash
# Check if Stage 2 DAG populated the database
docker exec -it trades_data_warehouse_postgres psql -U postgres -d Trades_Database -c "SELECT COUNT(*) FROM final_stocks;"
```

---

## 📝 Important Notes

1. **API Rate Limits**: Gemini free tier allows 5 requests/minute. The JSON log shows some queries failed due to quota exhaustion. Space out your queries or wait between batches.

2. **Data Source**: Dashboard queries the same `final_stocks` table that Stage 2 DAG populated.

3. **Real-time Updates**: Refresh the dashboard page to see new data after running Airflow DAGs.

4. **Multiple LLM Options**: Dashboard supports:
   - Google Gemini (configured)
   - OpenAI (requires API key)
   - Ollama (local, no API key needed)

---

## 🎓 Demo Flow for Assignment

1. Show **Airflow DAG** execution (http://localhost:8085)
   - Run `stage_6_ai_agent` DAG
   - Show successful execution in logs

2. Show **JSON Output** 
   - Open `agent_logs/2025/agent_volume_*.json`
   - Display 15 queries with SQL + results

3. Show **Database Storage**
   - Query `agent_query_results` table in pgAdmin

4. Show **Interactive Dashboard** (http://localhost:8501)
   - Enter Gemini API key
   - Run live natural language queries
   - Show SQL generation + results

5. Explain **Architecture**:
   - Airflow orchestrates batch processing
   - Streamlit provides interactive interface
   - Both use same Gemini LLM
   - PostgreSQL stores all results
