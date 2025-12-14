📊 Stock Transaction Dataset
📘 Overview

This dataset contains detailed records of stock market transactions executed by customers. Each record represents a single transaction, capturing trade details, customer information, and contextual metadata such as date, sector, and market characteristics.

---

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for local development)
- PostgreSQL
- Apache Spark
- Apache Kafka

### 📦 How to Run the Pipeline

#### Method 1: Using Docker Compose (Recommended)

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

2. **Verify services are running:**
   ```bash
   docker-compose ps
   ```

3. **Check logs if needed:**
   ```bash
   docker-compose logs -f airflow-webserver
   ```

#### Method 2: Manual Setup

1. **Set up environment variables:**
   - Create a `.env` file in the project root
   - Add required database credentials and configuration

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Initialize Airflow database:**
   ```bash
   airflow db init
   ```

4. **Start Airflow webserver:**
   ```bash
   airflow webserver -p 8080
   ```

5. **Start Airflow scheduler (in a new terminal):**
   ```bash
   airflow scheduler
   ```

---

## 🌐 How to Access the Airflow UI

Once Airflow is running, access the web interface:

- **URL:** http://localhost:8080
- **Default Credentials:**
  - Username: `airflow`
  - Password: `airflow` (or as configured in your setup)

### Using the Airflow UI:

1. Navigate to the **DAGs** page
2. Find `stock_portfolio_pipeline`
3. Toggle the DAG to "ON" to enable it
4. Click the DAG name to view details
5. Click the **Play** button (▶️) to trigger a manual run
6. Monitor task progress in the **Graph View** or **Grid View**

---

## 📊 How to View the Dashboard

The Streamlit dashboard provides real-time visualization of your stock portfolio analytics.

### Access the Dashboard:

- **URL:** http://localhost:8502

### Prerequisites:
- Pipeline must have completed at least once (Stage 5 creates visualization tables)
- PostgreSQL database must be running

### Access Methods:

**Option 1: Using Docker Compose**
```bash
docker-compose up streamlit
```

**Option 2: Local Python**
```bash
streamlit run dashboard.py --server.port 8502
```

The dashboard displays:
- Portfolio performance metrics
- Stock price trends
- Transaction volume analysis
- Customer segmentation insights
- Sector and industry distributions
- Risk analytics
- Real-time market indicators

---

## 🤖 How to Test the AI Agent

The AI agent (Stage 6) processes portfolio data and provides intelligent insights.

### Testing the AI Agent:

1. **Trigger the Pipeline:**
   - Ensure the DAG has run successfully through Stage 5
   - Stage 6 will automatically execute after Stages 1-4 complete

2. **Monitor Agent Execution:**
   - Check Airflow UI for `stage_6.setup_agent_volume` task
   - Verify `stage_6.process_with_ai_agent` completes successfully
   - Review `stage_6.log_agent_responses` for output

3. **Check Agent Logs:**
   ```bash
   docker-compose logs ai-agent
   ```

4. **Review Agent Responses:**
   - Agent responses are logged in the PostgreSQL database
   - Check the `ai_agent_logs` table for insights

5. **Manual Testing:**
   ```bash
   # Access the AI agent container
   docker exec -it <ai-agent-container-name> bash
   
   # Run test queries
   python test_agent.py
   ```

### Expected Agent Capabilities:
- Portfolio risk assessment
- Trading pattern analysis
- Anomaly detection
- Predictive insights
- Natural language query responses

---

## 🏗️ Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          STOCK PORTFOLIO PIPELINE                        │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│   STAGE 1:      │     Raw Data → Cleaning → PostgreSQL
│ Data Cleaning   │     ├── Clean missing values
│                 │     ├── Detect outliers
│                 │     ├── Integrate datasets
│                 │     └── Load to PostgreSQL (pgdatabase)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   STAGE 2:      │     Data → Categorical Encoding → Streaming Prep
│ Data Encoding   │     ├── Prepare streaming data
│                 │     └── Encode categorical features
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   STAGE 3:      │     Kafka Stream → Processing → PostgreSQL
│ Kafka Streaming │     ├── Consume and process stream
│                 │     └── Save to PostgreSQL
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   STAGE 4:      │     PostgreSQL → Spark Analytics → Results
│ Spark Analytics │     ├── Initialize Spark session
│                 │     └── Run analytics (aggregations, trends)
└────────┬────────┘
         │
         ├─────────────────────────────┐
         │                             │
         ▼                             ▼
┌─────────────────┐          ┌─────────────────┐
│   STAGE 5:      │          │   STAGE 6:      │
│ Visualization   │          │   AI Agent      │
│                 │          │                 │
│ Create viz_*    │          │ ├── Setup volume│
│ tables for      │          │ ├── Process data│
│ dashboard       │          │ └── Log insights│
└────────┬────────┘          └─────────────────┘
         │
         ▼
┌─────────────────┐
│   Streamlit     │          Dashboard @ http://localhost:8502
│   Dashboard     │          
└─────────────────┘

### Data Flow:

1. **Stage 1:** Raw CSV files → Cleaned data in PostgreSQL
2. **Stage 2:** Encoded data → Kafka topics
3. **Stage 3:** Kafka streams → Processed data in PostgreSQL
4. **Stage 4:** PostgreSQL data → Spark analytics → Results
5. **Stage 5:** Spark results → 13 viz_* tables for dashboard
6. **Stage 6:** Data → AI agent analysis → Insights logged




## 🧾 Field Descriptions
Each column in the dataset is described below:
| **Column Name**         | **Description**                                                                                                                                                            | **Data Type**        | **Example Value** |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- | ----------------- |
| `transaction_id`        | A unique numeric identifier assigned to each transaction record.                                                                                                           | Integer              | 1                 |
| `timestamp`             | The date on which the stock transaction took place.                                                                                                                        | Date                 | 2023-01-02        |
| `customer_id`           | A unique identifier representing the customer executing the trade.                                                                                                         | Integer              | 4747              |
| `stock_ticker`          | The stock’s unique ticker symbol indicating which security was traded.                                                                                                     | String               | STK006            |
| `transaction_type`      | Specifies whether the transaction was a buy or sell action.                                                                                                                | Categorical (String) | BUY               |
| `quantity`              | The total number of stock units involved in the transaction.                                                                                                               | Integer              | 503               |
| `average_trade_size`    | The average quantity of shares traded per transaction for the customer and stock.                                                                                          | Float                | 195.33            |
| `stock_price`           | **Log-transformed stock price** at the time of transaction. The original stock price can be recovered using the exponential function: `original_price = exp(stock_price)`. | Float (Log scale)    | 4.917154          |
| `total_trade_amount`    | The total value of the transaction calculated as quantity × (original stock price).                                                                                        | Float                | 68716.45          |
| `customer_account_type` | The classification of the customer’s account, such as retail or institutional.                                                                                             | Categorical (String) | Retail            |
| `day_name`              | The day of the week on which the transaction occurred.                                                                                                                     | Categorical (String) | Monday            |
| `is_weekend`            | Indicates whether the transaction took place on a weekend.                                                                                                                 | Boolean              | False             |
| `is_holiday`            | Indicates whether the transaction occurred on a recognized public holiday.                                                                                                 | Boolean              | False             |
| `stock_liquidity_tier`  | The liquidity classification of the traded stock based on its trading volume or activity level.                                                                            | Categorical (String) | High              |
| `stock_sector`          | The broad economic sector to which the traded stock belongs.                                                                                                               | Categorical (String) | Energy            |
| `stock_industry`        | The specific industry category within the broader sector of the stock.                                                                                                     | Categorical (String) | Oil & Gas         |
