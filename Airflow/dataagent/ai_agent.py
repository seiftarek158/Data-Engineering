import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
import time
from datetime import datetime, timedelta
from collections import deque

# LangChain Imports
try:
    from langchain_community.utilities import SQLDatabase
    from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
    from langchain_openai import ChatOpenAI
    from langchain_community.llms import Ollama
    from langchain_google_genai import ChatGoogleGenerativeAI
    LANGCHAIN_AVAILABLE = True
except ImportError as e:
    st.error(f"Missing libraries! Error: {e}")
    st.error("Please ensure all packages from requirements.txt are installed.")
    LANGCHAIN_AVAILABLE = False

# Page Config
st.set_page_config(page_title="Stock Portfolio Analytics", layout="wide")

# -----------------------------------------------------------------------------
# Rate Limiter for Gemini API
# -----------------------------------------------------------------------------
class GeminiRateLimiter:
    """Rate limiter to respect Gemini API free tier limits."""
    
    def __init__(self, requests_per_minute=5, requests_per_day=20):
        self.rpm_limit = requests_per_minute
        self.rpd_limit = requests_per_day
        
        # Use session state to persist across Streamlit reruns
        if 'request_timestamps' not in st.session_state:
            st.session_state.request_timestamps = deque()
        if 'daily_request_count' not in st.session_state:
            st.session_state.daily_request_count = 0
        if 'daily_reset_time' not in st.session_state:
            st.session_state.daily_reset_time = datetime.now() + timedelta(days=1)
    
    def _clean_old_requests(self):
        """Remove requests older than 1 minute."""
        current_time = datetime.now()
        one_minute_ago = current_time - timedelta(minutes=1)
        
        while (st.session_state.request_timestamps and 
               st.session_state.request_timestamps[0] < one_minute_ago):
            st.session_state.request_timestamps.popleft()
    
    def _reset_daily_count(self):
        """Reset daily counter if 24 hours have passed."""
        if datetime.now() >= st.session_state.daily_reset_time:
            st.session_state.daily_request_count = 0
            st.session_state.daily_reset_time = datetime.now() + timedelta(days=1)
    
    def can_make_request(self):
        """Check if a request can be made without exceeding limits."""
        self._clean_old_requests()
        self._reset_daily_count()
        
        rpm_ok = len(st.session_state.request_timestamps) < self.rpm_limit
        rpd_ok = st.session_state.daily_request_count < self.rpd_limit
        
        return rpm_ok and rpd_ok
    
    def wait_if_needed(self):
        """Wait until a request can be made."""
        self._clean_old_requests()
        self._reset_daily_count()
        
        # Check daily limit first
        if st.session_state.daily_request_count >= self.rpd_limit:
            time_until_reset = (st.session_state.daily_reset_time - datetime.now()).total_seconds()
            return False, f"Daily limit reached ({self.rpd_limit} requests/day). Resets in {int(time_until_reset/3600)} hours."
        
        # Check minute limit
        if len(st.session_state.request_timestamps) >= self.rpm_limit:
            oldest_request = st.session_state.request_timestamps[0]
            wait_time = 60 - (datetime.now() - oldest_request).total_seconds()
            
            if wait_time > 0:
                return False, f"Rate limit reached. Please wait {int(wait_time)} seconds."
        
        return True, None
    
    def record_request(self):
        """Record a new request."""
        st.session_state.request_timestamps.append(datetime.now())
        st.session_state.daily_request_count += 1
    
    def get_status(self):
        """Get current rate limit status."""
        self._clean_old_requests()
        self._reset_daily_count()
        
        rpm_used = len(st.session_state.request_timestamps)
        rpd_used = st.session_state.daily_request_count
        
        return {
            'rpm': f"{rpm_used}/{self.rpm_limit}",
            'rpd': f"{rpd_used}/{self.rpd_limit}",
            'rpm_available': self.rpm_limit - rpm_used,
            'rpd_available': self.rpd_limit - rpd_used
        }

# Initialize rate limiter (only for Gemini)
rate_limiter = GeminiRateLimiter(requests_per_minute=5, requests_per_day=20)

# -----------------------------------------------------------------------------
# Database Connection
# -----------------------------------------------------------------------------
# Get connection details from Environment Variables (set in docker-compose) or use defaults
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost") # Defaults to localhost for local testing
DB_PORT = os.getenv("DB_PORT", "5454")      # Defaults to 5454 (external port) for local testing
DB_NAME = os.getenv("POSTGRES_DB", "Trades_Database")

# If running inside Docker (DB_HOST is likely 'pgdatabase'), the internal port is usually 5432
if DB_HOST == 'pgdatabase':
    DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_resource
def get_database_engine():
    return create_engine(DATABASE_URL)

try:
    engine = get_database_engine()
    db = SQLDatabase(engine)
    st.sidebar.success(f"Connected to DB at {DB_HOST}")
except Exception as e:
    st.sidebar.error(f"Database Connection Failed: {e}")
    st.stop()

# -----------------------------------------------------------------------------
# Sidebar Configuration
# -----------------------------------------------------------------------------
st.sidebar.header("AI Configuration")
llm_provider = st.sidebar.selectbox("Select LLM Provider", ["Google Gemini", "OpenAI", "Ollama (Local)"])

# Show rate limit status for Gemini
if llm_provider == "Google Gemini":
    status = rate_limiter.get_status()
    st.sidebar.markdown("### Rate Limits (Free Tier)")
    col_rpm, col_rpd = st.sidebar.columns(2)
    with col_rpm:
        st.metric("Requests/Min", status['rpm'])
    with col_rpd:
        st.metric("Requests/Day", status['rpd'])
    
    # Color-coded status
    if status['rpm_available'] == 0:
        st.sidebar.error("⚠️ Minute limit reached")
    elif status['rpm_available'] <= 1:
        st.sidebar.warning("⚠️ Almost at minute limit")
    
    if status['rpd_available'] <= 2:
        st.sidebar.warning("⚠️ Approaching daily limit")

llm = None
if llm_provider == "Google Gemini":
    api_key = st.sidebar.text_input("Gemini API Key", type="password", 
                                     help="Get your key from: https://makersuite.google.com/app/apikey")
    model = st.sidebar.selectbox("Model", ["gemini-2.5-flash", "gemini-1.5-flash", "gemini-1.5-pro"], index=0)
    if api_key:
        try:
            llm = ChatGoogleGenerativeAI(model=model, temperature=0, api_key=api_key)
            st.sidebar.success(f"✅ {model} initialized")
        except Exception as e:
            st.sidebar.error(f"Failed to initialize Gemini: {e}")
elif llm_provider == "OpenAI":
    api_key = st.sidebar.text_input("OpenAI API Key", type="password")
    if api_key:
        llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0, openai_api_key=api_key)
elif llm_provider == "Ollama (Local)":
    # If running in Docker, 'localhost' refers to the container itself. 
    # To reach Ollama on the host, you might need 'host.docker.internal' or the specific IP.
    base_url = st.sidebar.text_input("Base URL", value="http://host.docker.internal:11434")
    model_name = st.sidebar.text_input("Model Name", value="llama2")
    
    if model_name:
        try:
            llm = Ollama(model=model_name, base_url=base_url)
        except:
            st.sidebar.warning("Could not initialize Ollama.")

# -----------------------------------------------------------------------------
# Main Dashboard
# -----------------------------------------------------------------------------
st.title("Real-Time Stock Portfolio Analytics 📈")

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📊 Market Overview")
    st.markdown("### Recent Trades")
    try:
        query = "SELECT * FROM final_stocks LIMIT 500"
        df_trades = pd.read_sql(query, engine)
        
        if not df_trades.empty:
            st.dataframe(df_trades, use_container_width=True, height=300)
            
            if 'quantity' in df_trades.columns and 'stock_price' in df_trades.columns:
                fig = px.scatter(df_trades, x='quantity', y='stock_price', color='transaction_type', 
                                 title="Trade Price vs Quantity")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data found in 'trades' table.")
            
    except Exception as e:
        st.error(f"Error fetching data: {e}")

with col2:
    st.subheader("🤖 AI Analyst")
    
    # Tabs for single query vs batch processing
    tab1, tab2 = st.tabs(["Single Query", "Batch Processing"])
    
    with tab1:
        st.markdown("Ask questions about your data.")
        user_query = st.text_area("Question:", placeholder="e.g., What is the total trading volume by sector?")
        
        if st.button("Analyze", key="single_analyze"):
            if llm and LANGCHAIN_AVAILABLE:
                # Check rate limits for Gemini
                if llm_provider == "Google Gemini":
                    can_proceed, error_msg = rate_limiter.wait_if_needed()
                    if not can_proceed:
                        st.error(error_msg)
                        st.info("💡 Tip: Consider upgrading to a paid plan or using a different LLM provider for higher limits.")
                        st.stop()
                
                with st.spinner("Analyzing..."):
                    try:
                        # Record the request for Gemini
                        if llm_provider == "Google Gemini":
                            rate_limiter.record_request()
                        
                        # Get table information
                        table_info = db.get_table_info()
                        
                        # Create prompt with table schema
                        prompt = f"""You are a SQL expert. Given the following database schema:

    {table_info}

    Generate a SQL query to answer the following question. Return only the SQL query without any markdown formatting or explanations.

    Question: {user_query}

    SQL Query:"""
                        
                        # Generate SQL using LLM
                        sql_query = llm.invoke(prompt).content if hasattr(llm.invoke(prompt), 'content') else str(llm.invoke(prompt))
                        
                        st.markdown("**Generated SQL:**")
                        st.code(sql_query, language="sql")
                        
                        cleaned_sql = sql_query.strip().replace('```sql', '').replace('```', '').strip()
                        
                        if cleaned_sql.lower().startswith("select"):
                            result_df = pd.read_sql(cleaned_sql, engine)
                            st.markdown("**Result:**")
                            st.dataframe(result_df)
                        else:
                            execute_query = QuerySQLDataBaseTool(db=db)
                            result = execute_query.invoke(cleaned_sql)
                            st.write(result)
                        
                        # Show updated rate limit status
                        if llm_provider == "Google Gemini":
                            status = rate_limiter.get_status()
                            st.success(f"✅ Query completed. Remaining today: {status['rpd_available']} requests")
                            
                    except Exception as e:
                        st.error(f"Analysis failed: {e}")
                        st.error(f"Details: {str(e)}")
        else:
            st.warning("Please configure the LLM in the sidebar first.")
    
    with tab2:
        st.markdown("Process multiple queries automatically with rate limiting.")
        
        # Batch input methods
        batch_input_method = st.radio("Input Method:", ["Text Area", "Upload File"], horizontal=True)
        
        queries_list = []
        
        if batch_input_method == "Text Area":
            batch_queries = st.text_area(
                "Enter queries (one per line):",
                placeholder="What is the total trading volume?\nShow top 10 customers\nWhat is average trade size?",
                height=150
            )
            if batch_queries:
                queries_list = [q.strip() for q in batch_queries.split('\n') if q.strip()]
        else:
            uploaded_file = st.file_uploader("Upload text file with queries", type=['txt'])
            if uploaded_file:
                queries_list = [q.strip() for q in uploaded_file.read().decode('utf-8').split('\n') if q.strip()]
        
        if queries_list:
            st.info(f"📋 {len(queries_list)} queries loaded")
            
            # Calculate estimated time for Gemini
            if llm_provider == "Google Gemini" and queries_list:
                status = rate_limiter.get_status()
                available_rpm = status['rpm_available']
                available_rpd = status['rpd_available']
                
                can_process = min(len(queries_list), available_rpd)
                batches_needed = (can_process + available_rpm - 1) // available_rpm  # Ceiling division
                estimated_time = batches_needed * 60  # seconds
                
                st.warning(f"⏱️ Estimated time: ~{estimated_time // 60} min {estimated_time % 60} sec (respecting 5 RPM limit)")
                st.info(f"Will process: {can_process}/{len(queries_list)} queries (daily limit: {available_rpd})")
                
                if can_process < len(queries_list):
                    st.error(f"⚠️ Cannot process all queries due to daily limit. {len(queries_list) - can_process} will be skipped.")
        
        col_batch_1, col_batch_2 = st.columns(2)
        with col_batch_1:
            process_batch = st.button("🚀 Process Batch", type="primary", disabled=not queries_list)
        with col_batch_2:
            save_results = st.checkbox("Save results to CSV", value=True)
        
        if process_batch and queries_list:
            if llm and LANGCHAIN_AVAILABLE:
                # Initialize results storage
                if 'batch_results' not in st.session_state:
                    st.session_state.batch_results = []
                
                st.session_state.batch_results = []
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                results_container = st.container()
                
                successful_queries = 0
                failed_queries = 0
                skipped_queries = 0
                
                for idx, query in enumerate(queries_list):
                    # Check rate limits for Gemini
                    if llm_provider == "Google Gemini":
                        can_proceed, error_msg = rate_limiter.wait_if_needed()
                        
                        if not can_proceed:
                            status_text.warning(f"⏸️ Query {idx + 1}/{len(queries_list)}: {error_msg}")
                            
                            # If daily limit reached, skip remaining
                            if "Daily limit" in error_msg:
                                skipped_queries = len(queries_list) - idx
                                st.error(f"❌ Daily limit reached. Skipping {skipped_queries} remaining queries.")
                                break
                            
                            # Wait for RPM limit
                            wait_time = int(error_msg.split()[-2]) if "wait" in error_msg else 12
                            time.sleep(wait_time + 1)
                            can_proceed, _ = rate_limiter.wait_if_needed()
                    
                    status_text.info(f"⚙️ Processing query {idx + 1}/{len(queries_list)}: {query[:50]}...")
                    
                    try:
                        # Record the request for Gemini
                        if llm_provider == "Google Gemini":
                            rate_limiter.record_request()
                        
                        # Get table information
                        table_info = db.get_table_info()
                        
                        # Create prompt
                        prompt = f"""You are a SQL expert. Given the following database schema:

{table_info}

Generate a SQL query to answer the following question. Return only the SQL query without any markdown formatting or explanations.

Question: {query}

SQL Query:"""
                        
                        # Generate SQL using LLM
                        response = llm.invoke(prompt)
                        sql_query = response.content if hasattr(response, 'content') else str(response)
                        cleaned_sql = sql_query.strip().replace('```sql', '').replace('```', '').strip()
                        
                        # Execute query
                        result_data = None
                        if cleaned_sql.lower().startswith("select"):
                            result_df = pd.read_sql(cleaned_sql, engine)
                            result_data = result_df.to_dict('records')
                        
                        # Store result
                        st.session_state.batch_results.append({
                            'query_num': idx + 1,
                            'question': query,
                            'sql': cleaned_sql,
                            'status': 'Success',
                            'result_rows': len(result_data) if result_data else 0,
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        
                        successful_queries += 1
                        
                    except Exception as e:
                        st.session_state.batch_results.append({
                            'query_num': idx + 1,
                            'question': query,
                            'sql': 'Error',
                            'status': f'Failed: {str(e)[:100]}',
                            'result_rows': 0,
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        failed_queries += 1
                    
                    # Update progress
                    progress_bar.progress((idx + 1) / len(queries_list))
                    
                    # Add delay between requests for Gemini (rate limiting)
                    if llm_provider == "Google Gemini" and idx < len(queries_list) - 1:
                        time.sleep(12)  # 12 seconds = 5 requests per minute
                
                # Show results summary
                status_text.empty()
                progress_bar.empty()
                
                st.success(f"✅ Batch processing complete!")
                col_sum_1, col_sum_2, col_sum_3 = st.columns(3)
                with col_sum_1:
                    st.metric("Successful", successful_queries)
                with col_sum_2:
                    st.metric("Failed", failed_queries)
                with col_sum_3:
                    st.metric("Skipped", skipped_queries)
                
                # Display results
                if st.session_state.batch_results:
                    results_df = pd.DataFrame(st.session_state.batch_results)
                    st.dataframe(results_df, use_container_width=True)
                    
                    # Save to CSV
                    if save_results:
                        csv_filename = f"batch_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                        results_df.to_csv(csv_filename, index=False)
                        st.success(f"💾 Results saved to {csv_filename}")
                
                # Show updated rate limit status
                if llm_provider == "Google Gemini":
                    status = rate_limiter.get_status()
                    st.info(f"📊 Remaining today: {status['rpd_available']} requests | This minute: {status['rpm_available']} requests")
                    
            else:
                st.warning("Please configure the LLM in the sidebar first.")
