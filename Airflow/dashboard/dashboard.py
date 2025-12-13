import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

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
    st.markdown("Ask questions about your data.")
    
    user_query = st.text_area("Question:", placeholder="e.g., What is the total trading volume by sector?")
    
    if st.button("Analyze"):
        if llm and LANGCHAIN_AVAILABLE:
            with st.spinner("Analyzing..."):
                try:
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
                        
                except Exception as e:
                    st.error(f"Analysis failed: {e}")
                    st.error(f"Details: {str(e)}")
        else:
            st.warning("Please configure the LLM in the sidebar first.")