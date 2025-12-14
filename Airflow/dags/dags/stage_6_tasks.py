"""
Stage 6: AI Agent Query Processing Task Functions
==================================================

This module contains all task functions for Stage 6 of the pipeline:
- setup_agent_volume_task: Create agent volume directory and test queries file
- process_with_ai_agent_task: Initialize LangChain agent and process test queries
- log_agent_responses_task: Log agent results to JSON file and PostgreSQL
"""

import os
from datetime import datetime


def setup_agent_volume(**context):
    """Create agent volume directory and test queries file"""
    import json
    from pathlib import Path
    
    print("="*70)
    print("STAGE 6 TASK 1: SETUP AGENT VOLUME")
    print("="*70)
    
    agents_dir = Path('/opt/airflow/dags/agents')
    logs_dir = Path('/opt/airflow/dags/agent_logs')
    
    agents_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    test_queries = [
        "What was the total trading volume for technology stocks last month?",
        "Show me the top 10 customers by trade amount",
        "What is the average trade size for retail accounts?",
        "How many transactions occurred on weekends?",
        "Which stock sector has the highest liquidity?",
        "Show me all transactions for customer with ID 4747",
        "What are the total buy versus sell transaction amounts?",
        "Which day of the week has the most trading activity?",
        "Show me all stocks in the high liquidity tier",
        "What is the transaction count by customer account type?",
        "Which stock ticker had the highest trade volume?",
        "What percentage of trades happened during holidays?",
        "Show me the average stock price by sector",
        "How many unique customers made transactions?",
        "What is the total trade amount by transaction type?"
    ]
    
    queries_file = agents_dir / 'user_query_test.txt'
    with open(queries_file, 'w') as f:
        for query in test_queries:
            f.write(query + '\n')
    
    print(f"✅ Created agents directory: {agents_dir}")
    print(f"✅ Created test queries file with {len(test_queries)} questions")
    
    context['ti'].xcom_push(key='queries_file', value=str(queries_file))
    context['ti'].xcom_push(key='logs_dir', value=str(logs_dir))
    
    print("✓ STAGE 6 TASK 1 COMPLETED")


def process_with_ai_agent(**context):
    """Initialize LangChain agent and process test queries"""
    import time
    import pandas as pd
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_community.utilities import SQLDatabase
    from sqlalchemy import create_engine
    
    print("="*70)
    print("STAGE 6 TASK 2: PROCESS WITH AI AGENT")
    print("="*70)
    
    ti = context['ti']
    queries_file = ti.xcom_pull(task_ids='stage_6_ai_agent.setup_agent_volume', key='queries_file')
    
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    if not gemini_api_key:
        raise ValueError("GEMINI_API_KEY not found in environment variables")
    
    DB_HOST = os.getenv('DB_HOST', 'pgdatabase')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'Trades_Database')
    
    print("🔧 Initializing database connection...")
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_string)
    db = SQLDatabase(engine)
    
    print("🤖 Initializing Gemini LLM...")
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0,
        google_api_key=gemini_api_key
    )
    
    print("⛓️ Getting database schema...")
    table_info = db.get_table_info()
    
    with open(queries_file, 'r') as f:
        queries = [line.strip() for line in f if line.strip()]
    
    print(f"📝 Processing {len(queries)} test queries...")
    
    results = []
    for idx, question in enumerate(queries, 1):
        try:
            print(f"\n🔍 Query {idx}/{len(queries)}: {question}")
            
            prompt = f"""Given the following database schema:
{table_info}

Generate a SQL query to answer this question: {question}

Return ONLY the SQL query without any explanation or markdown formatting."""
            
            response = llm.invoke(prompt)
            sql_query = response.content if hasattr(response, 'content') else str(response)
            cleaned_sql = sql_query.strip().replace('```sql', '').replace('```', '').strip()
            
            print(f"📊 Generated SQL: {cleaned_sql[:100]}...")
            
            if cleaned_sql.lower().startswith("select"):
                result_df = pd.read_sql(cleaned_sql, engine)
                response = result_df.to_dict('records')
                row_count = len(result_df)
                print(f"✅ Query returned {row_count} rows")
            else:
                response = "Non-SELECT query generated"
                row_count = 0
            
            results.append({
                'query_number': idx,
                'user_query': question,
                'sql_generated': cleaned_sql,
                'row_count': row_count,
                'agent_response': str(response)[:500],
                'status': 'success',
                'timestamp': datetime.now().isoformat()
            })
            
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Error processing query {idx}: {str(e)}")
            results.append({
                'query_number': idx,
                'user_query': question,
                'sql_generated': 'ERROR',
                'row_count': 0,
                'agent_response': f'Error: {str(e)}',
                'status': 'failed',
                'timestamp': datetime.now().isoformat()
            })
    
    ti.xcom_push(key='agent_results', value=results)
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    print(f"\n📊 Agent Processing Complete:")
    print(f"   Total queries: {len(results)}")
    print(f"   Successful: {success_count}")
    print(f"   Failed: {len(results) - success_count}")
    
    print("✓ STAGE 6 TASK 2 COMPLETED")
    return results


def log_agent_responses(**context):
    """Log agent results to JSON file and PostgreSQL"""
    import json
    import pandas as pd
    from pathlib import Path
    from sqlalchemy import create_engine
    
    print("="*70)
    print("STAGE 6 TASK 3: LOG AGENT RESPONSES")
    print("="*70)
    
    ti = context['ti']
    results = ti.xcom_pull(task_ids='stage_6_ai_agent.process_with_ai_agent', key='agent_results')
    logs_dir = ti.xcom_pull(task_ids='stage_6_ai_agent.setup_agent_volume', key='logs_dir')
    
    year_dir = Path(logs_dir) / '2025'
    year_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = year_dir / f'AGENT_LOGS.json'
    
    with open(log_file, 'w') as f:
        json.dump({
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].run_id,
            'total_queries': len(results),
            'queries': results
        }, f, indent=2)
    
    print(f"✅ Agent logs saved to: {log_file}")
    
    try:
        DB_HOST = os.getenv('DB_HOST', 'pgdatabase')
        DB_USER = os.getenv('DB_USER', 'postgres')
        DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
        DB_PORT = os.getenv('DB_PORT', '5432')
        DB_NAME = os.getenv('DB_NAME', 'Trades_Database')
        
        connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(connection_string)
        
        df = pd.DataFrame(results)
        df['dag_run_id'] = context['dag_run'].run_id
        df.to_sql('agent_query_results', engine, if_exists='append', index=False)
        
        print(f"✅ Results also saved to PostgreSQL table 'agent_query_results'")
    except Exception as e:
        print(f"⚠️ Could not save to PostgreSQL: {str(e)}")
    
    print("✓ STAGE 6 TASK 3 COMPLETED")
    return str(log_file)
