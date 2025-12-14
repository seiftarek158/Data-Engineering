"""
Stock Portfolio Pipeline DAG - Team 55_0654
=============================================

This unified DAG orchestrates the complete data engineering pipeline with all stages
organized into TaskGroups as per Milestone 3 requirements.

Pipeline Stages:
- Stage 1: Data Cleaning & Integration
- Stage 2: Encoding & Stream Preparation  
- Stage 3: Kafka Streaming
- Stage 4: Spark Analytics
- Stage 5: Visualization Preparation
- Stage 6: AI Agent Query Processing

Each stage is organized as a TaskGroup for better visualization and management.
All task functions are imported from the tasks/ module for better organization.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Import task functions from modular task files
from dags.stage_1_tasks import (
    clean_missing_values,
    detect_outliers,
    integrate_datasets,
    load_to_postgres
)
from dags.stage_2_tasks import (
    prepare_streaming_data,
    encode_categorical_data
)
from dags.stage_3_tasks import (
    consume_and_process_stream,
    save_final_to_postgres
)
from dags.stage_4_tasks import (
    initialize_spark_session,
    run_spark_analytics
)
from dags.stage_5_tasks import (
    prepare_visualization
)
from dags.stage_6_tasks import (
    setup_agent_volume,
    process_with_ai_agent,
    log_agent_responses
)

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering_team', 
    'depends_on_past': False, 
    'start_date': "<yesterday>", # change to use days ago 
    'email_on_retry': False, 
    'retries': 1 
}


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
   'stock_portfolio_pipeline_Scrooge_Mcdata', 
    default_args=default_args, 
    description='End-to-end stock portfolio analytics pipeline', 
    schedule_interval='@daily', 
    catchup=False, 
    tags=['data-engineering', 'stocks', 'analytics'], 
) as dag:
    
    # ========================================================================
    # STAGE 1: DATA CLEANING & INTEGRATION
    # ========================================================================
    with TaskGroup(group_id='stage_1_data_cleaning_integration') as stage_1:
        
        clean_missing_values = PythonOperator(
            task_id='clean_missing_values',
            python_callable=clean_missing_values,
            op_kwargs={
                'dtp_input_path': '/opt/airflow/notebook/data/daily_trade_prices.csv',
                'trades_input_path': '/opt/airflow/notebook/data/trades.csv'
            },
            provide_context=True,
        )
        
        detect_outliers = PythonOperator(
            task_id='detect_outliers',
            python_callable=detect_outliers,
            op_kwargs={
                'trades_input_path': '/opt/airflow/notebook/data/trades.csv',
                'dtp_input_path': '/opt/airflow/notebook/data/dtp_cleaned.csv',
                'dim_customer_input_path': '/opt/airflow/notebook/data/dim_customer.csv'
            },
            provide_context=True,
        )
        
        integrate_datasets = PythonOperator(
            task_id='integrate_datasets',
            python_callable=integrate_datasets,
            op_kwargs={
                'trades_input_path': '/opt/airflow/notebook/data/trades_outliers_handled.csv',
                'dim_customer_input_path': '/opt/airflow/notebook/data/dim_customer_outlier_handled.csv',
                'dim_date_input_path': '/opt/airflow/notebook/data/dim_date.csv',
                'dim_stock_input_path': '/opt/airflow/notebook/data/dim_stock.csv',
                'dtp_input_path': '/opt/airflow/notebook/data/dtp_cleaned_outlier_handled.csv'
            },
            provide_context=True
        )
        
        load_to_postgres = PythonOperator(
            task_id='load_to_postgres',
            python_callable=load_to_postgres,
            op_kwargs={
                'input_path': '/opt/airflow/notebook/data/integrated_data.csv',
                'table_name': 'cleaned_trades'
            },
            provide_context=True
        )
        
        clean_missing_values >> detect_outliers >> integrate_datasets >> load_to_postgres
    
    # ========================================================================
    # STAGE 2: ENCODING & STREAM PREPARATION
    # ========================================================================
    with TaskGroup(group_id='stage_2_data_processing') as stage_2:
        
        prepare_streaming_data = PythonOperator(
            task_id='prepare_streaming_data',
            python_callable=prepare_streaming_data,
            provide_context=True,
        )
        
        encode_categorical_data = PythonOperator(
            task_id='encode_categorical_data',
            python_callable=encode_categorical_data,
            provide_context=True,
        )
        
        prepare_streaming_data >> encode_categorical_data
    
    # ========================================================================
    # STAGE 3: KAFKA STREAMING
    # ========================================================================
    with TaskGroup(group_id='stage_3_kafka_streaming') as stage_3:
        
        start_kafka_producer = BashOperator(
            task_id='start_kafka_producer',
            bash_command='python /opt/airflow/plugins/kafka_producer.py',
        )
        
        consume_and_process_stream = PythonOperator(
            task_id='consume_and_process_stream',
            python_callable=consume_and_process_stream,
            provide_context=True,
        )
        
        save_final_to_postgres = PythonOperator(
            task_id='save_final_to_postgres',
            python_callable=save_final_to_postgres,
            provide_context=True,
        )
        
        start_kafka_producer >> consume_and_process_stream >> save_final_to_postgres
    
    # ========================================================================
    # STAGE 4: SPARK ANALYTICS
    # ========================================================================
    with TaskGroup(group_id='stage_4_spark_analytics') as stage_4:
        
        initialize_spark_session = PythonOperator(
            task_id='initialize_spark_session',
            python_callable=initialize_spark_session,
            provide_context=True,
        )
        
        run_spark_analytics = PythonOperator(
            task_id='run_spark_analytics',
            python_callable=run_spark_analytics,
            op_kwargs={
                'input_path': '/opt/airflow/notebook/data/FULL_STOCKS.csv'
            },
            provide_context=True,
        )
        
        initialize_spark_session >> run_spark_analytics
    
    # ========================================================================
    # STAGE 5: VISUALIZATION PREPARATION
    # ========================================================================
    with TaskGroup(group_id='stage_5_visualization') as stage_5:
        
        prepare_visualization = PythonOperator(
            task_id='prepare_visualization',
            python_callable=prepare_visualization,
            provide_context=True,
        )
    
    # ========================================================================
    # STAGE 6: AI AGENT QUERY PROCESSING
    # ========================================================================
    with TaskGroup(group_id='stage_6_ai_agent') as stage_6:
        
        setup_agent_volume = PythonOperator(
            task_id='setup_agent_volume',
            python_callable=setup_agent_volume,
            provide_context=True,
        )
        
        process_with_ai_agent = PythonOperator(
            task_id='process_with_ai_agent',
            python_callable=process_with_ai_agent,
            provide_context=True,
        )
        
        log_agent_responses = PythonOperator(
            task_id='log_agent_responses',
            python_callable=log_agent_responses,
            provide_context=True,
        )
        
        setup_agent_volume >> process_with_ai_agent >> log_agent_responses
    
    # ========================================================================
    # CROSS-STAGE DEPENDENCIES
    # ========================================================================
    stage_1 >> stage_2 >> stage_3 >> stage_4 >> [stage_5, stage_6]
