from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import asyncio
import logging
from ..config.config import CONFIG
from .utils import (
    setup_logging, send_notification, fetch_redash_data, fetch_google_sheet,
    fetch_csv, fetch_api, create_bigquery_table, push_to_bigquery, run_bigquery_query
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def process_data_sources(**context):
    """Process data from all sources and push to BigQuery."""
    setup_logging()
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    sheet = pd.DataFrame(client.open_by_key(CONFIG['sheet_id']).worksheet("taskSources").get_all_records())
    task_list = context['dag_run'].conf.get('task_list', sheet['task_name'].tolist())
    tasks = sheet[(sheet['active_flag'].str.lower() == 'y') & (sheet['task_name'].isin(task_list))].reset_index(drop=True)

    for task in tasks.to_dict('records'):
        try:
            source_type = task['source_type']
            dataset_id = task['dataset']
            table_id = task['table_name']
            write_mode = task['write_mode']
            schema = json.loads(task['schema']) if task['schema'] else []

            # Create table if not exists
            create_bigquery_table(dataset_id, table_id, schema)

            # Fetch data
            if source_type == 'redash':
                df = asyncio.run(fetch_redash_data(task['query_id'], task['api_key'], task['params']))
                send_notification(f"Successfully fetched {len(df)} rows from Redash query {task['query_id']}")
            elif source_type == 'gsheet':
                df = fetch_google_sheet(task['worksheet_name'])
                send_notification(f"Successfully fetched {len(df)} rows from Google Sheet {task['worksheet_name']}")
            elif source_type == 'csv':
                df = fetch_csv(task['file_path'])
                send_notification(f"Successfully fetched {len(df)} rows from CSV {task['file_path']}")
            elif source_type == 'api':
                df = asyncio.run(fetch_api(task['api_url'], json.loads(task['headers']) if task['headers'] else None, json.loads(task['params']) if task['params'] else None))
                send_notification(f"Successfully fetched {len(df)} rows from API {task['api_url']}")
            else:
                raise ValueError(f"Unknown source type: {source_type}")

            # Push to BigQuery
            record_count = push_to_bigquery(df, dataset_id, table_id, write_mode)
            send_notification(f"Pushed {record_count} rows to BigQuery {dataset_id}.{table_id} (mode: {write_mode})")
        
        except Exception as e:
            error_msg = f"Failed processing task {task['task_name']}: {str(e)}"
            send_notification(error_msg)
            logging.error(error_msg)

def run_saved_queries(**context):
    """Run saved BigQuery queries."""
    setup_logging()
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    sheet = pd.DataFrame(client.open_by_key(CONFIG['sheet_id']).worksheet("taskQueries").get_all_records())
    task_list = context['dag_run'].conf.get('task_list', sheet['task_name'].tolist())
    queries = sheet[(sheet['active_flag'].str.lower() == 'y') & (sheet['task_name'].isin(task_list))].reset_index(drop=True)

    for query in queries.to_dict('records'):
        try:
            query_text = query['query_text']
            run_bigquery_query(query_text)
            send_notification(f"Successfully ran BigQuery query {query['query_name']}")
        except Exception as e:
            error_msg = f"Failed running query {query['query_name']}: {str(e)}"
            send_notification(error_msg)
            logging.error(error_msg)

with DAG(
    'data_pipeline',
    default_args=default_args,
    description='Warehouse data pipeline',
    schedule_interval=None,  # Controlled by Google Sheets
    start_date=datetime(2025, 4, 27),
    catchup=False,
) as dag:

    process_data_task = PythonOperator(
        task_id='process_data_sources',
        python_callable=process_data_sources,
        provide_context=True,
    )

    run_queries_task = PythonOperator(
        task_id='run_saved_queries',
        python_callable=run_saved_queries,
        provide_context=True,
    )

    process_data_task >> run_queries_task
