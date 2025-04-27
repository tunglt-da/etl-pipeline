import os
import logging
from datetime import datetime
import pandas as pd
import requests
import aiohttp
import json
from tenacity import retry, stop_after_attempt, wait_fixed
from google.cloud import bigquery
from google.oauth2 import service_account
import gspread
from tabulate import tabulate
from ..config.config import CONFIG

def setup_logging():
    """Set up logging to file and console."""
    log_path = CONFIG['log_path']
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    
    log_file_path = os.path.join(log_path, f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.txt")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info('Logging system initialized')

def send_notification(message):
    """Send a notification to Google Chat."""
    try:
        requests.post(CONFIG['webhook_url'], json={'text': message})
        logging.info(f"Sent notification: {message}")
    except Exception as e:
        logging.error(f"Failed to send notification: {e}")

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
async def fetch_redash_data(query_id, api_key, params=None):
    """Fetch data from Redash API."""
    async with aiohttp.ClientSession() as session:
        headers = {'Authorization': f"Key {api_key}"}
        params = json.loads(params.replace("'", "\"")) if params else {}
        url_result = f"{CONFIG['redash_domain']}/api/queries/{query_id}/results"
        payload = {
            'apply_auto_limit': False,
            'id': query_id,
            'max_age': 0,
            'parameters': params
        }
        async with session.post(url_result, json=payload, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"Redash API error: {await response.text()}")
            result = await response.json()
        
        job_id = result['job'].get('id')
        if not job_id:
            raise Exception(result['job']['error'])
        
        url_job = f"{CONFIG['redash_domain']}/api/jobs/{job_id}"
        while True:
            async with session.get(url_job, headers=headers) as job_response:
                job_result = await job_response.json()
                status = job_result['job']['status']
                if status == 3:
                    query_result_id = job_result['job']['query_result_id']
                    async with session.get(f"{CONFIG['redash_domain']}/api/query_results/{query_result_id}", headers=headers) as result_response:
                        data = await result_response.json()
                        if data['query_result']['data']['rows']:
                            return pd.DataFrame(data['query_result']['data']['rows'])
                        else:
                            columns = [col['name'] for col in data['query_result']['data']['columns']]
                            return pd.DataFrame([['' for _ in columns]], columns=columns)
                elif status in [4, 5]:
                    raise Exception(job_result['job']['error'])
                await asyncio.sleep(1)

def fetch_google_sheet(worksheet_name):
    """Fetch data from a Google Sheet worksheet."""
    creds = service_account.Credentials.from_service_account_file(CONFIG['service_account_path'], scopes=CONFIG['scopes'])
    client = gspread.authorize(creds)
    worksheet = client.open_by_key(CONFIG['sheet_id']).worksheet(worksheet_name)
    return pd.DataFrame(worksheet.get_all_records())

def fetch_csv(file_path):
    """Read data from a CSV file."""
    full_path = os.path.join(CONFIG['data_path'], file_path)
    if os.path.exists(full_path):
        return pd.read_csv(full_path)
    else:
        raise FileNotFoundError(f"CSV file {full_path} not found")

async def fetch_api(api_url, headers=None, params=None):
    """Fetch data from a generic API."""
    async with aiohttp.ClientSession() as session:
        async with session.get(api_url, headers=headers, params=params) as response:
            if response.status != 200:
                raise Exception(f"API error: {await response.text()}")
            data = await response.json()
            return pd.DataFrame(data)

def create_bigquery_table(dataset_id, table_id, schema):
    """Create a BigQuery table if it doesn't exist."""
    client = bigquery.Client.from_service_account_json(CONFIG['service_account_path'])
    dataset_ref = client.dataset(dataset_id)
    
    try:
        client.get_dataset(dataset_ref)
    except:
        client.create_dataset(dataset_ref)
    
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref, schema=[
            bigquery.SchemaField(field['name'], field['type']) for field in schema
        ])
        client.create_table(table)

def push_to_bigquery(df, dataset_id, table_id, write_mode):
    """Push a DataFrame to BigQuery."""
    client = bigquery.Client.from_service_account_json(CONFIG['service_account_path'])
    table_ref = client.dataset(dataset_id).table(table_id)
    
    write_disposition = {
        'append': 'WRITE_APPEND',
        'replace': 'WRITE_TRUNCATE',
        'upsert': 'WRITE_APPEND'  # Simplified upsert, requires additional logic for true upsert
    }.get(write_mode, 'WRITE_APPEND')
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return len(df)

def run_bigquery_query(query):
    """Run a BigQuery saved query."""
    client = bigquery.Client.from_service_account_json(CONFIG['service_account_path'])
    query_job = client.query(query)
    query_job.result()
    return query_job
