# Warehouse Airflow Pipeline

This repository sets up an Apache Airflow pipeline on WSL (Windows Subsystem for Linux) to extract data from CSV, Google Sheets, Redash API, and other APIs, push it to BigQuery, run saved queries, and send status notifications via Google Chat. It uses WSL paths (`/mnt/...`) to share directories with Windows.

## Features
- Extract data from CSV, Google Sheets, Redash API, and generic REST APIs.
- Push data to BigQuery with configurable datasets, schemas, and tables (append, replace, upsert).
- Automatically create BigQuery datasets and tables if they don't exist.
- Run saved BigQuery queries.
- Send status notifications (success/failure, record counts) to Google Chat.

## Prerequisites
- **WSL2** installed on Windows (Ubuntu recommended).
- Python 3.8+ installed in WSL.
- **Google Cloud SDK** installed in WSL for BigQuery authentication.
- Access to a Windows directory via `/mnt/c/...`.

## Installation

1. **Set up WSL**:
   - Install WSL2 and Ubuntu: `wsl --install -d Ubuntu`.
   - Update Ubuntu: `sudo apt update && sudo apt upgrade`.
   - Install Python and pip:
     ```bash
     sudo apt install python3 python3-pip python3-venv
     ```

2. **Install Google Cloud SDK**:
   - Follow [Google Cloud SDK installation guide for Ubuntu](https://cloud.google.com/sdk/docs/install#deb).
   - Authenticate: `gcloud auth application-default login`.

3. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/warehouse-airflow-pipeline.git
   cd warehouse-airflow-pipeline
   ```

4. **Set up virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

5. **Set up environment variables**:
   - Create `config/.env` with the following:
     ```
     REDASH_DOMAIN=https://your-redash-domain.com
     WEBHOOK_URL=https://your-google-chat-webhook-url
     SERVICE_ACCOUNT_PATH=/mnt/c/path/to/your/service_account.json
     SHEET_ID=your-google-sheet-id
     DATA_PATH=/mnt/c/path/to/csv/data
     LOG_PATH=/mnt/c/path/to/logs
     BIGQUERY_PROJECT_ID=your-bigquery-project-id
     ```

6. **Set up Google Cloud credentials**:
   - Place your service account JSON file at `SERVICE_ACCOUNT_PATH` (e.g., `/mnt/c/Users/YourUser/Documents/service_account.json`).
   - Ensure the service account has BigQuery and Google Sheets API permissions.

7. **Configure Google Sheets**:
   - Create a Google Sheet with the following worksheets:
     - `taskSources`: Defines data sources (type: csv/gsheet/redash/api, path/query_id/api_url, table_name, dataset, schema, write_mode).
     - `taskQueries`: Lists BigQuery saved queries to run (query_name, query_text).
     - `taskSchedule`: Defines task schedules (task_name, time_slot).

8. **Initialize Airflow**:
   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

9. **Copy DAGs**:
   ```bash
   cp -r dags/* ~/airflow/dags/
   ```

10. **Run Airflow**:
    - Start the webserver:
      ```bash
      airflow webserver -p 8080
      ```
    - In a new terminal, start the scheduler:
      ```bash
      source venv/bin/activate
      airflow scheduler
      ```
    - Access Airflow UI at `http://localhost:8080` (login: `admin`/`admin`).
    - Enable the `data_pipeline` DAG.

## Usage
- The DAG `data_pipeline` runs tasks based on the schedule defined in the `taskSchedule` worksheet.
- Manually trigger the DAG via the Airflow UI or CLI:
  ```bash
  airflow dags trigger -d data_pipeline
  ```

## File Structure
- `dags/`: Airflow DAGs and utility functions.
- `config/`: Configuration files (`.env`, `config.py`).

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
