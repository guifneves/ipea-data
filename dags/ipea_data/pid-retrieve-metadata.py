import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import raizenlib.utils.adl as adl
import ipea_data.tasks.ingest as ingest

ADL = 'raizenprd01'
dag_id = 'PID-retrieve_ipea_metadata'
workdir = "ldt_dev/sandbox/lbarbosa"
worker_queue = "ipea-data-worker-queue"

default_args = {
    'owner': 'Projeto IPEA Data',
    'start_date': datetime(2019, 9, 3, 7, 0, 0, 695232),
    'depends_on_past': False,
    "email": ["guilherme.neves@raizen.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id,
    catchup=False,
    default_args=default_args,
    schedule_interval="0 15 * * *",
    max_active_runs=1,
    concurrency = 4
)

get_metadata = PythonOperator(
    task_id = "get_metadata",
    python_callable = ingest.get_metadata,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/')
    },
    queue = worker_queue,
    dag = dag
)

save_metadata = PythonOperator(
    task_id = "save_metadata",
    python_callable = ingest.save_metadata,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'table_name': 'metadados'
    },
    queue = worker_queue,
    dag = dag
)

clear_timeseries = PythonOperator(
    task_id = "clear_timeseries",
    python_callable = ingest.clear_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/series/')
    },
    queue = worker_queue,
    dag = dag
)

save_relationship_table  = PythonOperator(
    task_id = "save_relationship_table",
    python_callable = ingest.save_relationship_table,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),

        'table': 'sercodigo_temcodigo'
    },
    queue = worker_queue,
    dag = dag
)

get_metadata >> save_metadata >> save_relationship_table >> clear_timeseries
