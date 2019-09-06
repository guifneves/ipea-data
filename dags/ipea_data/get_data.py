import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import raizenlib.utils.adl as adl
import ipea_data.tasks.extract as extract
import ipea_data.tasks.persist as persist

ADL = 'raizenprd01'
dag_id = 'PID-retrieve_ipea_data'
workdir = 'ldt_dev/sandbox/'
worker_queue = "ipea-data-worker-queue"

default_args = {
    'owner': 'Projeto IPEA Data',
    'start_date': datetime(2019, 9, 3, 7, 0, 0, 695232),
    'depends_on_past': False,
    "email": ["guilherme.neves@raizen.com"],
    "email_on_failure": True,
    "email_on_retry": False
}

dag = DAG(dag_id, catchup=False, default_args=default_args, schedule_interval="@daily", max_active_runs=1)


get_metadata = PythonOperator(
    task_id = "get_metadata",
    python_callable = extract.fn_get_metadata,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/ipea_data/raw_igor/metadados'),
        'table_name': 'ipeadata_metadados'
    },
    queue = worker_queue,
    dag = dag
)

save_metadata = PythonOperator(
    task_id = "save_metadata",
    python_callable = persist.fn_save_metadata,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/ipea_data/raw/metadados'),
        'table_name': 'ipeadata_metadados'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries = PythonOperator(
    task_id = "get_timeseries",
    python_callable = extract.fn_get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/ipea_data/raw_igor//metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + 'ipea_data/dados_igor/'),
        'table_name': 'ipeadata_metadados'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries = PythonOperator(
    task_id = "save_timeseries",
    python_callable = persist.fn_save_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/ipea_data/dados/'),
        'table_name': 'ipeadata_metadados'
    },
    queue = worker_queue,
    dag = dag
)

get_metadata >> [save_metadata, get_timeseries]
get_timeseries >> save_timeseries
