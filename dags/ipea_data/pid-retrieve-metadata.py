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

## Metadados
get_metadados = PythonOperator(
    task_id = "get_metadados",
    python_callable = ingest.get_metadados,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/')
    },
    queue = worker_queue,
    dag = dag
)

save_metadados = PythonOperator(
    task_id = "save_metadados",
    python_callable = ingest.save_metadados,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'table_name': 'metadados'
    },
    queue = worker_queue,
    dag = dag
)

## Territorios
get_territorios = PythonOperator(
    task_id = "get_territorios",
    python_callable = ingest.get_territorios,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/territorios/')
    },
    queue = worker_queue,
    dag = dag
)

save_territorios = PythonOperator(
    task_id = "save_territorios",
    python_callable = ingest.save_territorios,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/territorios/'),
        'table_name': 'territorios'
    },
    queue = worker_queue,
    dag = dag
)

## Temas
get_temas = PythonOperator(
    task_id = "get_temas",
    python_callable = ingest.get_temas,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/temas/')
    },
    queue = worker_queue,
    dag = dag
)

save_temas = PythonOperator(
    task_id = "save_temas",
    python_callable = ingest.save_temas,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/temas/'),
        'table_name': 'temas'
    },
    queue = worker_queue,
    dag = dag
)

## Paises
get_paises = PythonOperator(
    task_id = "get_paises",
    python_callable = ingest.get_paises,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/paises/')
    },
    queue = worker_queue,
    dag = dag
)

save_paises = PythonOperator(
    task_id = "save_paises",
    python_callable = ingest.save_paises,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/paises/'),
        'table_name': 'paises'
    },
    queue = worker_queue,
    dag = dag
)

## Fontes
get_fontes = PythonOperator(
    task_id = "get_fontes",
    python_callable = ingest.get_fontes,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/fontes/')
    },
    queue = worker_queue,
    dag = dag
)

save_fontes = PythonOperator(
    task_id = "save_fontes",
    python_callable = ingest.save_fontes,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/fontes/'),
        'table_name': 'fontes'
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

get_metadados >> save_metadados 
get_territorios >> save_territorios 
get_temas >> save_temas 
get_paises >> save_paises 
get_fontes >> save_fontes
clear_timeseries

