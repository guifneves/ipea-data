import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import raizenlib.utils.adl as adl
import ipea_data.tasks.ingest as ingest

ADL = 'raizenprd01'
dag_id = 'PID-retrieve_ipea_metadata'
workdir = "ldt_dev/projetos/ipea_data"
# worker_queue = "ipea-data-worker-queue"

executor_config={ 'KubernetesExecutor' : { 'image' : 'raizenanalyticsdev.azurecr.io/ipea-data:1.0.0' }}

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


start_download = DummyOperator(
    task_id="start_download",
    executor_config=executor_config,
    dag = dag
)

## Metadados
get_metadados = PythonOperator(
    task_id = "get_metadados",
    python_callable = ingest.get_metadados,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/')
    },
    executor_config=executor_config,
    dag = dag
)

save_metadados = PythonOperator(
    task_id = "save_metadados",
    python_callable = ingest.save_metadados,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'table_name': 'metadados'
    },
    executor_config=executor_config,
    dag = dag
)

## Territorios
get_territorios = PythonOperator(
    task_id = "get_territorios",
    python_callable = ingest.get_territorios,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/territorios/')
    },
    executor_config=executor_config,
    dag = dag
)

save_territorios = PythonOperator(
    task_id = "save_territorios",
    python_callable = ingest.save_territorios,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/territorios/'),
        'table_name': 'territorios'
    },
    executor_config=executor_config,
    dag = dag
)

## Temas
get_temas = PythonOperator(
    task_id = "get_temas",
    python_callable = ingest.get_temas,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/temas/')
    },
    executor_config=executor_config,
    dag = dag
)

save_temas = PythonOperator(
    task_id = "save_temas",
    python_callable = ingest.save_temas,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/temas/'),
        'table_name': 'temas'
    },
    executor_config=executor_config,
    dag = dag
)

## Paises
get_paises = PythonOperator(
    task_id = "get_paises",
    python_callable = ingest.get_paises,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/paises/')
    },
    executor_config=executor_config,
    dag = dag
)

save_paises = PythonOperator(
    task_id = "save_paises",
    python_callable = ingest.save_paises,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/paises/'),
        'table_name': 'paises'
    },
    executor_config=executor_config,
    dag = dag
)

## Fontes
get_fontes = PythonOperator(
    task_id = "get_fontes",
    python_callable = ingest.get_fontes,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/fontes/')
    },
    executor_config=executor_config,
    dag = dag
)

save_fontes = PythonOperator(
    task_id = "save_fontes",
    python_callable = ingest.save_fontes,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/fontes/'),
        'table_name': 'fontes'
    },
    executor_config=executor_config,
    dag = dag
)

finish_download = DummyOperator(
    task_id="finish_download",
    executor_config=executor_config,
    dag = dag
)


start_download >> [get_metadados,get_territorios,get_temas,get_paises,get_fontes]
get_metadados >> save_metadados 
get_territorios >> save_territorios 
get_temas >> save_temas 
get_paises >> save_paises 
get_fontes >> save_fontes
[save_metadados,save_territorios,save_temas,save_paises,save_fontes] >> finish_download


