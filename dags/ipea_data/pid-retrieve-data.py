import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import raizenlib.utils.adl as adl
import ipea_data.tasks.ingest as ingest

ADL = 'raizenprd01'
dag_id = 'PID-retrieve_ipea_data'
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
    concurrency = 1
)

sensor_wait_ipea_metadata = ExternalTaskSensor(
    task_id = 'sensor_wait_ipea_metadata',
    external_dag_id = 'PID-retrieve_ipea_metadata',
    external_task_id = 'finish_download',
    queue = worker_queue,
    dag = dag
)

get_timeseries_agropecuaria = PythonOperator(
    task_id = "get_timeseries_agropecuaria",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '28'
    },
    executor_config=executor_config,
    dag = dag
)

get_timeseries_financas_publicas = PythonOperator(
    task_id = "get_timeseries_financas_publicas",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '6'
    },
    executor_config=executor_config,
    dag = dag
)

get_timeseries_seguranca_publica = PythonOperator(
    task_id = "get_timeseries_seguranca_publica",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '20'
    },
    executor_config=executor_config,
    dag = dag
)

save_timeseries_agropecuaria = PythonOperator(
    task_id = "save_timeseries_agropecuaria",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),        
        'cod_tema': 28,
        'name_tema': 'agropecuaria'
    },
    executor_config=executor_config,
    dag = dag
)

save_timeseries_financas_publicas = PythonOperator(
    task_id = "save_timeseries_financas_publicas",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 6,
        'name_tema': 'financas_publicas'
    },
    executor_config=executor_config,
    dag = dag
)

save_timeseries_seguranca_publica = PythonOperator(
    task_id = "save_timeseries_seguranca_publica",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 20,
        'name_tema': 'seguranca_publica'
    },
    executor_config=executor_config,
    dag = dag
)

sensor_wait_ipea_metadata >> get_timeseries_agropecuaria >> save_timeseries_agropecuaria
sensor_wait_ipea_metadata >> get_timeseries_financas_publicas >> save_timeseries_financas_publicas
sensor_wait_ipea_metadata >> get_timeseries_seguranca_publica >> save_timeseries_seguranca_publica