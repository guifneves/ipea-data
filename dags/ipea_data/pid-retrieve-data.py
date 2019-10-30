import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import raizenlib.utils.adl as adl
import ipea_data.tasks.ingest as ingest

ADL = 'raizenprd01'
dag_id = 'PID-retrieve_ipea_data'
workdir = "ldt_dev/sandbox/lbarbosa"
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
        'table_name': 'ipea_data_metadados'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_temas_1 = PythonOperator(
    task_id = "get_timeseries_temas_1",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'total_temas': 43,
        'cod_temas': [28, 23, 10,  7,  5, 2,   8, 81, 24, 37, 54],
        'range': '1'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_temas_2 = PythonOperator(
    task_id = "get_timeseries_temas_2",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'total_temas': 43,
        'cod_temas': [55, 38, 11, 29, 63, 12, 19,  6, 39, 32, 56],
        'range': '2'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_temas_3 = PythonOperator(
    task_id = "get_timeseries_temas_3",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'total_temas': 43,
        'cod_temas': [31, 79, 15, 40,  3, 27, 14,  9, 57, 58,  1],
        'range': '3'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_temas_4 = PythonOperator(
    task_id = "get_timeseries_temas_4",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'total_temas': 43,
        'cod_temas': [16, 30, 13, 41, 20, 59, 17, 33, 26, 60],
        'range': '4'
    },
    queue = worker_queue,
    dag = dag
)

union_timeseries = PythonOperator(
    task_id = "union_timeseries",
    python_callable = ingest.union_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/series/')
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries = PythonOperator(
    task_id = "save_timeseries",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/series'),
        'table_name': 'ipea_data_series'
    },
    queue = worker_queue,
    dag = dag
)

get_metadata >> [save_metadata, get_timeseries_temas_1, get_timeseries_temas_2, get_timeseries_temas_3, get_timeseries_temas_4]
[get_timeseries_temas_1, get_timeseries_temas_2, get_timeseries_temas_3, get_timeseries_temas_4] >> union_timeseries >> save_timeseries
