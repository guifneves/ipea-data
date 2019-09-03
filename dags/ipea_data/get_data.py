import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'Projeto IPEA Data',
    'start_date': datetime(2019, 9, 3, 7, 0, 0, 695232),
    'depends_on_past': False,
    "email": ["guilherme.neves@raizen.com"],
    "email_on_failure": True,
    "email_on_retry": False
}

dag = DAG("PID-retrieve_ipea_data", catchup=False, default_args=default_args, schedule_interval="@daily",max_active_runs=1)


retrieve_data = DummyOperator(task_id = "retrieve_data", dag = dag)


retrieve_data