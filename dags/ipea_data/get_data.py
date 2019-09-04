import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import raizenlib.utils.adl as adl

ADL = 'raizenprd01'
dag_id = 'PID-retrieve_ipea_data'
workdir = 'ldt_dev/projetos/'
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

def fn_get_metadata(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea
    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_metadados()
    df = spark.createDataFrame(df_pd)
    df = df.withColumn("ref_date", F.lit(datetime.now().strftime("%Y-%m-%d")))
    df.coalesce(1).write \
        .partitionBy("ref_date") \
        .format("parquet") \
        .save(args["save_path"])


get_metadata = PythonOperator(
    task_id = "get_metadata",
    python_callable = fn_get_metadata,
    op_kwargs = {
        'save_path': adl.adl_full_url(ADL, workdir + '/ipeadata/raw/metadados'),
        'table_name': 'ipeadata_metadados'
    },
    queue = worker_queue,
    dag = dag
)

def fn_save_metadata(**args):
    print("TODO")

save_metadata = DummyOperator(task_id = "save_metadata", dag = dag)

def fn_get_timeseries(**args):
    print("TODO")

get_timeseries = PythonOperator(
    task_id = "get_timeseries",
    python_callable = fn_get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/ipeadata/raw/metadados'),
        'save_path': adl.adl_full_url(ADL, workdir + '/ipeadata/dados/'),
        'table_name': 'ipeadata_metadados'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries = DummyOperator(task_id = "save_timeseries", dag = dag)

get_metadata >> [save_metadata, get_timeseries]
get_timeseries >> save_timeseries
