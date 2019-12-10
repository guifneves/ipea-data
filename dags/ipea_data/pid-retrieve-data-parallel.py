import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

import raizenlib.utils.adl as adl
import ipea_data.tasks.ingest as ingest

ADL = 'raizenprd01'
dag_id = 'PID-retrieve_ipea_data_parallel'
workdir = "ldt_dev/projetos/ipea_data"
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

sensor_wait_ipea_metadata = ExternalTaskSensor(
    task_id = 'sensor_wait_ipea_metadata',
    external_dag_id = 'PID-retrieve_ipea_metadata',
    external_task_id = 'finish_download',
    dag = dag
)

get_timeseries_assistencia_social = PythonOperator(
    task_id = "get_timeseries_assistencia_social",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '23'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_balanco_de_pagamentos = PythonOperator(
    task_id = "get_timeseries_balanco_de_pagamentos",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '10'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_cambio = PythonOperator(
    task_id = "get_timeseries_cambio",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '7'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_comercio_exterior = PythonOperator(
    task_id = "get_timeseries_comercio_exterior",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '5'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_consumo_e_vendas = PythonOperator(
    task_id = "get_timeseries_consumo_e_vendas",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '2'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_contas_nacionais = PythonOperator(
    task_id = "get_timeseries_contas_nacionais",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '8'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_contas_regionais = PythonOperator(
    task_id = "get_timeseries_contas_regionais",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '81'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_correcao_monetaria = PythonOperator(
    task_id = "get_timeseries_correcao_monetaria",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '24'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_demografia = PythonOperator(
    task_id = "get_timeseries_demografia",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '37'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_deputado_estadual = PythonOperator(
    task_id = "get_timeseries_deputado_estadual",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '54'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_deputado_federal = PythonOperator(
    task_id = "get_timeseries_deputado_federal",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '55'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_desenvolvimento_humano = PythonOperator(
    task_id = "get_timeseries_desenvolvimento_humano",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '38'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_economia_internacional = PythonOperator(
    task_id = "get_timeseries_economia_internacional",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '11'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_educacao = PythonOperator(
    task_id = "get_timeseries_educacao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '29'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_eleitorado = PythonOperator(
    task_id = "get_timeseries_eleitorado",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '63'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_emprego = PythonOperator(
    task_id = "get_timeseries_emprego",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '12'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_estoque_de_capital = PythonOperator(
    task_id = "get_timeseries_estoque_de_capital",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '19'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_financeiras = PythonOperator(
    task_id = "get_timeseries_financeiras",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '39'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_geografico = PythonOperator(
    task_id = "get_timeseries_geografico",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '32'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_governador = PythonOperator(
    task_id = "get_timeseries_governador",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '56'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_habitacao = PythonOperator(
    task_id = "get_timeseries_habitacao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '31'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_idhm2000 = PythonOperator(
    task_id = "get_timeseries_idhm2000",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '79'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_indicadores_sociais = PythonOperator(
    task_id = "get_timeseries_indicadores_sociais",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '15'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_mercado_de_trabalho = PythonOperator(
    task_id = "get_timeseries_mercado_de_trabalho",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '40'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_moeda_e_credito = PythonOperator(
    task_id = "get_timeseries_moeda_e_credito",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '3'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_percepcao_e_expectativa = PythonOperator(
    task_id = "get_timeseries_percepcao_e_expectativa",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '27'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_populacao = PythonOperator(
    task_id = "get_timeseries_populacao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '14'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_precos = PythonOperator(
    task_id = "get_timeseries_precos",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '9'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_prefeito = PythonOperator(
    task_id = "get_timeseries_prefeito",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '57'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_presidente = PythonOperator(
    task_id = "get_timeseries_presidente",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '58'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_producao = PythonOperator(
    task_id = "get_timeseries_producao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '1'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_projecoes = PythonOperator(
    task_id = "get_timeseries_projecoes",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '16'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_renda = PythonOperator(
    task_id = "get_timeseries_renda",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '30'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_salario_e_renda = PythonOperator(
    task_id = "get_timeseries_salario_e_renda",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '13'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_saude = PythonOperator(
    task_id = "get_timeseries_saude",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '41'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_senador = PythonOperator(
    task_id = "get_timeseries_senador",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '59'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_sinopse_macroeconomica = PythonOperator(
    task_id = "get_timeseries_sinopse_macroeconomica",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '17'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_transporte = PythonOperator(
    task_id = "get_timeseries_transporte",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '33'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_vendas = PythonOperator(
    task_id = "get_timeseries_vendas",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '26'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_vereador = PythonOperator(
    task_id = "get_timeseries_vereador",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': '60'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_assistencia_social = PythonOperator(
    task_id = "save_timeseries_assistencia_social",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 23,
        'name_tema': 'assistencia_social'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_balanco_de_pagamentos = PythonOperator(
    task_id = "save_timeseries_balanco_de_pagamentos",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 10,
        'name_tema': 'balanco_de_pagamentos'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_cambio = PythonOperator(
    task_id = "save_timeseries_cambio",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 7,
        'name_tema': 'cambio'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_comercio_exterior = PythonOperator(
    task_id = "save_timeseries_comercio_exterior",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 5,
        'name_tema': 'comercio_exterior'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_consumo_e_vendas = PythonOperator(
    task_id = "save_timeseries_consumo_e_vendas",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 2,
        'name_tema': 'consumo_e_vendas'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_contas_nacionais = PythonOperator(
    task_id = "save_timeseries_contas_nacionais",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 8,
        'name_tema': 'contas_nacionais'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_contas_regionais = PythonOperator(
    task_id = "save_timeseries_contas_regionais",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 81,
        'name_tema': 'contas_regionais'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_correcao_monetaria = PythonOperator(
    task_id = "save_timeseries_correcao_monetaria",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 24,
        'name_tema': 'correcao_monetaria'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_demografia = PythonOperator(
    task_id = "save_timeseries_demografia",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 37,
        'name_tema': 'demografia'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_deputado_estadual = PythonOperator(
    task_id = "save_timeseries_deputado_estadual",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 54,
        'name_tema': 'deputado_estadual'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_deputado_federal = PythonOperator(
    task_id = "save_timeseries_deputado_federal",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 55,
        'name_tema': 'deputado_federal'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_desenvolvimento_humano = PythonOperator(
    task_id = "save_timeseries_desenvolvimento_humano",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 38,
        'name_tema': 'desenvolvimento_humano'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_economia_internacional = PythonOperator(
    task_id = "save_timeseries_economia_internacional",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 11,
        'name_tema': 'economia_internacional'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_educacao = PythonOperator(
    task_id = "save_timeseries_educacao",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 29,
        'name_tema': 'educacao'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_eleitorado = PythonOperator(
    task_id = "save_timeseries_eleitorado",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 63,
        'name_tema': 'eleitorado'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_emprego = PythonOperator(
    task_id = "save_timeseries_emprego",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 12,
        'name_tema': 'emprego'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_estoque_de_capital = PythonOperator(
    task_id = "save_timeseries_estoque_de_capital",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 19,
        'name_tema': 'estoque_de_capital'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_financeiras = PythonOperator(
    task_id = "save_timeseries_financeiras",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 39,
        'name_tema': 'financeiras'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_geografico = PythonOperator(
    task_id = "save_timeseries_geografico",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 32,
        'name_tema': 'geografico'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_governador = PythonOperator(
    task_id = "save_timeseries_governador",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 56,
        'name_tema': 'governador'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_habitacao = PythonOperator(
    task_id = "save_timeseries_habitacao",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 31,
        'name_tema': 'habitacao'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_idhm2000 = PythonOperator(
    task_id = "save_timeseries_idhm2000",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 79,
        'name_tema': 'idhm2000'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_indicadores_sociais = PythonOperator(
    task_id = "save_timeseries_indicadores_sociais",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 15,
        'name_tema': 'indicadores_sociais'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_mercado_de_trabalho = PythonOperator(
    task_id = "save_timeseries_mercado_de_trabalho",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 40,
        'name_tema': 'mercado_de_trabalho'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_moeda_e_credito = PythonOperator(
    task_id = "save_timeseries_moeda_e_credito",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 3,
        'name_tema': 'moeda_e_credito'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_percepcao_e_expectativa = PythonOperator(
    task_id = "save_timeseries_percepcao_e_expectativa",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 27,
        'name_tema': 'percepcao_e_expectativa'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_populacao = PythonOperator(
    task_id = "save_timeseries_populacao",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 14,
        'name_tema': 'populacao'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_precos = PythonOperator(
    task_id = "save_timeseries_precos",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 9,
        'name_tema': 'precos'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_prefeito = PythonOperator(
    task_id = "save_timeseries_prefeito",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 57,
        'name_tema': 'prefeito'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_presidente = PythonOperator(
    task_id = "save_timeseries_presidente",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 58,
        'name_tema': 'presidente'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_producao = PythonOperator(
    task_id = "save_timeseries_producao",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 1,
        'name_tema': 'producao'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_projecoes = PythonOperator(
    task_id = "save_timeseries_projecoes",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 16,
        'name_tema': 'projecoes'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_renda = PythonOperator(
    task_id = "save_timeseries_renda",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 30,
        'name_tema': 'renda'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_salario_e_renda = PythonOperator(
    task_id = "save_timeseries_salario_e_renda",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 13,
        'name_tema': 'salario_e_renda'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_saude = PythonOperator(
    task_id = "save_timeseries_saude",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 41,
        'name_tema': 'saude'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_senador = PythonOperator(
    task_id = "save_timeseries_senador",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 59,
        'name_tema': 'senador'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_sinopse_macroeconomica = PythonOperator(
    task_id = "save_timeseries_sinopse_macroeconomica",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 17,
        'name_tema': 'sinopse_macroeconomica'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_transporte = PythonOperator(
    task_id = "save_timeseries_transporte",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 33,
        'name_tema': 'transporte'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_vendas = PythonOperator(
    task_id = "save_timeseries_vendas",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 26,
        'name_tema': 'vendas'
    },
    queue = worker_queue,
    dag = dag
)

save_timeseries_vereador  = PythonOperator(
    task_id = "save_timeseries_vereador",
    python_callable = ingest.save_timeseries,
    op_kwargs = {
        'source_path_series': adl.adl_full_url(ADL, workdir + '/trusted/series/'),
        'cod_tema': 60,
        'name_tema': 'vereador '
    },
    queue = worker_queue,
    dag = dag
)

sensor_wait_ipea_metadata >> get_timeseries_assistencia_social >> save_timeseries_assistencia_social
sensor_wait_ipea_metadata >> get_timeseries_balanco_de_pagamentos >> save_timeseries_balanco_de_pagamentos
sensor_wait_ipea_metadata >> get_timeseries_cambio >> save_timeseries_cambio
sensor_wait_ipea_metadata >> get_timeseries_comercio_exterior >> save_timeseries_comercio_exterior
sensor_wait_ipea_metadata >> get_timeseries_consumo_e_vendas >> save_timeseries_consumo_e_vendas
sensor_wait_ipea_metadata >> get_timeseries_contas_nacionais >> save_timeseries_contas_nacionais
sensor_wait_ipea_metadata >> get_timeseries_contas_regionais >> save_timeseries_contas_regionais
sensor_wait_ipea_metadata >> get_timeseries_correcao_monetaria >> save_timeseries_correcao_monetaria
sensor_wait_ipea_metadata >> get_timeseries_demografia >> save_timeseries_demografia
sensor_wait_ipea_metadata >> get_timeseries_deputado_estadual >> save_timeseries_deputado_estadual
sensor_wait_ipea_metadata >> get_timeseries_deputado_federal >> save_timeseries_deputado_federal
sensor_wait_ipea_metadata >> get_timeseries_desenvolvimento_humano >> save_timeseries_desenvolvimento_humano
sensor_wait_ipea_metadata >> get_timeseries_economia_internacional >> save_timeseries_economia_internacional
sensor_wait_ipea_metadata >> get_timeseries_educacao >> save_timeseries_educacao
sensor_wait_ipea_metadata >> get_timeseries_eleitorado >> save_timeseries_eleitorado
sensor_wait_ipea_metadata >> get_timeseries_emprego >> save_timeseries_emprego
sensor_wait_ipea_metadata >> get_timeseries_estoque_de_capital >> save_timeseries_estoque_de_capital
sensor_wait_ipea_metadata >> get_timeseries_financeiras >> save_timeseries_financeiras
sensor_wait_ipea_metadata >> get_timeseries_geografico >> save_timeseries_geografico
sensor_wait_ipea_metadata >> get_timeseries_governador >> save_timeseries_governador
sensor_wait_ipea_metadata >> get_timeseries_habitacao >> save_timeseries_habitacao
sensor_wait_ipea_metadata >> get_timeseries_idhm2000 >> save_timeseries_idhm2000
sensor_wait_ipea_metadata >> get_timeseries_indicadores_sociais >> save_timeseries_indicadores_sociais
sensor_wait_ipea_metadata >> get_timeseries_mercado_de_trabalho >> save_timeseries_mercado_de_trabalho
sensor_wait_ipea_metadata >> get_timeseries_moeda_e_credito >> save_timeseries_moeda_e_credito
sensor_wait_ipea_metadata >> get_timeseries_percepcao_e_expectativa >> save_timeseries_percepcao_e_expectativa
sensor_wait_ipea_metadata >> get_timeseries_populacao >> save_timeseries_populacao
sensor_wait_ipea_metadata >> get_timeseries_precos >> save_timeseries_precos
sensor_wait_ipea_metadata >> get_timeseries_prefeito >> save_timeseries_prefeito
sensor_wait_ipea_metadata >> get_timeseries_presidente >> save_timeseries_presidente
sensor_wait_ipea_metadata >> get_timeseries_producao >> save_timeseries_producao
sensor_wait_ipea_metadata >> get_timeseries_projecoes >> save_timeseries_projecoes
sensor_wait_ipea_metadata >> get_timeseries_renda >> save_timeseries_renda
sensor_wait_ipea_metadata >> get_timeseries_salario_e_renda >> save_timeseries_salario_e_renda
sensor_wait_ipea_metadata >> get_timeseries_saude >> save_timeseries_saude
sensor_wait_ipea_metadata >> get_timeseries_senador >> save_timeseries_senador
sensor_wait_ipea_metadata >> get_timeseries_sinopse_macroeconomica >> save_timeseries_sinopse_macroeconomica
sensor_wait_ipea_metadata >> get_timeseries_transporte >> save_timeseries_transporte
sensor_wait_ipea_metadata >> get_timeseries_vendas >> save_timeseries_vendas
sensor_wait_ipea_metadata >> get_timeseries_vereador >> save_timeseries_vereador
