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

dag = DAG(
    dag_id, 
    catchup=False, 
    default_args=default_args, 
    schedule_interval="@daily", 
    max_active_runs=1,
    concurrency = 3
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
        'table_name': 'ipea_data_metadados'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_agropecuaria = PythonOperator(
    task_id = "get_timeseries_agropecuaria",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '28'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_assistencia_social = PythonOperator(
    task_id = "get_timeseries_assistencia_social",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '23'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_balanco_de_pagamentos = PythonOperator(
    task_id = "get_timeseries_balanco_de_pagamentos",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '10'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_cambio = PythonOperator(
    task_id = "get_timeseries_cambio",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '7'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_comercio_exterior = PythonOperator(
    task_id = "get_timeseries_comercio_exterior",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '5'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_consumo_e_vendas = PythonOperator(
    task_id = "get_timeseries_consumo_e_vendas",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '2'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_contas_nacionais = PythonOperator(
    task_id = "get_timeseries_contas_nacionais",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '8'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_contas_regionais = PythonOperator(
    task_id = "get_timeseries_contas_regionais",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '81'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_correcao_monetaria = PythonOperator(
    task_id = "get_timeseries_correcao_monetaria",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '24'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_demografia = PythonOperator(
    task_id = "get_timeseries_demografia",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '37'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_deputado_estadual = PythonOperator(
    task_id = "get_timeseries_deputado_estadual",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '54'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_deputado_federal = PythonOperator(
    task_id = "get_timeseries_deputado_federal",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '55'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_desenvolvimento_humano = PythonOperator(
    task_id = "get_timeseries_desenvolvimento_humano",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '38'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_economia_internacional = PythonOperator(
    task_id = "get_timeseries_economia_internacional",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '11'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_educacao = PythonOperator(
    task_id = "get_timeseries_educacao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '29'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_eleitorado = PythonOperator(
    task_id = "get_timeseries_eleitorado",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '63'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_emprego = PythonOperator(
    task_id = "get_timeseries_emprego",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '12'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_estoque_de_capital = PythonOperator(
    task_id = "get_timeseries_estoque_de_capital",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '19'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_financas_publicas = PythonOperator(
    task_id = "get_timeseries_financas_publicas",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '6'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_financeiras = PythonOperator(
    task_id = "get_timeseries_financeiras",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '39'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_geografico = PythonOperator(
    task_id = "get_timeseries_geografico",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '32'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_governador = PythonOperator(
    task_id = "get_timeseries_governador",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '56'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_habitacao = PythonOperator(
    task_id = "get_timeseries_habitacao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '31'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_idhm2000 = PythonOperator(
    task_id = "get_timeseries_idhm2000",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '79'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_indicadores_sociais = PythonOperator(
    task_id = "get_timeseries_indicadores_sociais",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '15'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_mercado_de_trabalho = PythonOperator(
    task_id = "get_timeseries_mercado_de_trabalho",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '40'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_moeda_e_credito = PythonOperator(
    task_id = "get_timeseries_moeda_e_credito",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '3'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_percepcao_e_expectativa = PythonOperator(
    task_id = "get_timeseries_percepcao_e_expectativa",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '27'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_populacao = PythonOperator(
    task_id = "get_timeseries_populacao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '14'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_precos = PythonOperator(
    task_id = "get_timeseries_precos",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '9'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_prefeito = PythonOperator(
    task_id = "get_timeseries_prefeito",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '57'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_presidente = PythonOperator(
    task_id = "get_timeseries_presidente",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '58'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_producao = PythonOperator(
    task_id = "get_timeseries_producao",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '1'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_projecoes = PythonOperator(
    task_id = "get_timeseries_projecoes",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '16'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_renda = PythonOperator(
    task_id = "get_timeseries_renda",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '30'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_salario_e_renda = PythonOperator(
    task_id = "get_timeseries_salario_e_renda",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '13'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_saude = PythonOperator(
    task_id = "get_timeseries_saude",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '41'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_seguranca_publica = PythonOperator(
    task_id = "get_timeseries_seguranca_publica",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '20'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_senador = PythonOperator(
    task_id = "get_timeseries_senador",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '59'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_sinopse_macroeconomica = PythonOperator(
    task_id = "get_timeseries_sinopse_macroeconomica",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '17'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_transporte = PythonOperator(
    task_id = "get_timeseries_transporte",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '33'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_vendas = PythonOperator(
    task_id = "get_timeseries_vendas",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '26'
    },
    queue = worker_queue,
    dag = dag
)

get_timeseries_vereador = PythonOperator(
    task_id = "get_timeseries_vereador",
    python_callable = ingest.get_timeseries,
    op_kwargs = {
        'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/metadados/'),
        'save_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
        'cod_tema': '60'
    },
    queue = worker_queue,
    dag = dag
)


# union_timeseries = PythonOperator(
#     task_id = "union_timeseries",
#     python_callable = ingest.union_timeseries,
#     op_kwargs = {
#         'source_path': adl.adl_full_url(ADL, workdir + '/stage/ipea_data/series/'),
#         'save_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/series/')
#     },
#     queue = worker_queue,
#     dag = dag
# )

# save_timeseries = PythonOperator(
#     task_id = "save_timeseries",
#     python_callable = ingest.save_timeseries,
#     op_kwargs = {
#         'source_path': adl.adl_full_url(ADL, workdir + '/trusted/ipea_data/series'),
#         'table_name': 'ipea_data_series'
#     },
#     queue = worker_queue,
#     dag = dag
# )

get_metadata >> save_metadata >> [get_timeseries_agropecuaria, get_timeseries_assistencia_social, get_timeseries_balanco_de_pagamentos, get_timeseries_cambio, get_timeseries_comercio_exterior, get_timeseries_consumo_e_vendas, get_timeseries_contas_nacionais, get_timeseries_contas_regionais, get_timeseries_correcao_monetaria, get_timeseries_demografia, get_timeseries_deputado_estadual, get_timeseries_deputado_federal, get_timeseries_desenvolvimento_humano, get_timeseries_economia_internacional, get_timeseries_educacao, get_timeseries_eleitorado, get_timeseries_emprego, get_timeseries_estoque_de_capital, get_timeseries_financas_publicas, get_timeseries_financeiras, get_timeseries_geografico, get_timeseries_governador, get_timeseries_habitacao, get_timeseries_idhm2000, get_timeseries_indicadores_sociais, get_timeseries_mercado_de_trabalho, get_timeseries_moeda_e_credito, get_timeseries_percepcao_e_expectativa, get_timeseries_populacao, get_timeseries_precos, get_timeseries_prefeito, get_timeseries_presidente, get_timeseries_producao, get_timeseries_projecoes, get_timeseries_renda, get_timeseries_salario_e_renda, get_timeseries_saude, get_timeseries_seguranca_publica, get_timeseries_senador, get_timeseries_sinopse_macroeconomica, get_timeseries_transporte, get_timeseries_vendas, get_timeseries_vereador]
[get_timeseries_agropecuaria, get_timeseries_assistencia_social, get_timeseries_balanco_de_pagamentos, get_timeseries_cambio, get_timeseries_comercio_exterior, get_timeseries_consumo_e_vendas, get_timeseries_contas_nacionais, get_timeseries_contas_regionais, get_timeseries_correcao_monetaria, get_timeseries_demografia, get_timeseries_deputado_estadual, get_timeseries_deputado_federal, get_timeseries_desenvolvimento_humano, get_timeseries_economia_internacional, get_timeseries_educacao, get_timeseries_eleitorado, get_timeseries_emprego, get_timeseries_estoque_de_capital, get_timeseries_financas_publicas, get_timeseries_financeiras, get_timeseries_geografico, get_timeseries_governador, get_timeseries_habitacao, get_timeseries_idhm2000, get_timeseries_indicadores_sociais, get_timeseries_mercado_de_trabalho, get_timeseries_moeda_e_credito, get_timeseries_percepcao_e_expectativa, get_timeseries_populacao, get_timeseries_precos, get_timeseries_prefeito, get_timeseries_presidente, get_timeseries_producao, get_timeseries_projecoes, get_timeseries_renda, get_timeseries_salario_e_renda, get_timeseries_saude, get_timeseries_seguranca_publica, get_timeseries_senador, get_timeseries_sinopse_macroeconomica, get_timeseries_transporte, get_timeseries_vendas, get_timeseries_vereador] 
#>> union_timeseries >> save_timeseries
