import airflow
import pandas as pd
from datetime import datetime, timedelta

import raizenlib.utils.adl as adl

def get_connection_sql_server():
    from airflow.hooks.base_hook import BaseHook
    connection = BaseHook.get_connection("mssql_default")

    print("Get connection sql server")

    host = connection.host
    schema = connection.schema
    port = connection.port
    username = connection.login
    password = connection.password

    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(host, port, schema)
    connection_properties = {
        "user" : username,
        "password" : password,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    return jdbc_url, connection_properties

def get_metadata(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea
    
    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_metadados()

    df = spark.createDataFrame(df_pd)    
    ref_date = datetime.now().strftime("%Y%m%d")
    
    save_path = "{}REF_DATE={}".format(args["save_path"], ref_date)
    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def save_metadata(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties = get_connection_sql_server()            
    spark = adl.get_adl_spark(args["source_path"])
    
    current_date = datetime.now().strftime("%Y%m%d")
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("REF_DATE") == current_date)
    
    table = "{}.{}".format(args["schema_name"], args["table_name"])
    df.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)

def clear_timeseries(**args):
    adl.remove_on_adl(args["source_path"])

def get_timeseries(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea

    def get_from_ipea(code, client,save_path):
        import pyIpeaData as ipea
        print("==========", code)

        sercodigo = code[0]
        temcodigo = code[1]
        
        df_serie = ipea.get_serie(sercodigo)        
        df_serie = df_serie.drop('SERCODIGO', 1)
        df_serie["VALVALOR"] = df_serie["VALVALOR"].astype("str")

        ref_date = datetime.now().strftime("%Y%m%d")
        save_path = "{}REF_DATE={}/TEMCODIGO={}/SERCODIGO={}/{}.parquet".format(save_path, ref_date, temcodigo, sercodigo, sercodigo)

        with client.open(save_path, 'wb') as f:        
            df_serie.to_parquet(f)
        del df_serie


    cod_temas = args["cod_tema"]
    print("Temas: " + str(cod_temas))

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["save_path"])
    df_md = spark.read.format("parquet").load(args["source_path"]).filter((F.col("REF_DATE") == current_date) & F.col("TEMCODIGO").isin([cod_temas]))    

    #df_md = df_md.limit(5)
    codes = list(map(lambda i: [i.SERCODIGO,i.TEMCODIGO], df_md.select("SERCODIGO", "TEMCODIGO").distinct().collect()))
  
    client, path = adl.get_adl_client(args["save_path"])
    spark.sparkContext.parallelize(codes).map(lambda x: get_from_ipea(x, client, path)).collect()

def save_timeseries(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties = get_connection_sql_server()
    
    spark = adl.get_adl_spark(args["source_path_series"])

    temas =\
        [
            [1, 'producao'], [2, 'consumo_e_vendas'], [3, 'moeda_e_credito'], [5, 'comercio_exterior'], [6, 'financas_publicas'], [7, 'cambio'],
            [8, 'contas_nacionais'], [9, 'precos'], [10, 'balanco_de_pagamentos'], [11, 'economia_internacional'], [12, 'emprego'], [13, 'salario_e_renda'],
            [14, 'populacao'], [15, 'indicadores_sociais'], [16, 'projecoes'], [17, 'sinopse_macroeconomica'], [19, 'estoque_de_capital'],
            [20, 'seguranca_publica'], [23, 'assistencia_social'], [24, 'correcao_monetaria'], [26, 'vendas'], [27, 'percepcao_e_expectativa'],
            [28, 'agropecuaria'], [29, 'educacao'], [30, 'renda'], [31, 'habitacao'], [32, 'geografico'], [33, 'transporte'], [37, 'demografia'],
            [38, 'desenvolvimento_humano'], [39, 'financeiras'], [40, 'mercado_de_trabalho'], [41, 'saude'], [54, 'deputado_estadual'],
            [55, 'deputado_federal'], [56, 'governador'], [57, 'prefeito'], [58, 'presidente'], [59, 'senador'], [60, 'vereador'], [63, 'eleitorado'],
            [79, 'idhm2000'], [81, 'contas_regionais']
        ]

    current_date = datetime.now().strftime("%Y%m%d")    
    df = spark.read.format("parquet").load(args["source_path_series"])

    df.cache()
    df.head(1)

    print("Save tables")
    for cod_tema, desc_tema in temas:
        print("Save table ", desc_tema)
        table = "{}.{}{}".format(args["schema_name"],"timeserie_", desc_tema)
        df.filter(F.col("TEMCODIGO") == cod_tema).write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)

    df_temas = spark.createDataFrame(temas,["TEMCODIGO", "TEMNOME"]) 

    df_relacionamento = df.select(F.col("SERCODIGO"), F.col("TEMCODIGO")).distinct()
    df_relacionamento = df_relacionamento \
        .join(df_temas, df_relacionamento.TEMCODIGO == df_temas.TEMCODIGO, "inner") \
        .select(
            df_relacionamento.SERCODIGO,
            df_temas.TEMCODIGO,
            df_temas.TEMNOME
        )


    print("Save table relationship")
    table = "{}.{}".format(args["schema_name"],args["relationship_table"],)
    df_relacionamento.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)

    df.unpersist()