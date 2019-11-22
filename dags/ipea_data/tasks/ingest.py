import airflow
import pandas as pd
from datetime import datetime, timedelta

import raizenlib.utils.adl as adl

def get_connection_sql_server():
    from airflow.hooks.base_hook import BaseHook
    import json

    connection = BaseHook.get_connection("mssql_ipea")

    print("Get connection sql server")

    host = connection.host
    schema = connection.schema
    port = connection.port
    username = connection.login
    password = connection.password
    database_schema = json.loads(connection.extra)['database_schema']

    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(host, port, schema)
    connection_properties = {
        "user" : username,
        "password" : password,
        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    return jdbc_url, connection_properties, database_schema

def get_metadados(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea
    
    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_metadados()
    if type(df_pd) == type(None):
        raise ValueError('API return error!')

    df = spark.createDataFrame(df_pd)    
    ref_date = datetime.now().strftime("%Y%m%d")
    
    save_path = "{}REF_DATE={}".format(args["save_path"], ref_date)
    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def save_metadados(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties, database_schema = get_connection_sql_server()            
    spark = adl.get_adl_spark(args["source_path"])
    
    current_date = datetime.now().strftime("%Y%m%d")
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("REF_DATE") == current_date)

    temas =\
    [
        [1, 'timeserie_producao'], [2, 'timeserie_consumo_e_vendas'], [3, 'timeserie_moeda_e_credito'], [5, 'timeserie_comercio_exterior'], 
        [6, 'timeserie_financas_publicas'], [7, 'timeserie_cambio'], [8, 'timeserie_contas_nacionais'], [9, 'timeserie_precos'], [10, 'timeserie_balanco_de_pagamentos'], 
        [11, 'timeserie_economia_internacional'], [12, 'timeserie_emprego'], [13, 'timeserie_salario_e_renda'], [14, 'timeserie_populacao'], 
        [15, 'timeserie_indicadores_sociais'], [16, 'timeserie_projecoes'], [17, 'timeserie_sinopse_macroeconomica'], [19, 'timeserie_estoque_de_capital'],
        [20, 'timeserie_seguranca_publica'], [23, 'timeserie_assistencia_social'], [24, 'timeserie_correcao_monetaria'], [26, 'timeserie_vendas'], 
        [27, 'timeserie_percepcao_e_expectativa'], [28, 'timeserie_agropecuaria'], [29, 'timeserie_educacao'], [30, 'timeserie_renda'], [31, 'timeserie_habitacao'], 
        [32, 'timeserie_geografico'], [33, 'timeserie_transporte'], [37, 'timeserie_demografia'], [38, 'timeserie_desenvolvimento_humano'], 
        [39, 'timeserie_financeiras'], [40, 'timeserie_mercado_de_trabalho'], [41, 'timeserie_saude'], [54, 'timeserie_deputado_estadual'],
        [55, 'timeserie_deputado_federal'], [56, 'timeserie_governador'], [57, 'timeserie_prefeito'], [58, 'timeserie_presidente'], [59, 'timeserie_senador'], 
        [60, 'timeserie_vereador'], [63, 'timeserie_eleitorado'], [79, 'timeserie_idhm2000'], [81, 'timeserie_contas_regionais']
    ]

    df_temas = spark.createDataFrame(temas,["TEMCODIGO", "TABELA"])

    df = df \
        .join(df_temas, df.TEMCODIGO == df_temas.TEMCODIGO, "inner") \
        .select(
            df.BASNOME,
            df.FNTNOME,
            df.FNTSIGLA,
            df.FNTURL,
            df.MULNOME,
            df.PAICODIGO,
            df.PERNOME,
            df.SERATUALIZACAO,
            df.SERCODIGO,
            df.SERCOMENTARIO,
            df.SERNOME,
            df.SERNUMERICA,
            df.SERSTATUS,
            df.TEMCODIGO,
            df.UNINOME,
            df_temas.TABELA
        )
    
    table = "{}.{}".format(database_schema, args["table_name"])
    df.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)


def get_territorios(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea
    
    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_territorios()
    if type(df_pd) == type(None):
        raise ValueError('API return error!')

    df = spark.createDataFrame(df_pd)    
    ref_date = datetime.now().strftime("%Y%m%d")
    
    save_path = "{}REF_DATE={}".format(args["save_path"], ref_date)
    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def save_territorios(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties, database_schema = get_connection_sql_server()            
    spark = adl.get_adl_spark(args["source_path"])
    
    current_date = datetime.now().strftime("%Y%m%d")
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("REF_DATE") == current_date)
    
    table = "{}.{}".format(database_schema, args["table_name"])
    df.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)


def get_temas(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea
    
    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_temas()
    if type(df_pd) == type(None):
        raise ValueError('API return error!')

    df = spark.createDataFrame(df_pd)    
    ref_date = datetime.now().strftime("%Y%m%d")
    
    save_path = "{}REF_DATE={}".format(args["save_path"], ref_date)
    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def save_temas(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties, database_schema = get_connection_sql_server()            
    spark = adl.get_adl_spark(args["source_path"])
    
    current_date = datetime.now().strftime("%Y%m%d")
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("REF_DATE") == current_date)
    
    table = "{}.{}".format(database_schema, args["table_name"])
    df.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)


def get_paises(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea
    
    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_paises()
    if type(df_pd) == type(None):
        raise ValueError('API return error!')

    df = spark.createDataFrame(df_pd)    
    ref_date = datetime.now().strftime("%Y%m%d")
    
    save_path = "{}REF_DATE={}".format(args["save_path"], ref_date)
    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def save_paises(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties, database_schema = get_connection_sql_server()            
    spark = adl.get_adl_spark(args["source_path"])
    
    current_date = datetime.now().strftime("%Y%m%d")
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("REF_DATE") == current_date)
    
    table = "{}.{}".format(database_schema, args["table_name"])
    df.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)


def get_fontes(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea
    
    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_fontes()
    if type(df_pd) == type(None):
        raise ValueError('API return error!')

    df = spark.createDataFrame(df_pd)    
    ref_date = datetime.now().strftime("%Y%m%d")
    
    save_path = "{}REF_DATE={}".format(args["save_path"], ref_date)
    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def save_fontes(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties, database_schema = get_connection_sql_server()            
    spark = adl.get_adl_spark(args["source_path"])
    
    current_date = datetime.now().strftime("%Y%m%d")
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("REF_DATE") == current_date)
    
    table = "{}.{}".format(database_schema, args["table_name"])
    df.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)


def get_timeseries(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea

    def get_from_ipea(code, client,save_path):
        import pyIpeaData as ipea
        print("==========", code)

        sercodigo = code[0]
        temcodigo = code[1]
        sernumerica = code[2]
        
        df_serie = ipea.get_serie(sercodigo)
        if type(df_serie) == type(None):
            raise ValueError('API return error!')

        df_serie["TEMCODIGO"] = temcodigo
        df_serie = df_serie.drop('SERCODIGO', 1)        

        if sernumerica == 0:
            df_serie["DESCVALOR"] = df_serie["VALVALOR"]
            df_serie["VALVALOR"] = pd.np.nan
        else:
            df_serie["DESCVALOR"] = pd.np.nan

        ref_date = datetime.now().strftime("%Y%m%d")
        save_path = "{}TEMCODIGO={}/REF_DATE={}/SERCODIGO={}/{}.parquet".format(save_path, temcodigo, ref_date, sercodigo, sercodigo)

        df_serie["VALVALOR"] = df_serie["VALVALOR"].astype("float")
        df_serie["DESCVALOR"] = df_serie["DESCVALOR"].astype("str")
        #df_serie["VALDATA"] = df_serie["VALDATA"].astype('datetime64[ns]')

        with client.open(save_path, 'wb') as f:        
            df_serie.to_parquet(f)
        del df_serie


    cod_temas = args["cod_tema"]
    print("Temas: " + str(cod_temas))

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["save_path"])
    df_md = spark.read.format("parquet").load(args["source_path"]).filter((F.col("REF_DATE") == current_date) & F.col("TEMCODIGO").isin([cod_temas]))    

    #df_md = df_md.limit(5)
    codes = list(map(lambda i: [i.SERCODIGO,i.TEMCODIGO, i.SERNUMERICA], df_md.select("SERCODIGO", "TEMCODIGO", "SERNUMERICA").distinct().collect()))
  
    client, path = adl.get_adl_client(args["save_path"])
    spark.sparkContext.parallelize(codes).map(lambda x: get_from_ipea(x, client, path)).collect()

def save_timeseries(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties, database_schema = get_connection_sql_server()

    cod_tema = args["cod_tema"]
    name_tema = args["name_tema"]
    source_path_series = args["source_path_series"]
    spark = adl.get_adl_spark(args["source_path_series"])    

    source_path = "{}TEMCODIGO={}".format(source_path_series, cod_tema)
    print("Read file ", source_path)
    df = spark.read.format("parquet").load(source_path)

    print("Transformations") 
    df = df.withColumn("VALVALOR", F.when(F.col("VALVALOR") == "nan", None).otherwise(F.col("VALVALOR")))
    df = df.withColumn("DESCVALOR", F.when(F.col("DESCVALOR") == "nan", None).otherwise(F.col("DESCVALOR")))
    df = df.withColumn("NIVNOME", F.when(F.col("NIVNOME") == "", None).otherwise(F.col("NIVNOME")))
    df = df.withColumn("TERCODIGO", F.when(F.col("TERCODIGO") == "", None).otherwise(F.col("TERCODIGO")))
    df = df.withColumn("VALDATA", F.col("VALDATA").cast("date"))

    df = df.select(
        F.col("NIVNOME"),
        F.col("SERCODIGO"),
        F.col("TERCODIGO"),
        F.col("TEMCODIGO"),
        F.col("VALDATA"),
        F.col("VALVALOR"),
        F.col("DESCVALOR"),
        F.col("REF_DATE")
    )

    print("Save table ", name_tema)
    table = "{}.{}{}".format(database_schema,"timeserie_", name_tema)
    df.write.jdbc(url=jdbc_url, table=table, mode='overwrite', properties=connection_properties)


def clear_timeseries(**args):
    adl.remove_on_adl(args["source_path"])