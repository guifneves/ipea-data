import airflow
import pandas as pd
from datetime import datetime, timedelta

import raizenlib.utils.adl as adl

def get_connection_sql_server():
    from airflow.hooks.base_hook import BaseHook
    connection = BaseHook.get_connection("mssql_autosugar")

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
    
    save_path = "{}ref_date={}".format(args["save_path"], ref_date)
    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def save_metadata(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties = get_connection_sql_server()

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["source_path"])
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    df.show()
    df.write.jdbc(url=jdbc_url, table=args["table_name"], mode='overwrite', properties=connection_properties)

def clear_timeseries(**args):
    adl.remove_on_adl(args["source_path"])

def get_timeseries(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea

    def get_from_ipea(code, client,save_path):
        import pyIpeaData as ipea
        print("==========", code)
        
        df_serie = ipea.get_serie(code[1])        
        df_serie = df_serie.drop('SERCODIGO', 1)
        df_serie["VALVALOR"] = df_serie["VALVALOR"].astype("str")
        
        ref_date = datetime.now().strftime("%Y%m%d")
        save_path = "{}ref_date={}/SERCODIGO={}/{}.parquet".format(save_path, ref_date, code[1], code[1])
        
        with client.open(save_path, 'wb') as f:        
            df_serie.to_parquet(f)
        del df_serie


    cod_temas = args["cod_tema"]
    print("Temas: " + str(cod_temas))

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["save_path"])
    df_md = spark.read.format("parquet").load(args["source_path"]).filter((F.col("ref_date") == current_date) & F.col("TEMCODIGO").isin([cod_temas]))    

    # df_md = df_md.limit(5)
    codes = list(map(lambda i: i.SERCODIGO, df_md.select("SERCODIGO").distinct().collect()))
  
    client, path = adl.get_adl_client(args["save_path"])
    spark.sparkContext.parallelize(list(enumerate(codes))).map(lambda x: get_from_ipea(x, client, path)).collect()

def save_timeseries(**args):
    import pyspark.sql.functions as F

    jdbc_url, connection_properties = get_connection_sql_server()
    
    spark = adl.get_adl_spark(args["source_path"])
    # current_date = datetime.now().strftime("%Y%m%d")
    # df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    df = spark.read.format("parquet").load(args["source_path"])
    df.write.jdbc(url=jdbc_url, table=args["table_name"], mode='overwrite', properties=connection_properties)
