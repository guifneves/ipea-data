import airflow
import pandas as pd
from datetime import datetime, timedelta

import raizenlib.utils.adl as adl

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

    jdbc_url = ""
    conn_properties = {}

    current_date = datetime.now().strftime("%Y-%m-%d")
    spark = adl.get_adl_spark(args["source_path"])
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    df.show()
    #if df.count() > 0:
    #    df.write.jdbc(url=jdbc_url, table=args["table_name"], mode='overwrite', properties=conn_properties)

def get_timeseries(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea

    def get_from_ipea(code):
        import pyIpeaData as ipea
        print("==========", code)
        return ipea.get_serie(code[1])

    cod_temas = args["cod_temas"]
    print("Temas: " + str(cod_temas))

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["save_path"])
    df_md = spark.read.format("parquet").load(args["source_path"]).filter((F.col("ref_date") == current_date) & F.col("TEMCODIGO").isin(cod_temas))
    #df_md = df_md.limit(5)
    codes = list(map(lambda i: i.SERCODIGO, df_md.select("SERCODIGO").distinct().collect()))

    print(codes)
    #dfs = list(map(lambda code: get_from_ipea(code), list(enumerate(codes))))    
    dfs = spark.sparkContext.parallelize(list(enumerate(codes))).map(lambda x: get_from_ipea(x)).collect()

    df_pd = pd.concat(dfs, ignore_index = True)

    df = spark.createDataFrame(df_pd)
    df.printSchema()

    ref_date = datetime.now().strftime("%Y%m%d")
    save_path = "{}ref_date={}/range={}".format(args["save_path"], ref_date, args["range"])

    df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def union_timeseries(**args):
    import pyspark.sql.functions as F
    
    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["save_path"])

    df = spark.read.format("parquet").load(args["source_path"]).filter((F.col("ref_date") == current_date)).drop("range")

    df.show()

    df.write.partitionBy("SERCODIGO", "ref_date").parquet(args["save_path"], mode = "overwrite")

def save_timeseries(**args):
    import pyspark.sql.functions as F

    jdbc_url = ""
    conn_properties = {}

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["source_path"])
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    df.show()
    #if df.count() > 0:
    #    df.write.jdbc(url=jdbc_url, table=args["table_name"], mode='overwrite', properties=conn_properties)
