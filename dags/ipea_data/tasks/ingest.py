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
    ref_date = datetime.now().strftime("%Y-%m-%d")
    print("*" * 80, args["save_path"])
    df.coalesce(1).write \
        .format("parquet") \
        .option("mode", "append") \
        .save(args["save_path"] + "/ref_date=" + ref_date)

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

def get_from_ipea(code):
    import pyIpeaData as ipea
    print("==========", code)
    return ipea.get_serie(code[1])

def get_timeseries(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea

    current_date = datetime.now().strftime("%Y-%m-%d")
    spark = adl.get_adl_spark(args["save_path"])
    df_md = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    codes = list(map(lambda i: i.SERCODIGO, df_md.select("SERCODIGO").distinct().collect()))

    dfs = list(map(lambda code: get_from_ipea(code), list(enumerate(codes))))

    df_pd = pd.concat(dfs, ignore_index = True)

    df = spark.createDataFrame(df_pd)
    df.printSchema()
    df = df.withColumn("ref_date", F.lit(datetime.now().strftime("%Y-%m-%d")))
    df.write \
        .partitionBy("SERCODIGO", "ref_date") \
        .parquet(args["save_path"], mode = "append")

def save_timeseries(**args):
    import pyspark.sql.functions as F

    jdbc_url = ""
    conn_properties = {}

    current_date = datetime.now().strftime("%Y-%m-%d")
    spark = adl.get_adl_spark(args["source_path"])
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    df.show()
    #if df.count() > 0:
    #    df.write.jdbc(url=jdbc_url, table=args["table_name"], mode='overwrite', properties=conn_properties)
