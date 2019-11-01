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

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["source_path"])
    df = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    df.show()
    #if df.count() > 0:
    #    df.write.jdbc(url=jdbc_url, table=args["table_name"], mode='overwrite', properties=conn_properties)

def get_timeseries(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea

    # def get_from_ipea(code):
    #     import pyIpeaData as ipea
    #     print("==========", code)
    #     df_serie = ipea.get_serie(code[1])
    #     df_serie["VALVALOR"] = df_serie["VALVALOR"].astype("str")
    #     return df_serie

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

    # def get_from_ipea(code,save_path):
    #     import pyIpeaData as ipea
    #     print("==========", code)
        
    #     df_serie = ipea.get_serie(code[1])
    #     df_serie["VALVALOR"] = df_serie["VALVALOR"].astype("str")
        
    #     ref_date = datetime.now().strftime("%Y%m%d")
    #     df = spark.createDataFrame(df_serie)
        
    #     save_path = "{}ref_date={}/cod_tema={}".format(save_path, ref_date, code[1])

    #     df.coalesce(1).write.parquet(save_path, mode = "overwrite")

    cod_temas = args["cod_tema"]
    print("Temas: " + str(cod_temas))

    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["save_path"])
    df_md = spark.read.format("parquet").load(args["source_path"]).filter((F.col("ref_date") == current_date) & F.col("TEMCODIGO").isin([cod_temas]))
    #df_md = df_md.limit(5)
    codes = list(map(lambda i: i.SERCODIGO, df_md.select("SERCODIGO").distinct().collect()))

    print(codes)
    # dfs = list(map(lambda code: get_from_ipea(code), list(enumerate(codes))))    
    # dfs = spark.sparkContext.parallelize(list(enumerate(codes))).map(lambda x: get_from_ipea(x)).collect()
    #list(map(lambda code: get_from_ipea(code,args["save_path"]), list(enumerate(codes))))    
    client, path = adl.get_adl_client(args["save_path"])
    # list(map(lambda code: get_from_ipea(code, client, path), list(enumerate(codes))))
    spark.sparkContext.parallelize(list(enumerate(codes))).map(lambda x: get_from_ipea(x, client, path)).collect()

    # df_pd = pd.concat(dfs, ignore_index = True)

    # df = spark.createDataFrame(df_pd)
    # df.printSchema()

    # ref_date = datetime.now().strftime("%Y%m%d")
    # save_path = "{}ref_date={}/cod_tema={}".format(args["save_path"], ref_date, cod_temas)

    # df.coalesce(1).write.parquet(save_path, mode = "overwrite")

def union_timeseries(**args):
    import pyspark.sql.functions as F
    
    current_date = datetime.now().strftime("%Y%m%d")
    spark = adl.get_adl_spark(args["save_path"])

    df = spark.read.format("parquet").load(args["source_path"]).filter((F.col("ref_date") == current_date))

    df.show()

    df.write.partitionBy("ref_date","SERCODIGO").parquet(args["save_path"], mode = "overwrite")

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
