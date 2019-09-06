from datetime import datetime, date
import pandas as pd
import multiprocessing as mp

import raizenlib.utils.adl as adl

def fn_get_metadata(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea

    spark = adl.get_adl_spark(args["save_path"])
    df_pd = ipea.get_metadados()
    df = spark.createDataFrame(df_pd)
    df = df.withColumn("ref_date", F.lit(datetime.now().strftime("%Y-%m-%d")))
    print("*" * 80, args["save_path"])
    df.coalesce(1).write \
        .partitionBy("ref_date") \
        .format("parquet") \
        .save(args["save_path"])

def fn_get_timeseries(**args):
    import pyspark.sql.functions as F
    import pyIpeaData as ipea    

    current_date = datetime.now().strftime("%Y-%m-%d")

    spark = adl.get_adl_spark(args["save_path"])
    df_md = spark.read.format("parquet").load(args["source_path"]).filter(F.col("ref_date") == current_date)
    
    codes = list(map(lambda i: i.SERCODIGO, df_md.select("SERCODIGO").distinct().collect()))
    client, path = adl.get_adl_client(args["save_path"])
     
    # pool = mp.Pool(mp.cpu_count())
    # [pool.apply(fn_save_pandas_on_data_lake, args=(client,path,x)) for x in codes]    
    # pool.close()     

    list(map(lambda x: fn_save_pandas_on_data_lake(client, path, x), codes))         


def fn_save_pandas_on_data_lake(client,save_path, code):
    import pyIpeaData as ipea

    df_pd = ipea.get_serie(code)    
    with client.open(save_path + "ref_date=" + date.today().strftime("%Y-%m-%d") + "/" + code + ".parquet", 'wb') as f:        
        df_pd.to_parquet(f)