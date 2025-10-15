from pyspark.sql import SparkSession, types as T
import pandas as pd
import time

spark = SparkSession.builder.getOrCreate()

hdfs_path = "hdfs://namenode:9000/data/openbeer/streaming_breweries"

schema = T.StructType([
    T.StructField("NUM", T.IntegerType(), True),
    T.StructField("NAME", T.StringType(), True),
    T.StructField("CITY", T.StringType(), True),
    T.StructField("STATE", T.StringType(), True),
    T.StructField("ID", T.IntegerType(), True),
])

counter = 0
while True:
    df = pd.DataFrame({
        "NUM": [counter],
        "NAME": [f"Test_Brewery_{counter}"],
        "CITY": ["TestCity"],
        "STATE": ["TS"],
        "ID": [1000 + counter]
    })

    sdf = spark.createDataFrame(df, schema=schema)
    sdf.write.mode("append").parquet(hdfs_path)

    counter += 1
    time.sleep(5)
