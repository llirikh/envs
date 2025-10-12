import time
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

hdfs_path = "hdfs://namenode:9000/data/openbeer/streaming_breweries"

counter = 0
while True:
    # создаём DataFrame с тестовыми данными
    df = pd.DataFrame({
        "NUM": [counter],
        "NAME": [f"Test_Brewery_{counter}"],
        "CITY": ["TestCity"],
        "STATE": ["TS"],
        "ID": [1000 + counter]
    })
    
    # конвертируем в Spark DataFrame
    sdf = spark.createDataFrame(df)
    
    # сохраняем как отдельный Parquet-файл (каждый файл — отдельная партия)
    sdf.write.mode("append").parquet(hdfs_path)
    
    counter += 1
    time.sleep(5)  # например, каждые 5 секунд
