from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import time, uuid

# Initialize Spark Session with Iceberg and ClickHouse configurations
spark = (
    SparkSession.builder
    .appName("writing_to_iceberg")
    .master("spark://spark-master:7077")
    .config(
        "spark.jars.packages",
        ",".join([
            # s3 (AWS SDK v2)
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            # iceberg packages
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0", 
            "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
        ])
    )
    # iceberg confs
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") 
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") 
    .config("spark.sql.catalog.iceberg.type", "rest") 
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg:8181") 
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") 
    .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg/") 
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") 
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    # s3 confs
    .config("spark.hadoop.fs.s3a.access.key", "minio") 
    .config("spark.hadoop.fs.s3a.secret.key", "minio-password") 
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") 
    .config("spark.hadoop.fs.s3a.path.style.access", "true") 
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # Чтобы сессия не занимала все воркеры
    .config("spark.cores.max", "3")
    .getOrCreate()
)

for i in range(1, 100):
    record_id = str(uuid.uuid4())
    df = spark.createDataFrame(
        [(record_id, f"test_record_{i}")],
        ["id", "name"]
    ).withColumn("hdttm", current_timestamp())

    df.writeTo("iceberg.test_schema.streaming_test_table").append()
    print(f"Inserted record: id={record_id}, name=test_record_{i}")
    time.sleep(1.5)

spark.stop()