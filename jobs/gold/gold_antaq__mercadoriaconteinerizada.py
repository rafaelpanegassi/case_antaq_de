import os
import unicodedata
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col

load_dotenv()

if __name__ == "__main__":
    # Load environment variables
    minio_endpoint   = os.getenv("ENDPOINT_URL")
    minio_access_key = os.getenv("MINIO_ROOT_USER")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    silver_bucket    = os.getenv("SILVER_BUCKET")
    gold_bucket      = os.getenv("GOLD_BUCKET")

    # Initialize SparkSession with Delta support
    spark = (
        SparkSession.builder
        .appName("GoldMercadoriaConteinerizadaUpsert")
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )

    silver_table_path = f"s3a://{silver_bucket}/mercadoriaconteinerizada/"
    gold_table_path   = f"s3a://{gold_bucket}/mercadoriaconteinerizada/"

    logger.info(f"Reading data from Gold at: {silver_table_path}")
    df_silver = spark.read.format("delta").load(silver_table_path)

    table_exists = False
    try:
        _ = spark.read.format("delta").load(gold_table_path).limit(1)
        table_exists = True
    except:
        table_exists = False

    if not table_exists:
        logger.info("Gold table does not exist. Creating a new table partitioned by _execution_date.")
        (
            df_silver.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("_execution_date")
            .save(gold_table_path)
        )
        logger.info("Gold table created successfully!")
    else:
        logger.info("Gold table found. Starting upsert via MERGE...")

        delta_table = DeltaTable.forPath(spark, gold_table_path)

        upsert_condition = (
            "t.cdmercadoriaconteinerizada = s.cdmercadoriaconteinerizada"
        )
        (
            delta_table.alias("t")
            .merge(
                source=df_silver.alias("s"),
                condition=upsert_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Upsert completed successfully on the Gold table.")

    spark.stop()
