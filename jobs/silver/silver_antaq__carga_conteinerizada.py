import os
import unicodedata
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf,
    col,
    trim,
    lower,
    current_timestamp,
    to_timestamp,
    lit
)
from pyspark.sql.types import StringType, IntegerType
from loguru import logger

load_dotenv()

def remove_accents_str(input_str):
    if input_str is None:
        return None
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])

remove_accents_udf = udf(remove_accents_str, StringType())

def normalize_col_name(col_name: str) -> str:
    col_name = col_name.strip()
    col_name = remove_accents_str(col_name)
    col_name = col_name.lower()
    col_name = col_name.replace(" ", "_")
    return col_name

if __name__ == "__main__":
    minio_endpoint   = os.getenv("ENDPOINT_URL")
    minio_access_key = os.getenv("MINIO_ROOT_USER")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    bronze_bucket    = os.getenv("BRONZE_BUCKET")
    silver_bucket    = os.getenv("SILVER_BUCKET")
    
    execution_date = datetime.now().strftime("%d_%m_%Y")

    spark = (
        SparkSession.builder
        .appName("SilverCargaConteinerizadaDelta")
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

    bronze_path = f"s3a://{bronze_bucket}/carga/carga_conteinerizada/*-{execution_date}.txt"
    logger.info(f"Reading bronze files: {bronze_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .csv(bronze_path)
    )

    old_cols = df.columns
    new_cols = [normalize_col_name(c) for c in old_cols]
    for old, new in zip(old_cols, new_cols):
        df = df.withColumnRenamed(old, new)

    for c, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(c, lower(trim(remove_accents_udf(col(c)))))

    data_columns = [
    ]
    for c in data_columns:
        if c in df.columns:
            df = df.withColumn(c, to_timestamp(col(c), "dd/MM/yyyy HH:mm:ss"))

    int_columns = [
        "idcarga",
        "cdmercadoriaconteinerizada",
        "vlpesocargaconteinerizada",
    ]
    for c in int_columns:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(IntegerType()))

    df = df.withColumn("_execution_date", lit(execution_date))
    df = df.withColumn("_processed_at", current_timestamp())

    delta_output_path = f"s3a://{silver_bucket}/carga_conteinerizada/"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_execution_date")
        .save(delta_output_path)
    )

    logger.info(f"Transformation complete. Delta table written to: {delta_output_path}")
    spark.stop()
