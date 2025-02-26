from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MinIO settings (using S3-compatible endpoint)
minio_endpoint = os.getenv("ENDPOINT_URL")
minio_access_key = os.getenv("MINIO_ROOT_USER")
minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD")
gold_bucket = os.getenv("GOLD_BUCKET")

# MySQL settings
mysql_config = {
    "url": "jdbc:mysql://localhost:3306/antaq",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "root",
    "password": "rootmysql"
}

# Configure SparkSession for Delta Lake and MinIO compatibility
spark = SparkSession.builder \
    .appName("Delta to MySQL") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,mysql:mysql-connector-java:8.0.33,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.default.parallelism", "4") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Function to load Delta data from MinIO and save it to MySQL
def load_to_mysql(table_path, table_name):
    try:
        # Read Delta files directly from MinIO
        df = spark.read.format("delta").load(f"s3a://{gold_bucket}/{table_path}")
        
        # Print basic information (optional, for debugging)
        print(f"Loading table {table_name} with {df.count()} records")
        
        # Save to MySQL
        df.write \
            .format("jdbc") \
            .option("url", mysql_config["url"]) \
            .option("driver", mysql_config["driver"]) \
            .option("dbtable", table_name) \
            .option("user", mysql_config["user"]) \
            .option("password", mysql_config["password"]) \
            .mode("overwrite") \
            .option("batchsize", "1000") \
            .save()
        
        print(f"Table {table_name} loaded successfully into MySQL!")
        
    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")

if __name__ == "__main__":
    # List of (table_path, table_name) tuples
    tables = [
        ("atracacao", "atracacao"),
        ("carga", "carga"),
        ("carga_conteinerizada", "carga_conteinerizada"),
        ("carga_hidrovia", "carga_hidrovia"),
        ("carga_regiao", "carga_regiao"),
        ("carga_rio", "carga_rio"),
        ("instalacao_destino", "instalacao_destino"),
        ("instalacao_origem", "instalacao_origem"),
        ("mercadoria", "mercadoria"),
        ("mercadoriaconteinerizada", "mercadoriaconteinerizada"),
        ("taxaocupacao", "taxaocupacao"),
        ("taxaocupacaocomcarga", "taxaocupacaocomcarga"),
        ("taxaocupacaotoatracacao", "taxaocupacaotoatracacao"),
        ("temposatracacao", "temposatracacao"),
        ("temposatracacaoparalisacao", "temposatracacaoparalisacao"),
    ]
    
    # Iterate over the list and load each table
    for table_path, table_name in tables:
        load_to_mysql(table_path, table_name)

    # Stop the Spark session
    spark.stop()
