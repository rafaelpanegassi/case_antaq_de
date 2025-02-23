import os
import datetime
from dotenv import load_dotenv
from loguru import logger
import boto3
from botocore.client import Config

def create_s3_client():
    required_vars = ["ENDPOINT_URL", "ACCESS_KEY", "SECRET_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    endpoint_url = os.getenv("ENDPOINT_URL")
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    region_name = os.getenv("REGION_NAME", "us-east-1")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name=region_name,
        config=Config(signature_version="s3v4")
    )
    s3_client.list_buckets()
    logger.info("S3 client initialized successfully.")
    return s3_client

def copy_file():
    today_str = datetime.datetime.now().strftime("%d_%m_%Y")

    source_bucket = os.getenv("SOURCE_BUCKET", "landing-zone")
    destination_bucket = os.getenv("DEST_BUCKET", "bronze")

    source_key = f"register/MercadoriaConteinerizada_register-{today_str}.txt"
    destination_key = f"mercadoria/MercadoriaConteinerizada_register-{today_str}.txt"

    s3_client = create_s3_client()
    copy_source = {"Bucket": source_bucket, "Key": source_key}

    try:
        s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        logger.info(f"Arquivo copiado com sucesso: {source_key} -> {destination_key}")
    except Exception as e:
        logger.error(f"Erro ao copiar o arquivo {source_key}: {e}")

def main():
    load_dotenv()
    logger.info("Iniciando o script de cópia: MercadoriaConteinerizada")
    copy_file()
    logger.info("Script finalizado.")

if __name__ == "__main__":
    main()
