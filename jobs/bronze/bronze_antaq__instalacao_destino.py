import datetime
import os

import boto3
from botocore.client import Config
from dotenv import load_dotenv
from loguru import logger


def create_s3_client():
    required_vars = ["ENDPOINT_URL", "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    endpoint_url = os.getenv("ENDPOINT_URL")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        config=Config(signature_version="s3v4")
    )
    s3_client.list_buckets()
    logger.info("S3 client initialized successfully.")
    return s3_client


def copy_file():
    # Dynamic date in the format "DD_MM_YYYY"
    today_str = datetime.datetime.now().strftime("%d_%m_%Y")

    source_bucket = os.getenv("RAW_BUCKET")
    destination_bucket = os.getenv("BRONZE_BUCKET")

    # Build the filename using the dynamic date
    source_key = f"register/Instalacao_Destino_register-{today_str}.txt"
    destination_key = f"instalacao/instalacao_destino/Instalacao_Destino_register-{today_str}.txt"

    s3_client = create_s3_client()
    copy_source = {"Bucket": source_bucket, "Key": source_key}

    try:
        s3_client.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
        logger.info(f"File copied successfully: {source_key} -> {destination_key}")
    except Exception as e:
        logger.error(f"Error copying file {source_key}: {e}")


def main():
    load_dotenv()
    logger.info("Starting copy script: Instalacao_Destino")
    copy_file()
    logger.info("Script finished.")


if __name__ == "__main__":
    main()
