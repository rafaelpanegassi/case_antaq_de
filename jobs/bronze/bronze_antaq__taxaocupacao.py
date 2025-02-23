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


def copy_atracacao_files():
    source_bucket = os.getenv("RAW_BUCKET")
    destination_bucket = os.getenv("BRONZE_BUCKET")

    s3_client = create_s3_client()

    response = s3_client.list_objects_v2(
        Bucket=source_bucket,
        Prefix="year/",
        Delimiter='/'
    )

    subfolders = response.get('CommonPrefixes', [])

    for subfolder in subfolders:
        prefix = subfolder.get('Prefix')
        year_str = prefix.split('/')[1]

        resp_files = s3_client.list_objects_v2(
            Bucket=source_bucket,
            Prefix=prefix
        )
        contents = resp_files.get('Contents', [])

        for obj in contents:
            key = obj['Key']
            filename = key.split('/')[-1]

            expected_prefix = f"{year_str}TaxaOcupacao_{year_str}-"
            if not filename.startswith(expected_prefix):
                continue

            destination_key = f"taxa/taxaocupacao/{filename}"

            copy_source = {"Bucket": source_bucket, "Key": key}
            try:
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=destination_bucket,
                    Key=destination_key
                )
                logger.info(f"File copied successfully: {key} -> {destination_key}")
            except Exception as e:
                logger.error(f"Error copying file {key}: {e}")


def main():
    load_dotenv()
    logger.info("Starting Taxa Ocupacao copy script.")
    copy_atracacao_files()
    logger.info("Script finished.")


if __name__ == "__main__":
    main()
