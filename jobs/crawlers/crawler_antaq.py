import os
import boto3
from dotenv import load_dotenv
from loguru import logger
from de_tools.utils.crawler_download import CrawlerDownload

load_dotenv()

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
        region_name=region_name
    )
    s3_client.list_buckets()  # Test connection
    logger.info("S3 client initialized successfully.")
    return s3_client

def main():
    logger.info("Starting ANTAQ crawler...")
    logger.info(f"Variables: ENDPOINT_URL={os.getenv('ENDPOINT_URL')}, LANDING_BUCKET={os.getenv('LANDING_BUCKET')}, BASE_URL={os.getenv('BASE_URL')}")

    # Initialize S3 client and environment variables
    s3_client = create_s3_client()
    bucket_name = os.getenv("LANDING_BUCKET")
    if not bucket_name:
        raise ValueError("LANDING_BUCKET not set in .env file.")
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL not set in .env file.")

    # Create an instance of CrawlerDownload
    crawler = CrawlerDownload(s3_client, bucket_name)

    # Test initial connection with a known file
    logger.info("Testing initial connection with a known file...")
    test_url = f"{base_url}/2021.zip"
    crawler.validate_url(test_url)

    # Process years
    years = [2021, 2022, 2023]
    for year in years:
        zip_url = f"{base_url}/{year}.zip"
        destination_folder = f"year/{year}"
        crawler.download_and_extract_zip(zip_url, year, destination_folder)

    # Process register files
    register_files = [
        "instalacaoOrigem.zip",
        "instalacaoDestino.zip",
        "Mercadoria.zip",
        "MercadoriaConteinerizada.zip"
    ]
    for file_name in register_files:
        zip_url = f"{base_url}/{file_name}"
        destination_folder = "register"
        crawler.download_and_extract_zip(zip_url, "register", destination_folder)

    logger.info("ANTAQ crawler completed successfully!")

if __name__ == "__main__":
    main()