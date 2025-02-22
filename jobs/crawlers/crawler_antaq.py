import datetime
import os
import tempfile
from zipfile import BadZipFile, ZipFile

import boto3
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

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
    s3_client.list_buckets()  # Testa conexão
    logger.info("S3 client initialized successfully.")
    return s3_client

def validate_url(url):
    try:
        response = requests.head(url, timeout=5, headers=HEADERS)
        # Se o HEAD retornar 403, registra aviso e continua, pois o download pode funcionar normalmente.
        if response.status_code == 403:
            logger.warning(f"HEAD request retornou 403 para URL: {url}. Continuando a execução.")
        else:
            response.raise_for_status()
        logger.info(f"URL validada com sucesso: {url}")
    except Exception as e:
        logger.error(f"Erro ao validar URL: {url}. Detalhes: {e}")
        raise

def download_and_extract_zip(url, year, s3_client, bucket_name, dest_folder_in_bucket):
    logger.info(f"Tentando baixar: {url}")
    response = requests.get(url, timeout=10, headers=HEADERS)
    response.raise_for_status()
    logger.info(f"Download concluído com status: {response.status_code}")

    today_str = datetime.datetime.now().strftime("%d_%m_%Y")
    with tempfile.TemporaryDirectory() as tmp_dir:
        zip_path = os.path.join(tmp_dir, f"{year}.zip")
        with open(zip_path, "wb") as f:
            f.write(response.content)
        logger.info(f"ZIP baixado em: {zip_path}")

        with ZipFile(zip_path, 'r') as zip_ref:
            for info in zip_ref.infolist():
                if info.is_dir():
                    continue

                original_name = info.filename
                base_original_name = os.path.basename(original_name)
                root_name, ext = os.path.splitext(base_original_name)
                new_file_name = f"{root_name}_{year}-{today_str}{ext}"

                extracted_path = os.path.join(tmp_dir, new_file_name)
                with zip_ref.open(info) as extracted_file, open(extracted_path, "wb") as out_file:
                    out_file.write(extracted_file.read())

                object_name_in_bucket = f"{dest_folder_in_bucket}/{new_file_name}"
                logger.info(f"Upload para: {bucket_name}/{object_name_in_bucket}")
                s3_client.upload_file(Filename=extracted_path, Bucket=bucket_name, Key=object_name_in_bucket)

    logger.info(f"Todos os arquivos de '{url}' processados com sucesso.")

def main():
    logger.info("Iniciando crawler ANTAQ...")
    logger.info(f"Variáveis: ENDPOINT_URL={os.getenv('ENDPOINT_URL')}, LANDING_BUCKET={os.getenv('LANDING_BUCKET')}, BASE_URL={os.getenv('BASE_URL')}")

    s3_client = create_s3_client()
    bucket_name = os.getenv("LANDING_BUCKET")
    if not bucket_name:
        raise ValueError("LANDING_BUCKET not set in .env file.")
    base_url = os.getenv("BASE_URL")
    if not base_url:
        raise ValueError("BASE_URL not set in .env file.")

    logger.info("Testando conexão inicial com um arquivo conhecido...")
    # Valida a URL testando o acesso a 2021.zip (arquivo conhecido)
    test_url = f"{base_url}/2021.zip"
    validate_url(test_url)

    years = [2021, 2022, 2023]
    for year in years:
        zip_url = f"{base_url}/{year}.zip"
        destination_folder = f"year/{year}"
        download_and_extract_zip(zip_url, year, s3_client, bucket_name, destination_folder)

    register_files = [
        "instalacaoOrigem.zip",
        "instalacaoDestino.zip",
        "Mercadoria.zip",
        "MercadoriaConteinerizada.zip"
    ]
    for file_name in register_files:
        zip_url = f"{base_url}/{file_name}"
        destination_folder = "register"
        download_and_extract_zip(zip_url, "register", s3_client, bucket_name, destination_folder)

    logger.info("Crawler ANTAQ concluído com sucesso!")

if __name__ == "__main__":
    main()
