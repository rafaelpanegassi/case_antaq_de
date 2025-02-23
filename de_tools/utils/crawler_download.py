import datetime
import os
import tempfile
from zipfile import ZipFile

import requests
from loguru import logger

class CrawlerDownload:
    def __init__(self, s3_client, bucket_name, headers=None):
        """Initializes the crawler with an S3 client, bucket, and optional HTTP headers."""
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.headers = headers or {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    def validate_url(self, url):
        """Validates if the URL is accessible."""
        try:
            response = requests.head(url, timeout=5, headers=self.headers)
            if response.status_code == 403:
                logger.warning(f"HEAD request returned 403 for URL: {url}. Continuing execution.")
            else:
                response.raise_for_status()
            logger.info(f"URL successfully validated: {url}")
        except Exception as e:
            logger.error(f"Error validating URL: {url}. Details: {e}")
            raise

    def download_and_extract_zip(self, url, identifier, dest_folder_in_bucket):
        """Downloads a ZIP file, extracts the files, and uploads them to S3."""
        logger.info(f"Attempting to download: {url}")
        response = requests.get(url, timeout=10, headers=self.headers)
        response.raise_for_status()
        logger.info(f"Download completed with status: {response.status_code}")

        today_str = datetime.datetime.now().strftime("%d_%m_%Y")
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = os.path.join(tmp_dir, f"{identifier}.zip")
            with open(zip_path, "wb") as f:
                f.write(response.content)
            logger.info(f"ZIP downloaded to: {zip_path}")

            with ZipFile(zip_path, 'r') as zip_ref:
                for info in zip_ref.infolist():
                    if info.is_dir():
                        continue

                    original_name = info.filename
                    base_original_name = os.path.basename(original_name)
                    root_name, ext = os.path.splitext(base_original_name)
                    new_file_name = f"{root_name}_{identifier}-{today_str}{ext}"

                    extracted_path = os.path.join(tmp_dir, new_file_name)
                    with zip_ref.open(info) as extracted_file, open(extracted_path, "wb") as out_file:
                        out_file.write(extracted_file.read())

                    object_name_in_bucket = f"{dest_folder_in_bucket}/{new_file_name}"
                    logger.info(f"Uploading to: {self.bucket_name}/{object_name_in_bucket}")
                    self.s3_client.upload_file(Filename=extracted_path, Bucket=self.bucket_name, Key=object_name_in_bucket)

        logger.info(f"All files from '{url}' have been processed successfully.")
