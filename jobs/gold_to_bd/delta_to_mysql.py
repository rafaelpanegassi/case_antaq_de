import boto3
import pandas as pd
import os
from dotenv import load_dotenv
import pymysql
from sqlalchemy import create_engine

# Carrega variáveis de ambiente
load_dotenv()

# Configurações do MinIO (usando S3-compatible endpoint)
minio_endpoint = os.getenv("ENDPOINT_URL")
minio_access_key = os.getenv("MINIO_ROOT_USER")
minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD")
gold_bucket = os.getenv("GOLD_BUCKET")

# Configurações do MySQL
mysql_config = {
    "host": "localhost",
    "database": "antaq",
    "user": "root",
    "password": "rootmysql",
    "port": 3306
}

# Função para conectar ao MinIO (via boto3) e baixar os arquivos
def download_from_minio(bucket, prefix, local_dir="/tmp"):
    # Configura o cliente S3 para o MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        use_ssl=False  # Ajuste para True se usar HTTPS
    )
    
    # Cria o diretório local se não existir
    os.makedirs(local_dir, exist_ok=True)
    
    # Lista os objetos no bucket com o prefixo especificado
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            object_key = obj['Key']
            # Extrai o nome do arquivo local a partir do caminho no bucket
            local_file = os.path.join(local_dir, os.path.basename(object_key))
            # Baixa o arquivo
            s3_client.download_file(bucket, object_key, local_file)
    
    return local_dir

# Função para ler arquivos Parquet/CSV e carregar no MySQL
def process_and_load(local_dir, table_name, file_format="parquet"):
    if file_format == "parquet":
        # Lê todos os arquivos Parquet no diretório (assumindo que são particionados)
        df = pd.read_parquet(f"{local_dir}/{table_name}/*.parquet")
    else:  # CSV, por exemplo
        df = pd.read_csv(f"{local_dir}/{table_name}/*.csv")
    
    # Conecta ao MySQL e carrega
    engine = create_engine(
        f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@"
        f"{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
    )
    
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Tabela {table_name} carregada com sucesso no MySQL!")

if __name__ == "__main__":
    # Baixa os dados do MinIO para um diretório temporário
    local_dir = "/tmp/gold_data"
    
    # Baixa os dados das tabelas
    download_from_minio(gold_bucket, "atracacao", local_dir)
    download_from_minio(gold_bucket, "carga", local_dir)
    
    # Processa e carrega as tabelas
    process_and_load(local_dir, "atracacao", "parquet")
    process_and_load(local_dir, "carga", "parquet")