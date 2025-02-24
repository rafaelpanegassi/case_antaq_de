import os
import duckdb
import pandas as pd
import numpy as np
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def get_mysql_connection():
    """Retorna uma conexão com o MySQL usando mysql-connector-python."""
    return mysql.connector.connect(
        host="localhost",
        user="admin",
        password="admin",
        database="antaq",
        connection_timeout=600  # tempo limite em segundos; ajuste conforme necessário
    )

def create_table_if_not_exists(conn, table_name, df):
    """
    Cria a tabela no MySQL com base no schema do DataFrame, se ela não existir.
    Conversão de tipos:
      - Inteiros: BIGINT
      - Floats: DOUBLE
      - Datas/Horas: DATETIME
      - Outros: VARCHAR(255)
    """
    cols = []
    for col, dtype in zip(df.columns, df.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            col_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            col_type = "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            col_type = "DATETIME"
        else:
            col_type = "VARCHAR(255)"
        cols.append(f"`{col}` {col_type}")
    create_stmt = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(cols)});"
    cursor = conn.cursor()
    cursor.execute(create_stmt)
    conn.commit()
    cursor.close()

def insert_data(conn, table_name, df, batch_size=1000):
    """
    Insere os dados do DataFrame na tabela MySQL em lotes usando executemany.
    Converte valores nulos, timestamps e tipos NumPy para formatos compatíveis com MySQL.
    """
    placeholders = ", ".join(["%s"] * len(df.columns))
    columns = ", ".join([f"`{col}`" for col in df.columns])
    insert_stmt = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
    
    data = []
    for row in df.itertuples(index=False, name=None):
        new_row = []
        for val in row:
            if pd.isnull(val):
                new_row.append(None)
            elif isinstance(val, pd.Timestamp):
                new_row.append(val.strftime("%Y-%m-%d %H:%M:%S"))
            elif isinstance(val, np.generic):
                new_row.append(val.item())
            else:
                new_row.append(val)
        data.append(tuple(new_row))
    
    cursor = conn.cursor()
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        cursor.executemany(insert_stmt, batch)
        conn.commit()
    cursor.close()

if __name__ == "__main__":
    # Variáveis de ambiente para acesso ao MinIO e bucket GOLD
    minio_endpoint   = os.getenv("ENDPOINT_URL", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minio")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
    gold_bucket      = os.getenv("GOLD_BUCKET", "gold")
    
    # Tabelas a serem exportadas: somente "atracacao" e "carga"
    table_list = [
        "atracacao",
        "carga"
    ]

    # Conexão com DuckDB
    con = duckdb.connect()

    # Instalar e carregar o módulo httpfs para acesso a S3/MinIO
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("SET s3_use_ssl=false;")
    
    # Configura os parâmetros de acesso ao MinIO e força o uso de path-style
    endpoint_limp = minio_endpoint.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{endpoint_limp}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_force_path_style=true;")  # Força o formato path-style
    con.execute(f"SET s3_access_key_id='{minio_access_key}';")
    con.execute(f"SET s3_secret_access_key='{minio_secret_key}';")
    con.execute("SET s3_region='us-east-1';")  # Ajuste conforme necessário

    # Loop para cada tabela a ser processada
    for table_name in table_list:
        # Ajuste o caminho conforme sua estrutura de partição; 
        # neste exemplo, presume-se que os arquivos estejam em: /<tabela>/_execution_date=*/*.parquet
        parquet_path = f"s3://{gold_bucket}/{table_name}/_execution_date=*/*.parquet"
        print(f"\n[INFO] Lendo Parquet via DuckDB: {parquet_path}")
        try:
            df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
            print(f"[INFO] Registros lidos para '{table_name}': {len(df)}")
            
            # Conecta ao MySQL
            mysql_conn = get_mysql_connection()
            
            # Cria a tabela (se não existir) e limpa os dados existentes
            create_table_if_not_exists(mysql_conn, table_name, df)
            cursor = mysql_conn.cursor()
            cursor.execute(f"DELETE FROM `{table_name}`")
            mysql_conn.commit()
            cursor.close()
            
            # Insere os dados na tabela MySQL em lotes
            insert_data(mysql_conn, table_name, df, batch_size=1000)
            print(f"[INFO] Tabela '{table_name}' exportada com sucesso para o MySQL.")
            
            mysql_conn.close()
        except Exception as e:
            print(f"[ERROR] Falha ao processar a tabela '{table_name}': {e}")

    print("\n[INFO] Processo concluído. As tabelas foram exportadas para o MySQL.")
