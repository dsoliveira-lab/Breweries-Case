import requests
import pandas as pd
import boto3
import io
import logging
from airflow.hooks.base_hook import BaseHook
import psycopg2

# Configuração dos logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def brewery_etl_bronze(api_url, bronze_bucket, endpoint_url, access_key, secret_key):
    # Acessando o MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    def get_api_data(api_url):
        try:
            response = requests.get(api_url)
            data = response.json()
            if not data:
                raise ValueError("Nenhum dado foi retornado da API")
            return pd.DataFrame(data)
        except Exception as e:
            logging.error(f"Erro ao obter dados da API: {e}")
            raise

    try:
        # Obter dados da API Open Breweries DB e converte para DataFrame
        pd_df = get_api_data(api_url)

        # Escrever os dados em formato Parquet no MinIO (camada bronze)
        parquet_buffer = io.BytesIO()
        pd_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(Bucket=bronze_bucket, Key="bs_bronze.parquet", Body=parquet_buffer.getvalue())
        logging.info("Dados salvos no MinIO com sucesso")

    except Exception as e:
        logging.error(f"Erro ao salvar os dados da API no Minio: {e}")
        raise

    def create_table(cursor):
        # Criação da tabela caso ela não exista
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bs_bronze (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                brewery_type VARCHAR(255),
                address_1 VARCHAR(255),
                address_2 VARCHAR(255),
                address_3 VARCHAR(255),
                city VARCHAR(255),
                state_province VARCHAR(255),
                postal_code VARCHAR(20),
                country VARCHAR(50),
                longitude FLOAT,
                latitude FLOAT,
                phone VARCHAR(50),
                website_url VARCHAR(255),
                state VARCHAR(255),
                street VARCHAR(255)
            );
        """)

    try:
        # Conexão Airflow
        conn = BaseHook.get_connection('my_postgres')

        # Conectar ao PostgreSQL e salvar os dados
        with psycopg2.connect(
            host=conn.host,
            database=conn.schema,
            user=conn.login,
            password=conn.password,
            port=conn.port
        ) as connection:
            with connection.cursor() as cursor:
                # Chamando a função para criar a tabela
                create_table(cursor)

                for index, row in pd_df.iterrows():
                    try:
                        # Verifica se o ID já existe na tabela
                        cursor.execute("SELECT COUNT(*) FROM bs_bronze WHERE id = %s", (row['id'],))
                        exists = cursor.fetchone()[0] > 0
                        
                        if not exists:
                            cursor.execute(
                                """
                                INSERT INTO bs_bronze (id, name, brewery_type, address_1, address_2, address_3,
                                city, state_province, postal_code, country, longitude, latitude, phone, website_url, state,	street)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (row['id'], row['name'], row['brewery_type'], row['address_1'], row['address_2'],
                                 row['address_3'], row['city'], row['state_province'], row['postal_code'], row['country'],
                                 row['longitude'], row['latitude'], row['phone'], row['website_url'], row['state'], row['street'])
                            )
                        else:
                            logging.info(f"O ID {row['id']} já existe no banco de dados e não será inserido.")
                            
                    except Exception as e:
                        logging.error(f"Erro ao inserir dados na linha {index}: {e}")
                connection.commit()

    except Exception as e:
        logging.error(f"Erro ao criar a tabela: {e}")
        raise

    logging.info("Processo Extração bronze concluído com sucesso.")
