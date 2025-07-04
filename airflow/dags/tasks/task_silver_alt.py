import pandas as pd
import boto3
import io
import logging
from airflow.hooks.base_hook import BaseHook
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def brewery_etl_silver(bronze_bucket, silver_bucket, endpoint_url, access_key, secret_key):
    # Cliente MinIO
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Função para ler dados da camada bronze
    def read_bronze_data(minio_client, bronze_bucket, obj_key):
        try:
            bronze_object = minio_client.get_object(Bucket=bronze_bucket, Key=obj_key)
            bronze_data = bronze_object['Body'].read()
            return pd.read_parquet(io.BytesIO(bronze_data))
        except Exception as e:
            logging.error(f"Erro ao ler os dados da camada bronze: {e}")
            raise

    # Função de transformação
    def data_transformation(df):
        try:
            latitude_replace = {
                "9c5a66c8-cc13-416f-a5d9-0a769c87d318": "30.2232157",
                "d81ff708-b5d2-478f-af6a-6d40f5beb9ac": "42.9539343",
                "fb94830f-6196-4f59-9189-c9060b778085": "33.3219732",
                "50521ef7-f543-4c5d-98b1-0d0ee1a2be01": "44.9477074",
                "45119c56-345b-4adc-b481-c5cf7bfe98c4": "34.9206198",
                "5c53b314-ebab-4e3e-89be-e4139d9318ae": "35.8009165",
                "b7b68d22-5045-4501-b9bf-ec94946eaffc": "46.6876583",
                "d35b40b0-a3ff-4878-a6ee-9caa2149b521": "39.4171642",
                "84d621c4-81a5-44e6-aca7-1566c2e67cc0": "38.3279312",
                "6c53984f-fac1-4ea7-9c44-44e25897c71a": "44.8149712",
                "34e8c68b-6146-453f-a4b9-1f6cd99a5ada": "42.7201082"
            }
            longitude_replace = {
                "9c5a66c8-cc13-416f-a5d9-0a769c87d318": "-97.7701612",
                "d81ff708-b5d2-478f-af6a-6d40f5beb9ac": "-78.7127541",
                "fb94830f-6196-4f59-9189-c9060b778085": "-111.7267596",
                "50521ef7-f543-4c5d-98b1-0d0ee1a2be01": "-93.0895051",
                "45119c56-345b-4adc-b481-c5cf7bfe98c4": "-82.2846578",
                "5c53b314-ebab-4e3e-89be-e4139d9318ae": "-78.6345215",
                "b7b68d22-5045-4501-b9bf-ec94946eaffc": "-94.1123597",
                "d35b40b0-a3ff-4878-a6ee-9caa2149b521": "-76.9773789",
                "84d621c4-81a5-44e6-aca7-1566c2e67cc0": "-77.7187148",
                "6c53984f-fac1-4ea7-9c44-44e25897c71a": "-73.0816367",
                "34e8c68b-6146-453f-a4b9-1f6cd99a5ada": "-87.8833635"
            }

            df['latitude'] = df.apply(lambda row: latitude_replace.get(row['id'], row['latitude']), axis=1)
            df['longitude'] = df.apply(lambda row: longitude_replace.get(row['id'], row['longitude']), axis=1)

            # Outras correções específicas
            df.loc[df['id'] == "e5f3e72a-fee2-4813-82cf-f2e53b439ae6", 'address_1'] = "Clonmore"
            df.loc[df['id'] == "e5f3e72a-fee2-4813-82cf-f2e53b439ae6", 'address_2'] = ""
            df.loc[df['id'] == "0faa0fb2-fffa-416d-9eab-46f67477c8ef", 'street'] = "12 W Main St"
            df.loc[df['id'] == "b7b68d22-5045-4501-b9bf-ec94946eaffc", 'street'] = "36846 Co Hwy 66"
            df.loc[df['id'] == "fe6b9893-b93e-43d5-a9f6-3e0c89a3f13c", 'street'] = "5114 Yosemite All-Year Hwy"

            df['latitude'] = df['latitude'].astype(float)
            df['longitude'] = df['longitude'].astype(float)

            return df
        except Exception as e:
            logging.error(f"Erro ao transformar dados: {e}")
            raise

    def create_table(cursor):
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bs_silver (
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
        # Leitura e transformação dos dados
        obj_key = "ds_bronze.parquet"
        bronze_df = read_bronze_data(minio_client, bronze_bucket, obj_key)
        silver_df = data_transformation(bronze_df)

        # Salva o DataFrame completo em um único arquivo parquet
        parquet_buffer = io.BytesIO()
        silver_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(Bucket=silver_bucket, Key="ds_silver.parquet", Body=parquet_buffer.getvalue())
        logging.info("Dados tratados e salvos na camada silver com sucesso.")

        # Inserção no banco PostgreSQL
        conn = BaseHook.get_connection('my_postgres')
        with psycopg2.connect(
            host=conn.host,
            database=conn.schema,
            user=conn.login,
            password=conn.password,
            port=conn.port
        ) as connection:
            with connection.cursor() as cursor:
                create_table(cursor)
                for index, row in silver_df.iterrows():
                    try:
                        cursor.execute(
                            """
                            INSERT INTO bs_silver (id, name, brewery_type, address_1, address_2, address_3,
                            city, state_province, postal_code, country, longitude, latitude, phone, website_url, state, street)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                name = EXCLUDED.name,
                                brewery_type = EXCLUDED.brewery_type,
                                address_1 = EXCLUDED.address_1,
                                address_2 = EXCLUDED.address_2,
                                address_3 = EXCLUDED.address_3,
                                city = EXCLUDED.city,
                                state_province = EXCLUDED.state_province,
                                postal_code = EXCLUDED.postal_code,
                                country = EXCLUDED.country,
                                longitude = EXCLUDED.longitude,
                                latitude = EXCLUDED.latitude,
                                phone = EXCLUDED.phone,
                                website_url = EXCLUDED.website_url,
                                state = EXCLUDED.state,
                                street = EXCLUDED.street;
                            """,
                            (
                                row['id'], row['name'], row['brewery_type'], row['address_1'], row['address_2'],
                                row['address_3'], row['city'], row['state_province'], row['postal_code'], row['country'],
                                row['longitude'], row['latitude'], row['phone'], row['website_url'], row['state'], row['street']
                            )
                        )
                    except Exception as e:
                        logging.error(f"Erro ao inserir linha {index}: {e}")
                connection.commit()

    except Exception as e:
        logging.error(f"Erro no processo da camada silver: {e}")
        raise

    logging.info("Processo ETL da camada silver concluído com sucesso.")
