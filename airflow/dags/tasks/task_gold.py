import pandas as pd
import boto3
import io
import logging
from airflow.hooks.base_hook import BaseHook
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def breweries_etl_gold(silver_bucket, gold_bucket, endpoint_url, access_key, secret_key):
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    def read_silver_data(minio_client, silver_bucket):
        try:
            silver_object = minio_client.get_object(Bucket=silver_bucket, Key='bs_silver.parquet')
            silver_data = silver_object['Body'].read()
            return pd.read_parquet(io.BytesIO(silver_data))
        except Exception as e:
            logging.error(f"Erro ao ler os dados da camada silver: {e}")
            raise

    def data_aggregation(df):
        try:
            # Agrupando por tipo de cervejaria e localização
            aggregated_df = df.groupby(['brewery_type', 'city', 'state_province', 'country']).size().reset_index(name='brewery_count')
            return aggregated_df
        except Exception as e:
            logging.error(f"Erro ao realizar a agregação de dados: {e}")
            raise

    try:
        # Leitura dos dados da camada silver
        silver_df = read_silver_data(minio_client, silver_bucket)

        # Agregação dos dados
        gold_df = data_aggregation(silver_df)

        # Escrever os dados agregados na camada gold
        parquet_buffer = io.BytesIO()
        gold_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(Bucket=gold_bucket, Key='bs_gold.parquet', Body=parquet_buffer.getvalue())
        logging.info("Dados agregados e salvos na camada gold com sucesso")

        # Opcional: Gravar no banco de dados
        conn = BaseHook.get_connection('my_postgres')
        with psycopg2.connect(
            host=conn.host,
            database=conn.schema,
            user=conn.login,
            password=conn.password,
            port=conn.port
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bs_gold (
                        brewery_type VARCHAR(255),
                        city VARCHAR(255),
                        state_province VARCHAR(255),
                        country VARCHAR(50),
                        brewery_count INT,
                        PRIMARY KEY (brewery_type, city, state_province, country)
                    );
                """)

                for index, row in gold_df.iterrows():
                    try:
                        cursor.execute(
                            """
                            INSERT INTO bs_gold (brewery_type, city, state_province, country, brewery_count)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (brewery_type, city, state_province, country) DO UPDATE SET
                                brewery_count = EXCLUDED.brewery_count;
                            """,
                            (row['brewery_type'], row['city'], row['state_province'], row['country'], row['brewery_count'])
                        )
                    except Exception as e:
                        logging.error(f"Erro ao inserir dados na linha {index}: {e}")
                connection.commit()

    except Exception as e:
        logging.error(f"Erro ao processar os dados da camada gold: {e}")
        raise

    logging.info("Processo ETL da camada gold concluído com sucesso.")
