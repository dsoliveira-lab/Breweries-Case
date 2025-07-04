import pandas as pd
import boto3
import io
import logging
from airflow.hooks.base_hook import BaseHook
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def brewery_etl_gold(silver_bucket, gold_bucket, endpoint_url, access_key, secret_key):
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    def read_all_silver_files(minio_client, silver_bucket):
        try:
            response = minio_client.list_objects_v2(Bucket=silver_bucket)
            files = [item['Key'] for item in response.get('Contents', []) if item['Key'].startswith("ds_silver_") and item['Key'].endswith(".parquet")]

            all_dfs = []
            for file_key in files:
                logging.info(f"Lendo arquivo: {file_key}")
                obj = minio_client.get_object(Bucket=silver_bucket, Key=file_key)
                data = obj['Body'].read()
                df = pd.read_parquet(io.BytesIO(data))
                all_dfs.append(df)

            if not all_dfs:
                raise ValueError("Nenhum arquivo Parquet encontrado na camada silver.")

            return pd.concat(all_dfs, ignore_index=True)

        except Exception as e:
            logging.error(f"Erro ao ler os arquivos da camada silver: {e}")
            raise

    def data_aggregation(df):
        try:
            aggregated_df = df.groupby(['brewery_type', 'city', 'state_province', 'country']) \
                              .size().reset_index(name='brewery_count')
            return aggregated_df
        except Exception as e:
            logging.error(f"Erro ao realizar a agregação de dados: {e}")
            raise

    try:
        silver_df = read_all_silver_files(minio_client, silver_bucket)
        gold_df = data_aggregation(silver_df)

        # Salvar no bucket Gold
        parquet_buffer = io.BytesIO()
        gold_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(
            Bucket=gold_bucket,
            Key='ds_gold.parquet',
            Body=parquet_buffer.getvalue()
        )
        logging.info("Arquivo ds_gold.parquet salvo com sucesso no bucket Gold")

        # Inserção no banco
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
                    CREATE TABLE IF NOT EXISTS ds_gold (
                        brewery_type VARCHAR(255),
                        city VARCHAR(255),
                        state_province VARCHAR(255),
                        country VARCHAR(50),
                        brewery_count INT,
                        PRIMARY KEY (brewery_type, city, state_province, country)
                    );
                """)

                for _, row in gold_df.iterrows():
                    try:
                        cursor.execute("""
                            INSERT INTO ds_gold (
                                brewery_type, city, state_province, country, brewery_count
                            ) VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (brewery_type, city, state_province, country)
                            DO UPDATE SET brewery_count = EXCLUDED.brewery_count;
                        """, (
                            row['brewery_type'], row['city'], row['state_province'],
                            row['country'], row['brewery_count']
                        ))
                    except Exception as e:
                        logging.error(f"Erro ao inserir linha: {e}")

                connection.commit()

    except Exception as e:
        logging.error(f"Erro no processo ETL da camada Gold: {e}")
        raise

    logging.info("Processo ETL da camada gold concluído com sucesso.")
