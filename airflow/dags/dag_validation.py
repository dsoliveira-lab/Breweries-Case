from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import boto3
import io
import pandas as pd
from airflow.models import Variable
import logging
import requests

# Configurações padrão da DAG
def slack_alert(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = (
        f":x: *Falha na DAG*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Tarefa:* `{task_id}`\n"
        f"*Execução:* `{execution_date}`\n"
        f"<{log_url}|Ver log>"
    )

    webhook_url = Variable.get("slack_webhook_url", default_var=None)  # Você coloca o webhook no Airflow Variables
    if not webhook_url:
        logging.warning("Variável slack_webhook_url não está configurada.")
        return  # Sai sem tentar enviar

    try:
        response = requests.post(webhook_url, json={"text": message})
        if response.status_code != 200:
            logging.error(f"Erro no envio do Slack: {response.status_code} - {response.text}")
    except Exception as e:
        logging.error(f"Erro ao enviar alerta para o Slack: {e}")


default_args = {
    'owner': 'Fake_Brewery',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'on_failure_callback': slack_alert  # Descomente para ativar alertas no Slack
}

@dag(schedule_interval=None, start_date=days_ago(1), catchup=False, default_args=default_args, tags=["validation"])
def validate_parquet_files():

    @task
    def validate_file(bucket: str, key: str, endpoint_url: str, access_key: str, secret_key: str, expected_columns: list):
        logging.info(f"Validando {bucket}/{key}")
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

        try:
            obj = client.get_object(Bucket=bucket, Key=key)
            size = obj['ContentLength']
            logging.info(f"Tamanho do arquivo: {size} bytes")

            if size == 0:
                raise ValueError(f"O arquivo {key} está vazio no bucket {bucket}")

            data = obj['Body'].read()
            df = pd.read_parquet(io.BytesIO(data))

            if df.empty:
                raise ValueError(f"O DataFrame de {key} está vazio.")

            # Verificar se colunas esperadas estão presentes
            missing_cols = [col for col in expected_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"O arquivo {key} está faltando as colunas: {missing_cols}")

            logging.info(f"{key} validado com sucesso. Linhas: {len(df)} | Colunas: OK")

        except Exception as e:
            logging.error(f"Erro ao validar {key} no bucket {bucket}: {e}")
            raise

    # ⚙️ Parâmetros do MinIO
    access_key = Variable.get("minio_access_key")
    secret_key = Variable.get("minio_secret_key")
    endpoint_url = Variable.get("minio_endpoint_url")

    # 📁 Schemas esperados por camada
    bronze_schema = ['id', 'name', 'brewery_type', 'address_1', 'address_2', 'address_3', 'city',
                     'state_province', 'postal_code', 'country', 'longitude', 'latitude',
                     'phone', 'website_url', 'state', 'street']

    silver_schema = bronze_schema  # Igual, mas com ajustes/correções

    gold_schema = ['brewery_type', 'city', 'state_province', 'country', 'brewery_count']

    # 🧪 Validação de cada camada
    bronze_task = validate_file.override(task_id="validate_bronze")(
        bucket="bronze",
        key="ds_bronze.parquet",
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        expected_columns=bronze_schema,
    )

    silver_task = validate_file.override(task_id="validate_silver")(
        bucket="silver",
        key="ds_silver.parquet",
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        expected_columns=silver_schema,
    )

    gold_task = validate_file.override(task_id="validate_gold")(
        bucket="gold",
        key="ds_gold.parquet",
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        expected_columns=gold_schema,
    )

    # Define ordem de execução (opcional)
    bronze_task >> silver_task >> gold_task

validate_parquet_files()
