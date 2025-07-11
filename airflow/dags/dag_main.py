from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

from tasks.task_bronze import brewery_etl_bronze
from tasks.task_silver import brewery_etl_silver
from tasks.task_gold import brewery_etl_gold

default_args = {
    'owner': 'Fake_Brewery',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False, # <-- Não enviar e-mail em caso de falha
    'email_on_retry': False,
    'retries': 1, # <-- Quantas vezes tentar novamente
    'retry_delay': timedelta(minutes=2), # <-- Tempo de espera entre tentativas
}

@dag(
    dag_id='main_dag',
    default_args=default_args,
    description='DAG responsavel pelo ETL do case brewery',
    schedule_interval='30 16 * * *',
    catchup=False
)
def main_dag():
    api_url = 'https://api.openbrewerydb.org/v1/breweries'
    bronze_bucket = 'bronze'
    silver_bucket = 'silver'
    gold_bucket = 'gold'
    #endpoint_url = 'http://minio:9000'
    #access_key = 'minioadmin'
    #secret_key = 'minio@9876!'
    access_key = Variable.get("minio_access_key")
    secret_key = Variable.get("minio_secret_key")
    endpoint_url = Variable.get("minio_endpoint_url")


    with TaskGroup("group_task_bronze", tooltip="Tasks processadas Da API para minio, salvando em .parquet") as group_task_bronze:
        PythonOperator(
            task_id='task_bronze',
            python_callable=brewery_etl_bronze,
            op_args=[api_url, bronze_bucket, endpoint_url, access_key, secret_key],
        ),
    with TaskGroup("group_task_silver", tooltip="Tasks processadas do Minio, salvando na camada silver") as group_task_silver:
        PythonOperator(
            task_id='task_silver',
            python_callable=brewery_etl_silver,
            op_args=[bronze_bucket, silver_bucket, endpoint_url, access_key, secret_key],
        ),
    with TaskGroup("group_task_gold", tooltip="Dados agregados, salvando na camada gold") as group_task_gold:
        PythonOperator(
            task_id='task_gold',
            python_callable=brewery_etl_gold,
            op_args=[silver_bucket, gold_bucket, endpoint_url, access_key, secret_key],
        ),

    # Definindo a ordem de execução dos grupos
    # Aqui a gente vai garantir que o airflow execute primeiramente as cargas e depois as transformações.
    group_task_bronze >> group_task_silver >> group_task_gold

main_dag_instance = main_dag()
