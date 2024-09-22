from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from tasks.task_bronze import brewery_etl_bronze
from tasks.task_silver import breweries_etl_silver
from tasks.task_gold import breweries_etl_gold

default_args = {
    'owner': 'Wuldson',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='main_dag',
    default_args=default_args,
    description='DAG responsavel pelo ETL do case breweries',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def main_dag():
    api_url = 'https://api.openbrewerydb.org/breweries'
    bronze_bucket = 'bronze'
    silver_bucket = 'silver'
    gold_bucket = 'gold'
    endpoint_url = 'http://minio:9000'
    access_key = 'minioadmin'
    secret_key = 'minio@1234!'

    with TaskGroup("group_task_bronze", tooltip="Tasks processadas Da API para minio, salvando em .parquet") as group_task_bronze:
        PythonOperator(
            task_id='task_bronze',
            python_callable=brewery_etl_bronze,
            op_args=[api_url, bronze_bucket, endpoint_url, access_key, secret_key],
        ),
    with TaskGroup("group_task_silver", tooltip="Tasks processadas do Minio, salvando na camada silver") as group_task_silver:
        PythonOperator(
            task_id='task_silver',
            python_callable=breweries_etl_silver,
            op_args=[bronze_bucket, silver_bucket, endpoint_url, access_key, secret_key],
        ),
    with TaskGroup("group_task_gold", tooltip="Dados agregados, salvando na camada gold") as group_task_gold:
        PythonOperator(
            task_id='task_gold',
            python_callable=breweries_etl_gold,
            op_args=[silver_bucket, gold_bucket, endpoint_url, access_key, secret_key],
        ),

    # Definindo a ordem de execução dos grupos
    # Aqui a gente vai garantir que o airflow execute primeiramente as cargas e depois as transformações.
    group_task_bronze >> group_task_silver >> group_task_gold

main_dag_instance = main_dag()
