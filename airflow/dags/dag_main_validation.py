from datetime import timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
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
    access_key = Variable.get("minio_access_key")
    secret_key = Variable.get("minio_secret_key")
    endpoint_url = Variable.get("minio_endpoint_url")

    with TaskGroup("group_task_bronze", tooltip="Tasks processadas Da API para minio, salvando em .parquet") as group_task_bronze:
        PythonOperator(
            task_id='task_bronze',
            python_callable=brewery_etl_bronze,
            op_args=[api_url, bronze_bucket, endpoint_url, access_key, secret_key],
        )
    with TaskGroup("group_task_silver", tooltip="Tasks processadas do Minio, salvando na camada silver") as group_task_silver:
        PythonOperator(
            task_id='task_silver',
            python_callable=brewery_etl_silver,
            op_args=[bronze_bucket, silver_bucket, endpoint_url, access_key, secret_key],
        )
    with TaskGroup("group_task_gold", tooltip="Dados agregados, salvando na camada gold") as group_task_gold:
        PythonOperator(
            task_id='task_gold',
            python_callable=brewery_etl_gold,
            op_args=[silver_bucket, gold_bucket, endpoint_url, access_key, secret_key],
        )

    # Operador para disparar a DAG de validação ao final
    trigger_validation = TriggerDagRunOperator(
        task_id='trigger_validation_dag',
        trigger_dag_id='validate_parquet_files',  # nome da DAG de validação
        wait_for_completion=True,  # aguarda a validação terminar antes de concluir
        poke_interval=10,          # checa status a cada 10 segundos
        reset_dag_run=True,        # reinicia execuções pendentes
    )

    # Define a ordem da execução
    group_task_bronze >> group_task_silver >> group_task_gold >> trigger_validation

main_dag_instance = main_dag()
