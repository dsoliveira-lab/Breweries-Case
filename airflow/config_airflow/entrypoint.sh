#!/bin/bash
set -e  # Encerra o script se qualquer comando falhar

echo "🌀 Iniciando migração do banco..."
airflow db migrate

echo "🔌 Criando conexões padrão..."
airflow connections create-default-connections

echo "👤 Criando usuário admin..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --password admin \
  --email admin@example.com

echo "🔗 Criando conexão my_postgres..."
airflow connections add 'my_postgres' \
  --conn-uri 'postgresql+psycopg2://post_airflow:airflow_123@postgres-airflow:5432/airflow' || true

echo "📦 Importando variáveis..."
airflow variables import /opt/airflow/dags/variables.json

echo "🕒 Iniciando o scheduler em segundo plano..."
airflow scheduler &

echo "🌐 Iniciando o webserver..."
exec airflow webserver
