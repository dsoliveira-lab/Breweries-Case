FROM apache/airflow:2.9.3-python3.12

USER root

# Instalar Java 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Variáveis de ambiente Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Volta para o usuário airflow para instalar os pacotes Python
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Volta para root só para copiar e dar permissão ao entrypoint
USER root
COPY airflow/config_airflow/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Volta para o usuário airflow
USER airflow
ENTRYPOINT ["/entrypoint.sh"]
