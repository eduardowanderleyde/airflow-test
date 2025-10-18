"""
DAG para processamento diário de equipamentos de cultura do Recife.

Fluxo:
1. Sensor verifica existência do arquivo no MinIO
2. Spark job processa e agrega os dados
3. Resultados salvos em formato Parquet particionado por data

Autor: Eduardo Wanderley de Oliveira
"""
import pendulum
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# Configurações (constantes definidas no topo, não chamadas externas)
BUCKET = "datalake"
KEY = "raw/recife/equipamentos_cultura_lazer.csv"
MINIO_ENDPOINT = "http://minio:9000"


def check_minio_file_exists(**context) -> bool:
    """
    Verifica se o arquivo existe no MinIO.
    
    Função é executada apenas durante a task (não no parsing).
    
    Args:
        **context: Contexto do Airflow (automático)
        
    Returns:
        True se arquivo existe, False caso contrário
    """
    import boto3
    from botocore.exceptions import ClientError
    
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1"
        )
        
        s3_client.head_object(Bucket=BUCKET, Key=KEY)
        print(f"✅ Arquivo encontrado: s3://{BUCKET}/{KEY}")
        return True
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            print(f"⏳ Arquivo não encontrado: s3://{BUCKET}/{KEY}")
        else:
            print(f"❌ Erro ao verificar arquivo: {e}")
        return False
        
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return False


# Timezone de Recife
tz_recife = pendulum.timezone("America/Recife")

# Default args
default_args = {
    "owner": "eduardo",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

# Definição da DAG
with DAG(
    dag_id="recife_cultura_etl",
    description="Pipeline ETL para análise de equipamentos culturais do Recife",
    schedule="0 3 * * *",  # Executa às 3h da manhã todo dia
    start_date=pendulum.datetime(2025, 10, 1, tz=tz_recife),
    catchup=False,  # Não executa runs passadas
    default_args=default_args,
    tags=["recife", "spark", "minio", "cultura", "etl"],
    max_active_runs=1,  # Apenas 1 execução por vez
) as dag:

    # Task 1: Sensor para verificar arquivo no MinIO
    sensor_minio = PythonSensor(
        task_id="sensor_minio_file",
        python_callable=check_minio_file_exists,
        poke_interval=30,  # Verifica a cada 30 segundos
        timeout=60 * 15,   # Timeout de 15 minutos
        mode="poke",
        doc_md="""
        ## Sensor MinIO
        
        Verifica se o arquivo CSV está disponível no bucket MinIO antes de processar.
        
        - **Bucket**: datalake
        - **Key**: raw/recife/equipamentos_cultura_lazer.csv
        - **Poke Interval**: 30 segundos
        - **Timeout**: 15 minutos
        
        Se o arquivo não estiver disponível após o timeout, a task falha.
        """
    )

    # Task 2: Job Spark para processar os dados
    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/app/jobs/recife_cultura_job.py",
        name="RecifeCulturaETL",
        verbose=True,
        # JARs empacotados na imagem Docker do Airflow
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
        application_args=[
            "--input", f"s3a://{BUCKET}/{KEY}",
            "--output_base", f"s3a://{BUCKET}/curated/recife",
            "--run_date", "{{ ds }}"  # Template: data de execução (YYYY-MM-DD)
        ],
        doc_md="""
        ## Job Spark
        
        Processa os dados de equipamentos culturais do Recife:
        
        1. **Leitura**: Carrega CSV do MinIO com schema explícito
        2. **Data Quality**: Valida colunas obrigatórias e remove nulos
        3. **Transformação**: Normaliza dados (trim, upper)
        4. **Agregação**: Calcula totais por TIPO e BAIRRO
        5. **KPIs**: Gera métricas agregadas
        6. **Escrita**: Salva Parquet particionado por data no MinIO
        
        ### Configuração
        
        - **Mode**: local[*] (executa no container Airflow)
        - **JARs**: hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262
        - **Output**: s3a://datalake/curated/recife/
        
        ### Nota
        
        Para executar em cluster mode, adicione `conn_id="spark_default"` 
        e configure a conexão Spark no Airflow.
        """
    )

    # Dependências
    sensor_minio >> run_spark_job
