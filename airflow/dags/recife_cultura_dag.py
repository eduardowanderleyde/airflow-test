"""
DAG para processamento diário de equipamentos de cultura do Recife
Inclui sensor para verificar existência do arquivo no MinIO antes de processar
"""
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Configurações do MinIO/S3
BUCKET = "datalake"
KEY = "raw/recife/equipamentos_cultura_lazer.csv"


def s3_object_exists(**context):
    """
    Verifica se o arquivo existe no MinIO
    Retorna True se existe, False caso contrário
    """
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            region_name="us-east-1"
        )
        s3.head_object(Bucket=BUCKET, Key=KEY)
        print(f"[INFO] Arquivo encontrado: s3://{BUCKET}/{KEY}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"[WARNING] Arquivo não encontrado: s3://{BUCKET}/{KEY}")
        else:
            print(f"[ERROR] Erro ao verificar arquivo: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Erro inesperado: {e}")
        return False


default_args = {
    "owner": "eduardo",
    "retries": 1,
}

with DAG(
    dag_id="recife_cultura_etl",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["recife", "spark", "minio", "cultura"],
    description="Pipeline ETL para análise de equipamentos culturais do Recife"
) as dag:

    # Sensor: aguarda arquivo estar disponível no MinIO
    wait_minio = PythonSensor(
        task_id="wait_minio_file",
        python_callable=s3_object_exists,
        poke_interval=15,  # verifica a cada 15 segundos
        timeout=60 * 10,   # timeout de 10 minutos
        mode="poke",
        doc_md="""
        ### Sensor MinIO
        Verifica se o arquivo CSV está disponível no bucket antes de processar.
        - **Bucket**: datalake
        - **Key**: raw/recife/equipamentos_cultura_lazer.csv
        """
    )

    # Spark Job: processa os dados
    # Temporariamente em modo local para fazer funcionar
    # Depois pode trocar para "spark://spark-master:7077" para modo cluster
    spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/app/jobs/recife_cultura_job.py",
        name="RecifeCulturaETL",
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
        verbose=False,
        application_args=[
            "--input", f"s3a://{BUCKET}/{KEY}",
            "--output_base", f"s3a://{BUCKET}/curated/recife",
            "--run_date", "{{ ds }}"  # data de execução no formato YYYY-MM-DD
        ],
        doc_md="""
        ### Job Spark
        Processa os dados de equipamentos culturais:
        1. Lê CSV do MinIO
        2. Aplica Data Quality (valida colunas, remove nulos)
        3. Agrega por TIPO e BAIRRO
        4. Gera KPIs
        5. Salva resultados particionados por data
        
        **Mode**: local[*] (processa no container do Airflow)
        **JARs incluídos**: hadoop-aws + aws-java-sdk (empacotados na imagem)
        
        Nota: Para usar cluster mode, adicionar conn_id="spark_default"
        """
    )

    # Dependência: aguarda arquivo antes de processar
    wait_minio >> spark_job

