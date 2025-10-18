"""
Spark job para processar equipamentos de cultura e lazer do Recife.

Fluxo:
1. Carregar dados raw do MinIO
2. Data Quality (validação e limpeza)
3. Agregações por TIPO e BAIRRO
4. Calcular KPIs
5. Salvar resultados particionados no MinIO
"""
import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# Schema explícito para evitar inferência incorreta
EQUIPAMENTOS_SCHEMA = StructType([
    StructField("TIPO", StringType(), True),
    StructField("NOME", StringType(), True),
    StructField("ENDERECO", StringType(), True),
    StructField("BAIRRO", StringType(), True),
    StructField("REGIONAL", StringType(), True),
])


def create_spark_session(app_name: str = "RecifeCulturaJob") -> SparkSession:
    """
    Cria sessão Spark com configurações S3A.
    
    Args:
        app_name: Nome da aplicação Spark
        
    Returns:
        SparkSession configurada
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    
    # Log de configuração
    spark.sparkContext.setLogLevel("WARN")
    print(f"[INFO] Spark session criada: {app_name}")
    print(f"[INFO] Spark version: {spark.version}")
    
    return spark


def load_raw_data(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Carrega dados raw do CSV com schema explícito.
    
    Args:
        spark: Sessão Spark
        input_path: Caminho S3A do arquivo CSV
        
    Returns:
        DataFrame com dados brutos
    """
    print(f"[INFO] Carregando dados de: {input_path}")
    
    df = (
        spark.read
        .option("header", True)
        .option("encoding", "UTF-8")
        .schema(EQUIPAMENTOS_SCHEMA)
        .csv(input_path)
    )
    
    record_count = df.count()
    print(f"[INFO] Registros carregados: {record_count}")
    df.printSchema()
    
    return df


def apply_data_quality(df: DataFrame) -> DataFrame:
    """
    Aplica regras de Data Quality.
    
    Regras:
    - Normaliza colunas (trim, upper)
    - Remove registros com TIPO ou BAIRRO nulos
    - Valida colunas obrigatórias
    
    Args:
        df: DataFrame bruto
        
    Returns:
        DataFrame limpo
    """
    print("[INFO] Aplicando Data Quality...")
    
    # Normalização
    df_clean = df.select([
        F.trim(F.upper(F.col(c))).alias(c) for c in df.columns
    ])
    
    # Validação de colunas obrigatórias
    required_cols = ["TIPO", "BAIRRO"]
    missing_cols = [c for c in required_cols if c not in df_clean.columns]
    
    if missing_cols:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_cols}")
    
    # Filtrar nulos
    initial_count = df_clean.count()
    df_clean = df_clean.filter(
        F.col("TIPO").isNotNull() & F.col("BAIRRO").isNotNull()
    )
    final_count = df_clean.count()
    
    removed = initial_count - final_count
    print(f"[INFO] Registros removidos (nulos): {removed}")
    print(f"[INFO] Registros válidos: {final_count}")
    
    return df_clean


def aggregate_by_tipo(df: DataFrame) -> DataFrame:
    """
    Agrega equipamentos por TIPO.
    
    Args:
        df: DataFrame limpo
        
    Returns:
        DataFrame agregado
    """
    print("[INFO] Agregando por TIPO...")
    
    agg_df = (
        df.groupBy("TIPO")
        .agg(F.count(F.lit(1)).alias("qtd_equipamentos"))
        .orderBy(F.col("qtd_equipamentos").desc())
    )
    
    print(f"[INFO] Tipos distintos: {agg_df.count()}")
    agg_df.show(10, truncate=False)
    
    return agg_df


def aggregate_by_bairro(df: DataFrame) -> DataFrame:
    """
    Agrega equipamentos por BAIRRO.
    
    Args:
        df: DataFrame limpo
        
    Returns:
        DataFrame agregado
    """
    print("[INFO] Agregando por BAIRRO...")
    
    agg_df = (
        df.groupBy("BAIRRO")
        .agg(F.count(F.lit(1)).alias("qtd_equipamentos"))
        .orderBy(F.col("qtd_equipamentos").desc())
    )
    
    print(f"[INFO] Bairros distintos: {agg_df.count()}")
    agg_df.show(10, truncate=False)
    
    return agg_df


def calculate_kpis(df: DataFrame, by_tipo: DataFrame, by_bairro: DataFrame) -> DataFrame:
    """
    Calcula KPIs agregados.
    
    Args:
        df: DataFrame limpo original
        by_tipo: Agregação por tipo
        by_bairro: Agregação por bairro
        
    Returns:
        DataFrame com KPIs
    """
    print("[INFO] Calculando KPIs...")
    
    total_equipamentos = df.count()
    tipos_distintos = by_tipo.count()
    bairros_distintos = by_bairro.count()
    
    # Top 3 tipos
    top_tipos = by_tipo.limit(3).collect()
    top_tipo_str = ", ".join([f"{r['TIPO']} ({r['qtd_equipamentos']})" for r in top_tipos])
    
    # Top 3 bairros
    top_bairros = by_bairro.limit(3).collect()
    top_bairro_str = ", ".join([f"{r['BAIRRO']} ({r['qtd_equipamentos']})" for r in top_bairros])
    
    print(f"[INFO] KPIs calculados:")
    print(f"  - Total equipamentos: {total_equipamentos}")
    print(f"  - Tipos distintos: {tipos_distintos}")
    print(f"  - Bairros distintos: {bairros_distintos}")
    print(f"  - Top 3 tipos: {top_tipo_str}")
    print(f"  - Top 3 bairros: {top_bairro_str}")
    
    spark = df.sparkSession
    kpi_df = spark.createDataFrame(
        [(
            total_equipamentos,
            tipos_distintos,
            bairros_distintos,
            top_tipo_str,
            top_bairro_str
        )],
        ["total_equipamentos", "tipos_distintos", "bairros_distintos", "top_tipos", "top_bairros"]
    )
    
    return kpi_df


def write_parquet_partitioned(
    df: DataFrame,
    output_path: str,
    run_date: str,
    partition_col: str = "dt"
) -> None:
    """
    Escreve DataFrame em formato Parquet particionado por data.
    
    Args:
        df: DataFrame a ser salvo
        output_path: Caminho de saída (sem partição)
        run_date: Data de execução (YYYY-MM-DD)
        partition_col: Nome da coluna de partição
    """
    full_path = f"{output_path}/{partition_col}={run_date}/"
    print(f"[INFO] Salvando Parquet em: {full_path}")
    
    df.write.mode("overwrite").parquet(full_path)
    print(f"[INFO] ✅ Parquet salvo com sucesso")


def write_json(df: DataFrame, output_path: str, run_date: str) -> None:
    """
    Escreve DataFrame em formato JSON particionado por data.
    
    Args:
        df: DataFrame a ser salvo
        output_path: Caminho de saída
        run_date: Data de execução (YYYY-MM-DD)
    """
    full_path = f"{output_path}/dt={run_date}/"
    print(f"[INFO] Salvando JSON em: {full_path}")
    
    df.write.mode("overwrite").json(full_path)
    print(f"[INFO] ✅ JSON salvo com sucesso")


def main():
    """
    Função principal do job Spark.
    """
    parser = argparse.ArgumentParser(
        description="Processar equipamentos de cultura do Recife"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Caminho S3A do CSV de entrada (ex: s3a://datalake/raw/recife/equipamentos.csv)"
    )
    parser.add_argument(
        "--output_base",
        required=True,
        help="Caminho base S3A de saída (ex: s3a://datalake/curated/recife)"
    )
    parser.add_argument(
        "--run_date",
        required=True,
        help="Data de execução no formato YYYY-MM-DD"
    )
    
    args = parser.parse_args()
    
    # Validar formato da data
    try:
        datetime.strptime(args.run_date, "%Y-%m-%d")
    except ValueError:
        print(f"[ERROR] Data inválida: {args.run_date}. Use formato YYYY-MM-DD")
        sys.exit(1)
    
    print("=" * 80)
    print(f"[INFO] 🚀 Iniciando RecifeCulturaJob")
    print(f"[INFO] Input: {args.input}")
    print(f"[INFO] Output base: {args.output_base}")
    print(f"[INFO] Run date: {args.run_date}")
    print("=" * 80)
    
    # 1. Criar sessão Spark
    spark = create_spark_session()
    
    try:
        # 2. Carregar dados
        df_raw = load_raw_data(spark, args.input)
        
        # 3. Data Quality
        df_clean = apply_data_quality(df_raw)
        
        # 4. Agregações
        by_tipo = aggregate_by_tipo(df_clean)
        by_bairro = aggregate_by_bairro(df_clean)
        
        # 5. KPIs
        kpis = calculate_kpis(df_clean, by_tipo, by_bairro)
        
        # 6. Salvar resultados
        write_parquet_partitioned(
            by_tipo,
            f"{args.output_base}/cultura_por_tipo",
            args.run_date
        )
        
        write_parquet_partitioned(
            by_bairro,
            f"{args.output_base}/cultura_por_bairro",
            args.run_date
        )
        
        write_json(
            kpis,
            f"{args.output_base}/reports/kpi",
            args.run_date
        )
        
        print("=" * 80)
        print("[INFO] ✅ Job finalizado com sucesso!")
        print("=" * 80)
        
    except Exception as e:
        print(f"[ERROR] ❌ Falha na execução do job: {e}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
