"""
Job Spark parametrizado para processar equipamentos de cultura do Recife
Aceita argumentos via --input, --output_base e --run_date
"""
import argparse
from pyspark.sql import SparkSession, functions as F


def require_cols(df, cols):
    """Valida se as colunas obrigatórias existem no DataFrame"""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes no dataset: {missing}")


def main():
    # Parse de argumentos
    ap = argparse.ArgumentParser(description="Recife Cultura ETL Job")
    ap.add_argument("--input", required=True, 
                    help="Caminho S3 do arquivo de entrada (ex: s3a://datalake/raw/recife/equipamentos.csv)")
    ap.add_argument("--output_base", required=True,
                    help="Caminho base S3 para saída (ex: s3a://datalake/curated/recife)")
    ap.add_argument("--run_date", required=True,
                    help="Data de execução no formato YYYY-MM-DD")
    args = ap.parse_args()

    print(f"[INFO] Iniciando job com input={args.input}, output_base={args.output_base}, run_date={args.run_date}")

    # Inicializa Spark
    # O master já vem configurado pelo spark-submit, então não setamos aqui
    spark = (SparkSession.builder
             .appName("RecifeCulturaJob")
             .getOrCreate())

    # Leitura do CSV
    print(f"[INFO] Lendo dados de {args.input}")
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(args.input))

    print(f"[INFO] Total de registros lidos: {df.count()}")

    # Normalização: converte nomes de colunas para uppercase e remove espaços
    df = df.select([F.trim(F.col(c)).alias(c.upper()) for c in df.columns])

    # Data Quality: valida colunas obrigatórias
    required_columns = ["TIPO", "BAIRRO"]
    require_cols(df, required_columns)

    # Remove registros com chaves nulas
    df_clean = df.filter(F.col("TIPO").isNotNull() & F.col("BAIRRO").isNotNull())
    
    registros_removidos = df.count() - df_clean.count()
    if registros_removidos > 0:
        print(f"[WARNING] {registros_removidos} registros removidos por ter TIPO ou BAIRRO nulo")

    # Agregação 1: Equipamentos por tipo
    print("[INFO] Calculando agregação por TIPO")
    por_tipo = (df_clean.groupBy("TIPO")
                .agg(F.count(F.lit(1)).alias("qtd"))
                .orderBy(F.col("qtd").desc()))

    # Agregação 2: Equipamentos por bairro
    print("[INFO] Calculando agregação por BAIRRO")
    por_bairro = (df_clean.groupBy("BAIRRO")
                  .agg(F.count(F.lit(1)).alias("qtd"))
                  .orderBy(F.col("qtd").desc()))

    # Escrita particionada por data de execução
    output_tipo = f"{args.output_base}/cultura_por_tipo/dt={args.run_date}/"
    output_bairro = f"{args.output_base}/cultura_por_bairro/dt={args.run_date}/"
    
    print(f"[INFO] Escrevendo resultados em {output_tipo}")
    por_tipo.write.mode("overwrite").parquet(output_tipo)
    
    print(f"[INFO] Escrevendo resultados em {output_bairro}")
    por_bairro.write.mode("overwrite").parquet(output_bairro)

    # KPIs: Resumo da execução
    tipos_distintos = por_tipo.count()
    bairros_distintos = por_bairro.count()
    total_equipamentos = df_clean.count()

    print(f"[INFO] KPIs: {tipos_distintos} tipos distintos, {bairros_distintos} bairros distintos, {total_equipamentos} equipamentos")

    kpi = spark.createDataFrame(
        [(tipos_distintos, bairros_distintos, total_equipamentos, args.run_date)],
        ["tipos_distintos", "bairros_distintos", "total_equipamentos", "run_date"]
    )

    output_kpi = f"{args.output_base}/reports/kpi/dt={args.run_date}/"
    print(f"[INFO] Escrevendo KPIs em {output_kpi}")
    kpi.write.mode("overwrite").json(output_kpi)

    print("[INFO] Job finalizado com sucesso!")
    spark.stop()


if __name__ == "__main__":
    main()

