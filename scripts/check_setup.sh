#!/bin/bash

# Script de validação do ambiente
# Executa todos os checks necessários antes de rodar o pipeline

set -e

echo "🔍 Verificando setup do pipeline..."
echo ""

# Cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Função para checks
check_pass() {
    echo -e "${GREEN}✅ $1${NC}"
}

check_fail() {
    echo -e "${RED}❌ $1${NC}"
}

check_warn() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# 1. Verificar containers rodando
echo "📦 Verificando containers..."
if docker compose ps | grep -q "Up"; then
    CONTAINERS=$(docker compose ps --format json | jq -r 'select(.State == "running") | .Service' | wc -l)
    check_pass "Containers rodando: $CONTAINERS"
else
    check_fail "Nenhum container rodando. Execute: docker compose up -d"
    exit 1
fi
echo ""

# 2. Verificar MinIO
echo "🪣 Verificando MinIO..."
if docker compose ps minio | grep -q "Up"; then
    check_pass "MinIO está rodando"
    
    # Verificar bucket
    if docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null; then
        if docker compose exec -T minio mc ls local/datalake 2>/dev/null; then
            check_pass "Bucket 'datalake' existe"
            
            # Verificar arquivo CSV
            if docker compose exec -T minio mc ls local/datalake/raw/recife/equipamentos_cultura_lazer.csv 2>/dev/null; then
                check_pass "Arquivo CSV encontrado no MinIO"
            else
                check_warn "Arquivo CSV NÃO encontrado em s3://datalake/raw/recife/"
                echo "         Execute: docker compose restart minio-setup"
            fi
        else
            check_warn "Bucket 'datalake' não encontrado"
        fi
    fi
else
    check_fail "MinIO não está rodando"
fi
echo ""

# 3. Verificar Spark
echo "⚡ Verificando Spark..."
if docker compose ps spark-master | grep -q "Up"; then
    check_pass "Spark Master está rodando"
    
    # Verificar workers
    WORKERS=$(docker compose ps spark-worker --format json 2>/dev/null | jq -r 'select(.State == "running")' | wc -l)
    if [ "$WORKERS" -gt 0 ]; then
        check_pass "Spark Workers rodando: $WORKERS"
    else
        check_warn "Nenhum Spark Worker encontrado"
    fi
else
    check_fail "Spark Master não está rodando"
fi
echo ""

# 4. Verificar Airflow
echo "🌬️  Verificando Airflow..."
if docker compose ps airflow-webserver | grep -q "Up"; then
    check_pass "Airflow Webserver está rodando"
else
    check_fail "Airflow Webserver não está rodando"
fi

if docker compose ps airflow-scheduler | grep -q "Up"; then
    check_pass "Airflow Scheduler está rodando"
else
    check_fail "Airflow Scheduler não está rodando"
fi

# Verificar provider Spark
echo ""
echo "📦 Verificando providers do Airflow..."
if docker compose exec -T airflow-webserver airflow providers list 2>/dev/null | grep -q "apache-airflow-providers-apache-spark"; then
    check_pass "Provider Spark instalado"
else
    check_warn "Provider Spark pode não estar instalado"
    echo "         Os containers estão instalando dependências no startup (pode levar 1-2 min)"
fi

# Verificar boto3
if docker compose exec -T airflow-webserver python -c "import boto3" 2>/dev/null; then
    check_pass "boto3 instalado"
else
    check_warn "boto3 pode não estar instalado"
    echo "         Os containers estão instalando dependências no startup (pode levar 1-2 min)"
fi
echo ""

# 5. Verificar DAGs
echo "📋 Verificando DAGs..."
sleep 2  # Aguarda um pouco para o scheduler processar
DAGS=$(docker compose exec -T airflow-webserver airflow dags list 2>/dev/null | grep "recife_cultura_etl" | wc -l)
if [ "$DAGS" -gt 0 ]; then
    check_pass "DAG 'recife_cultura_etl' encontrada"
else
    check_warn "DAG 'recife_cultura_etl' não encontrada"
    echo "         Execute: docker compose restart airflow-scheduler"
fi
echo ""

# 6. Verificar arquivo local
echo "📁 Verificando arquivos locais..."
if [ -f "./data/raw/equipamentos_cultura_lazer.csv" ]; then
    check_pass "CSV local existe em ./data/raw/"
else
    check_fail "CSV local NÃO existe em ./data/raw/"
    echo "         Baixe o dataset do Kaggle ou use o arquivo de exemplo"
fi
echo ""

# 7. Verificar configurações Spark
echo "⚙️  Verificando configurações Spark..."
if [ -f "./spark/conf/core-site.xml" ]; then
    check_pass "core-site.xml existe"
else
    check_fail "core-site.xml não encontrado"
fi

if [ -f "./spark/conf/spark-defaults.conf" ]; then
    check_pass "spark-defaults.conf existe"
else
    check_fail "spark-defaults.conf não encontrado"
fi
echo ""

# Resumo final
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 RESUMO"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "URLs para acesso:"
echo "  🌬️  Airflow:  http://localhost:8088 (admin/admin)"
echo "  🪣 MinIO:    http://localhost:9001 (minioadmin/minioadmin)"
echo "  ⚡ Spark UI: http://localhost:8080"
echo ""
echo "Próximo passo:"
echo "  1. Abra o Airflow: http://localhost:8088"
echo "  2. Ative o DAG 'recife_cultura_etl'"
echo "  3. Clique em 'Trigger DAG' ▶️"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

