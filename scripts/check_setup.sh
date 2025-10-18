#!/bin/bash
# Script para validar setup completo do pipeline Airflow + Spark + MinIO
# Autor: Eduardo Wanderley de Oliveira

set -euo pipefail

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Funções auxiliares
print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_info() {
    echo -e "ℹ️  $1"
}

# Header
echo "========================================"
echo "  Validação do Setup - Airflow Pipeline"
echo "========================================"
echo ""

# 1. Verificar Docker
print_info "Verificando Docker..."
if command -v docker &> /dev/null; then
    print_success "Docker instalado: $(docker --version)"
else
    print_error "Docker não encontrado. Instale: https://docs.docker.com/get-docker/"
    exit 1
fi

# 2. Verificar Docker Compose
print_info "Verificando Docker Compose..."
if docker compose version &> /dev/null; then
    print_success "Docker Compose instalado: $(docker compose version)"
else
    print_error "Docker Compose não encontrado"
    exit 1
fi

# 3. Verificar se containers estão rodando
print_info "Verificando containers..."
REQUIRED_CONTAINERS=(
    "airflow-practicing-postgres-1"
    "airflow-practicing-minio-1"
    "airflow-practicing-airflow-webserver-1"
    "airflow-practicing-airflow-scheduler-1"
)

ALL_RUNNING=true
for container in "${REQUIRED_CONTAINERS[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "$container"; then
        print_success "Container rodando: $container"
    else
        print_error "Container não encontrado ou parado: $container"
        ALL_RUNNING=false
    fi
done

if [ "$ALL_RUNNING" = false ]; then
    print_warning "Execute: docker compose up -d"
    exit 1
fi

# 4. Verificar portas
print_info "Verificando portas..."
PORTS=(5432 7077 8080 8088 9000 9001)
for port in "${PORTS[@]}"; do
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 || nc -z localhost $port 2>/dev/null; then
        print_success "Porta $port em uso"
    else
        print_warning "Porta $port não está em uso"
    fi
done

# 5. Verificar Airflow UI
print_info "Verificando Airflow UI..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8088/health | grep -q "200"; then
    print_success "Airflow UI acessível em http://localhost:8088"
else
    print_warning "Airflow UI não está respondendo. Aguarde inicialização completa."
fi

# 6. Verificar MinIO
print_info "Verificando MinIO..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/minio/health/live | grep -q "200"; then
    print_success "MinIO acessível em http://localhost:9000"
    print_success "MinIO Console em http://localhost:9001 (minioadmin/minioadmin)"
else
    print_warning "MinIO não está respondendo"
fi

# 7. Verificar bucket e arquivo no MinIO
print_info "Verificando bucket e dados no MinIO..."
if docker compose exec -T minio mc alias set local http://localhost:9000 minioadmin minioadmin &> /dev/null; then
    if docker compose exec -T minio mc ls local/datalake &> /dev/null; then
        print_success "Bucket 'datalake' existe"
        
        if docker compose exec -T minio mc ls local/datalake/raw/recife/equipamentos_cultura_lazer.csv &> /dev/null; then
            print_success "Arquivo CSV encontrado no MinIO"
        else
            print_warning "Arquivo CSV não encontrado. Execute: docker compose up minio-setup"
        fi
    else
        print_warning "Bucket 'datalake' não encontrado"
    fi
fi

# 8. Verificar Spark cluster
print_info "Verificando Spark cluster..."
if curl -s http://localhost:8080 | grep -q "Spark Master"; then
    print_success "Spark Master UI acessível em http://localhost:8080"
else
    print_warning "Spark Master não está respondendo"
fi

# 9. Verificar DAG no Airflow
print_info "Verificando DAG carregada..."
if docker compose exec -T airflow-webserver airflow dags list 2>/dev/null | grep -q "recife_cultura_etl"; then
    print_success "DAG 'recife_cultura_etl' carregada no Airflow"
else
    print_warning "DAG não encontrada. Verifique logs: docker compose logs airflow-scheduler"
fi

# 10. Verificar dependências Python
print_info "Verificando dependências Python no Airflow..."
DEPS=("pyspark" "boto3" "apache-airflow-providers-apache-spark")
for dep in "${DEPS[@]}"; do
    if docker compose exec -T airflow-webserver python -c "import ${dep%%[*}" &> /dev/null; then
        print_success "Dependência instalada: $dep"
    else
        print_error "Dependência não encontrada: $dep"
    fi
done

# Summary
echo ""
echo "========================================"
echo "  RESUMO DA VALIDAÇÃO"
echo "========================================"

if [ "$ALL_RUNNING" = true ]; then
    print_success "Setup completo e funcional!"
    echo ""
    echo "📊 Acessos:"
    echo "  - Airflow UI:  http://localhost:8088 (admin/admin)"
    echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    echo "  - Spark Master: http://localhost:8080"
    echo ""
    echo "🚀 Para executar a DAG:"
    echo "  docker compose exec airflow-webserver airflow dags trigger recife_cultura_etl"
else
    print_error "Setup incompleto. Corrija os erros acima."
    exit 1
fi
