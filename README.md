# Pipeline Airflow + Spark + MinIO

Pipeline de dados production-ready processando equipamentos culturais do Recife/PE com Apache Airflow, Apache Spark e MinIO.

## 🚀 Quick Start

```bash
# 1. Subir infraestrutura
docker compose up -d

# 2. Aguardar 2-3 minutos para inicialização

# 3. Acessar Airflow
open http://localhost:8088
# Login: admin / admin

# 4. Ativar e executar DAG "recife_cultura_etl"
```

## 🛠️ Tecnologias

- **Apache Airflow 2.10.2** (custom com Spark integrado)
- **Apache Spark 3.4.0**
- **MinIO** (S3-compatible storage)
- **PostgreSQL 15**
- **Docker Compose**

## 📦 Estrutura do Projeto

```
airflow-practicing/
├── dags/                   # DAGs do Airflow
│   └── recife_cultura_dag.py
├── include/                # Scripts auxiliares
│   └── jobs/
│       └── recife_cultura_job.py
├── plugins/                # Operadores customizados (vazio por enquanto)
├── config/                 # Configurações Spark
│   ├── core-site.xml
│   └── spark-defaults.conf
├── data/                   # Dados de exemplo
│   └── raw/
│       └── equipamentos_cultura_lazer.csv
├── scripts/                # Utilitários
│   └── check_setup.sh
├── Dockerfile              # Imagem customizada do Airflow
├── docker-compose.yml      # Orquestração completa
├── requirements.txt        # Dependências Python
└── .gitignore             # Arquivos ignorados
```

## ⚡ Funcionalidades

- ✅ Orquestração agendada com Airflow
- ✅ Processamento distribuído com Spark
- ✅ Data Lake (camadas raw → curated)
- ✅ Data Quality checks
- ✅ Sensor pattern (validação pré-processamento)
- ✅ Particionamento temporal
- ✅ JARs S3A empacotados (zero dependências externas)

## 🏗️ Arquitetura

```
┌─────────────────────────┐
│ Airflow (orchestrator)  │
│ + Spark + Java + JARs   │
└───────────┬─────────────┘
            │ spark-submit
            ▼
┌─────────────────────────┐
│ Spark Cluster           │
│ (opcional - modo local) │
└───────────┬─────────────┘
            │ s3a://
            ▼
┌─────────────────────────┐
│ MinIO (Data Lake)       │
│ raw/ → curated/         │
└─────────────────────────┘
```

## 📊 O que o Pipeline Faz

1. **Sensor**: Verifica arquivo CSV no MinIO
2. **Leitura**: Carrega dados via Spark
3. **Data Quality**: Valida colunas obrigatórias, remove nulos
4. **Transformação**: Agrega por TIPO e BAIRRO
5. **KPIs**: Calcula métricas
6. **Escrita**: Salva Parquet particionado por data no MinIO

## 🔧 Configuração

### Requisitos

- Docker e Docker Compose
- 8GB RAM mínimo
- Portas livres: 5432, 7077, 8080, 8088, 9000, 9001

### Variáveis de Ambiente

Configure no `docker-compose.yml`:
- `AIRFLOW__CORE__FERNET_KEY`: Chave de criptografia (gere com `cryptography.fernet.Fernet.generate_key()`)
- `AIRFLOW_CONN_SPARK_DEFAULT`: Conexão Spark (opcional)

## 🧪 Testes

```bash
# Validar setup
bash scripts/check_setup.sh

# Testar DAG manualmente
docker compose exec airflow-webserver \
  airflow tasks test recife_cultura_etl run_spark_job 2025-10-18
```

## 📁 Resultados

Após execução, os dados processados estarão em:

```
MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

datalake/curated/recife/
├── cultura_por_tipo/dt=YYYY-MM-DD/
├── cultura_por_bairro/dt=YYYY-MM-DD/
└── reports/kpi/dt=YYYY-MM-DD/
```

## 🐛 Troubleshooting

### DAG não aparece
```bash
docker compose restart airflow-scheduler
```

### Erro de conexão Spark
```bash
# Deletar conexão antiga e deixar usar modo local
docker compose exec airflow-webserver airflow connections delete spark_default
```

### Ver logs
```bash
docker compose logs -f airflow-scheduler
```

## 📚 Recursos

- [Apache Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Astronomer Registry](https://registry.astronomer.io/)
- [PEP 8 Style Guide](https://peps.python.org/pep-0008/)

## 📝 Licença

MIT

## 👨‍💻 Autor

Eduardo Wanderley de Oliveira

---

**Status**: Em desenvolvimento - Pipeline funcional em modo local
