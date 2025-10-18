# Pipeline Airflow + Spark + MinIO

[![CI](https://github.com/eduardowanderleyde/airflow-test/actions/workflows/ci.yml/badge.svg)](https://github.com/eduardowanderleyde/airflow-test/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow 2.10.2](https://img.shields.io/badge/airflow-2.10.2-brightgreen.svg)](https://airflow.apache.org/)
[![Apache Spark 3.4.0](https://img.shields.io/badge/spark-3.4.0-orange.svg)](https://spark.apache.org/)

Pipeline de dados production-ready processando equipamentos culturais do Recife/PE com Apache Airflow, Apache Spark e MinIO.

## 🚀 Quick Start

```bash
# 1. Clonar repositório
git clone https://github.com/eduardowanderleyde/airflow-test.git
cd airflow-test

# 2. Configurar variáveis de ambiente
cp .env.example .env
# Edite .env e gere nova Fernet key:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 3. Subir infraestrutura
docker compose up -d

# 4. Aguardar inicialização (2-3 minutos)
bash scripts/check_setup.sh

# 5. Acessar Airflow
open http://localhost:8088
# Login: admin / admin (configurável no .env)

# 6. Executar DAG
docker compose exec airflow-webserver airflow dags trigger recife_cultura_etl
```

## 🛠️ Stack Tecnológica

| Componente | Versão | Função |
|------------|--------|--------|
| Apache Airflow | 2.10.2 | Orquestração de pipelines |
| Apache Spark | 3.4.0 | Processamento distribuído |
| MinIO | 2024-06 | Data Lake (S3-compatible) |
| PostgreSQL | 15 | Metastore do Airflow |
| Python | 3.11 | Linguagem principal |
| Docker | 24+ | Containerização |

## 📦 Estrutura do Projeto

```
airflow-test/
├── .github/
│   └── workflows/
│       └── ci.yml              # GitHub Actions (lint + build)
├── airflow/
│   ├── dags/
│   │   └── recife_cultura_dag.py   # DAG principal
│   ├── plugins/                # Operadores customizados (vazio)
│   ├── logs/                   # Logs do Airflow (.gitignore)
│   ├── Dockerfile              # Imagem custom com Spark
│   └── requirements.txt        # Dependências Python
├── include/
│   └── jobs/
│       └── recife_cultura_job.py   # Job Spark
├── config/
│   └── spark/
│       ├── core-site.xml       # Configuração Hadoop S3A
│       └── spark-defaults.conf # Propriedades Spark
├── data/
│   └── raw/
│       └── equipamentos_cultura_lazer.csv  # Dataset de exemplo
├── scripts/
│   └── check_setup.sh          # Script de validação
├── .editorconfig               # Padrões de código
├── .env.example                # Template de variáveis
├── .gitignore                  # Arquivos ignorados
├── docker-compose.yml          # Orquestração completa
├── LICENSE                     # MIT License
├── README.md                   # Este arquivo
└── ruff.toml                   # Configuração do linter
```

## ⚡ Funcionalidades

- ✅ **Orquestração agendada** com Airflow (cron diário às 3h)
- ✅ **Processamento distribuído** com Spark (local ou cluster mode)
- ✅ **Data Lake** estruturado (camadas raw → curated)
- ✅ **Data Quality checks** (validação de schema, nulos, etc)
- ✅ **Sensor pattern** (validação de arquivo antes de processar)
- ✅ **Particionamento temporal** (por data de execução)
- ✅ **JARs S3A empacotados** (zero dependências externas em runtime)
- ✅ **Timezone Brasil** (America/Recife)
- ✅ **CI/CD básico** (ruff + docker build via GitHub Actions)

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────┐
│ Airflow Scheduler                       │
│ (orquestra tasks, gerencia estado)      │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Task 1: PythonSensor                    │
│ Verifica arquivo no MinIO               │
└──────────────┬──────────────────────────┘
               │ (sucesso)
               ▼
┌─────────────────────────────────────────┐
│ Task 2: SparkSubmitOperator             │
│ Executa job PySpark via spark-submit    │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│ Spark (local[*] ou cluster)             │
│ 1. Lê CSV do MinIO (S3A)                │
│ 2. Data Quality                         │
│ 3. Agregações (TIPO, BAIRRO)            │
│ 4. KPIs                                 │
└──────────────┬──────────────────────────┘
               │ (escreve via S3A)
               ▼
┌─────────────────────────────────────────┐
│ MinIO (Data Lake)                       │
│ datalake/                               │
│ ├── raw/recife/                         │
│ └── curated/recife/                     │
│     ├── cultura_por_tipo/dt=YYYY-MM-DD/ │
│     ├── cultura_por_bairro/dt=...       │
│     └── reports/kpi/dt=...              │
└─────────────────────────────────────────┘
```

## 📊 Fluxo da DAG

```python
sensor_minio_file >> run_spark_job
```

1. **Sensor**: Verifica se `raw/recife/equipamentos_cultura_lazer.csv` existe no MinIO
2. **Spark Job**: Processa dados e salva resultados particionados por data

## 🔧 Configuração

### Variáveis de Ambiente

Edite `.env` (use `.env.example` como base):

```bash
# Airflow
AIRFLOW__CORE__FERNET_KEY=<sua_chave_aqui>
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

### Portas Utilizadas

| Serviço | Porta | Descrição |
|---------|-------|-----------|
| Airflow UI | 8088 | Interface web |
| Spark Master UI | 8080 | Dashboard Spark |
| Spark Master | 7077 | Submit de jobs |
| MinIO API | 9000 | S3-compatible API |
| MinIO Console | 9001 | Interface web |
| PostgreSQL | 5432 | Banco de dados |

### Modo Cluster Spark (opcional)

Para executar jobs no cluster Spark ao invés de local mode:

1. Descomentar `conn_id="spark_default"` no `SparkSubmitOperator`
2. Configurar conexão no Airflow:

```bash
docker compose exec airflow-webserver airflow connections add \
  spark_default \
  --conn-type spark \
  --conn-host spark-master \
  --conn-port 7077
```

## 🧪 Validação e Testes

```bash
# Validar setup completo
bash scripts/check_setup.sh

# Testar DAG específica
docker compose exec airflow-webserver \
  airflow dags test recife_cultura_etl 2025-10-18

# Testar task específica
docker compose exec airflow-webserver \
  airflow tasks test recife_cultura_etl run_spark_job 2025-10-18

# Ver logs
docker compose logs -f airflow-scheduler
docker compose logs -f spark-master
```

## 📁 Resultados

Após execução bem-sucedida, os dados processados estarão no MinIO:

**MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)

```
datalake/curated/recife/
├── cultura_por_tipo/dt=2025-10-18/
│   └── part-00000-*.parquet
├── cultura_por_bairro/dt=2025-10-18/
│   └── part-00000-*.parquet
└── reports/kpi/dt=2025-10-18/
    └── part-00000-*.json
```

## 🐛 Troubleshooting

### DAG não aparece no Airflow

```bash
# Verificar erros de parsing
docker compose logs airflow-scheduler | grep -i error

# Restart scheduler
docker compose restart airflow-scheduler
```

### Erro de conexão S3A / MinIO

```bash
# Verificar se JARs estão presentes na imagem
docker compose exec airflow-webserver ls -lh /opt/airflow/jars/

# Verificar configuração Spark
docker compose exec spark-master cat /opt/spark/conf/spark-defaults.conf
```

### Porta 8088 já em uso

```bash
# Identificar processo
lsof -i :8088

# Ou ajustar porta no docker-compose.yml
# ports: ["8089:8080"]  # muda porta externa
```

### Rebuild completo

```bash
docker compose down -v  # Remove volumes
docker compose build --no-cache
docker compose up -d
```

## 📚 Desenvolvimento

### Linting

```bash
# Instalar ruff
pip install ruff

# Lint
ruff check include/ airflow/dags/

# Format
ruff format include/ airflow/dags/
```

### Adicionar nova DAG

1. Criar arquivo em `airflow/dags/`
2. Usar timezone `pendulum.timezone("America/Recife")`
3. Definir `catchup=False`
4. Evitar chamadas externas no topo do arquivo

### Adicionar novo job Spark

1. Criar arquivo em `include/jobs/`
2. Usar schemas explícitos (`StructType`)
3. Quebrar em funções pequenas e testáveis
4. Adicionar logs com contagens

## 🎯 Roadmap

- [ ] Testes unitários (pytest) para job Spark
- [ ] Testes de integração para DAG
- [ ] Alertas via Slack/email
- [ ] Métricas com Prometheus + Grafana
- [ ] Great Expectations para DQ avançado
- [ ] dbt para transformações SQL

## 📖 Referências

- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Astronomer Registry](https://registry.astronomer.io/)
- [PEP 8 Style Guide](https://peps.python.org/pep-0008/)
- [Spark S3A Documentation](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

## 📝 Licença

[MIT License](LICENSE)

## 👨‍💻 Autor

**Eduardo Wanderley de Oliveira**

---

⭐ **Se este projeto foi útil, considere dar uma estrela!**
