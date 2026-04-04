# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Config — Parâmetros Globais
# MAGIC Pipeline de Ingestão B3 · Alpha Vantage · PySpark · Delta Lake

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# TICKERS MONITORADOS — Ações da B3
# Adicione ou remova tickers conforme necessário.
# O sufixo .SA é adicionado automaticamente na função de busca.
# ──────────────────────────────────────────────────────────────────────────────
tickers = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"]

# ──────────────────────────────────────────────────────────────────────────────
# API KEY — Alpha Vantage
# Obtenha sua chave gratuita em: https://www.alphavantage.co/support/#api-key
# Plano gratuito: 25 requests/dia · 5 requests/minuto
# No Databricks: configure o secret com:
#   databricks secrets create-scope --scope cotacoes
#   databricks secrets put --scope cotacoes --key alpha_vantage_key
# ──────────────────────────────────────────────────────────────────────────────
import os as _os_env
try:
    # Databricks: lê do Secret Scope (seguro, sem expor a chave no código)
    API_KEY = dbutils.secrets.get(scope="cotacoes", key="alpha_vantage_key")  # noqa: F821
except Exception:
    # Execução local: lê da variável de ambiente ou usa valor padrão
    API_KEY = _os_env.environ.get("ALPHA_VANTAGE_API_KEY", "SUA_CHAVE_AQUI")

# ──────────────────────────────────────────────────────────────────────────────
# CAMINHOS DE ARMAZENAMENTO DELTA LAKE
# ──────────────────────────────────────────────────────────────────────────────
import os as _os
_is_databricks = "DATABRICKS_RUNTIME_VERSION" in _os.environ

if _is_databricks:
    # Unity Catalog com DBFS desabilitado: usar tabelas gerenciadas (sem LOCATION)
    # O storage é gerenciado automaticamente pelo Unity Catalog
    bronze_path = None
    silver_path = None
    gold_path   = None
else:
    _base = _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "data"))
    bronze_path = _os.path.join(_base, "bronze").replace("\\", "/")
    silver_path = _os.path.join(_base, "silver").replace("\\", "/")
    gold_path   = _os.path.join(_base, "gold").replace("\\", "/")

# ──────────────────────────────────────────────────────────────────────────────
# NOMES DAS TABELAS NO CATÁLOGO
# ──────────────────────────────────────────────────────────────────────────────
catalog        = "workspace"
db_bronze      = "bronze_cotacoes"
db_silver      = "silver_cotacoes"
db_gold        = "gold_cotacoes"
table_bronze   = f"{catalog}.{db_bronze}.cotacoes_raw"
table_silver   = f"{catalog}.{db_silver}.cotacoes"
table_gold     = f"{catalog}.{db_gold}.metricas_acoes"

# ──────────────────────────────────────────────────────────────────────────────
# PARÂMETROS DA API
# ──────────────────────────────────────────────────────────────────────────────
API_BASE_URL   = "https://www.alphavantage.co/query"
OUTPUT_SIZE    = "compact"   # "compact" = 100 dias | "full" = histórico completo
API_FUNCTION   = "TIME_SERIES_DAILY"

# ──────────────────────────────────────────────────────────────────────────────
# CONTROLE DE RATE LIMIT
# ──────────────────────────────────────────────────────────────────────────────
REQUEST_DELAY_SECONDS = 13   # ~5 req/min no plano gratuito (60s / 5 = 12s + margem)
MAX_RETRIES           = 3
RETRY_BACKOFF_SECONDS = 30

# COMMAND ----------

from pyspark.sql import SparkSession as _SparkSession
from delta import configure_spark_with_delta_pip as _configure_delta
_spark = _configure_delta(
    _SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# Detectar catálogo ativo (Unity Catalog pode ter catálogo diferente de "workspace")
if _is_databricks:
    try:
        catalog = _spark.sql("SELECT current_catalog()").collect()[0][0]
    except Exception:
        catalog = "workspace"

try:
    if _is_databricks:
        _spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db_bronze} COMMENT 'Camada Bronze — dados brutos da API Alpha Vantage'")
        _spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db_silver} COMMENT 'Camada Silver — dados limpos e enriquecidos'")
        _spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db_gold}   COMMENT 'Camada Gold — métricas e indicadores de mercado'")
    else:
        _spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_bronze}")
        _spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_silver}")
        _spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_gold}")
except Exception as _e:
    print(f"⚠️  Schema/Database não criado automaticamente: {_e}")
    print("   Os caminhos Delta Lake (bronze_path, silver_path, gold_path) continuam válidos.")

# Atualizar nomes das tabelas com catálogo detectado
table_bronze = f"{catalog}.{db_bronze}.cotacoes_raw"
table_silver = f"{catalog}.{db_silver}.cotacoes"
table_gold   = f"{catalog}.{db_gold}.metricas_acoes"

# COMMAND ----------

print("✅ Configurações carregadas com sucesso!")
print(f"   Tickers  : {tickers}")
print(f"   Bronze   : {bronze_path}")
print(f"   Silver   : {silver_path}")
print(f"   Gold     : {gold_path}")
