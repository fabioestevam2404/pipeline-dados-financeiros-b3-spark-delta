# Databricks notebook source
# MAGIC %md
# MAGIC # 🔶 Bronze · Ingestão de Cotações B3 via Alpha Vantage
# MAGIC **Pipeline**: Alpha Vantage API → Pandas → PySpark DataFrame → Delta Lake (Bronze)
# MAGIC
# MAGIC | Campo         | Detalhe                                      |
# MAGIC |---------------|----------------------------------------------|
# MAGIC | Camada        | Bronze (dados brutos, sem transformação)      |
# MAGIC | Fonte         | Alpha Vantage `TIME_SERIES_DAILY`            |
# MAGIC | Destino       | Delta Lake · `bronze_cotacoes.cotacoes_raw`  |
# MAGIC | Modo escrita  | `append` (acumula histórico)                 |
# MAGIC | Agendamento   | Diário após fechamento do pregão (17h BRT)   |

# COMMAND ----------

# MAGIC %run /Workspace/ProjetoAPI_cotações/Config/config

# COMMAND ----------

# MAGIC %run /Workspace/ProjetoAPI_cotações/Utils/helpers

# COMMAND ----------

import sys, os
try:
    if os.path.isdir(r"C:\hadoop"):
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from Config.config import *
    from utils.helpers import *
except NameError:
    pass  # Databricks: importações já feitas via %run

# Fallback: garantir que variáveis de config estão disponíveis
try:
    MAX_RETRIES
except NameError:
    import os as _os2
    tickers               = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"]
    API_KEY               = _os2.environ.get("ALPHA_VANTAGE_API_KEY", "XF5JLDVTEE8RMIUN")
    API_BASE_URL          = "https://www.alphavantage.co/query"
    API_FUNCTION          = "TIME_SERIES_DAILY"
    OUTPUT_SIZE           = "compact"
    REQUEST_DELAY_SECONDS = 13
    MAX_RETRIES           = 3
    RETRY_BACKOFF_SECONDS = 30
    bronze_path           = None
    silver_path           = None
    gold_path             = None
    catalog               = "workspace"
    table_bronze          = f"{catalog}.bronze_cotacoes.cotacoes_raw"
    table_silver          = f"{catalog}.silver_cotacoes.cotacoes"
    table_gold            = f"{catalog}.gold_cotacoes.metricas_acoes"

# Fallback: garantir que retry e logger estão disponíveis
try:
    retry
except NameError:
    import logging as _logging
    from functools import wraps as _wraps
    _logger = _logging.getLogger("cotacoes_pipeline")
    if not _logging.getLogger("cotacoes_pipeline").handlers:
        _logging.basicConfig(level=_logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = _logger

    def retry(max_attempts=3, delay_seconds=30, backoff=2.0):
        def decorator(func):
            @_wraps(func)
            def wrapper(*args, **kwargs):
                wait = delay_seconds
                for attempt in range(1, max_attempts + 1):
                    try:
                        return func(*args, **kwargs)
                    except Exception:
                        if attempt == max_attempts:
                            raise
                        import time as _t
                        _t.sleep(wait)
                        wait *= backoff
            return wrapper
        return decorator

    def validate_api_response(data, ticker):
        if "Note" in data or "Error Message" in data or "Information" in data:
            return False
        return "Time Series (Daily)" in data

    def rate_limit_wait(seconds, *_):
        import time as _t
        _t.sleep(seconds)

    def log_table_stats(spark, table_name):
        try:
            spark.sql(f"SELECT ticker, COUNT(*) as total FROM {table_name} GROUP BY ticker").show()
        except Exception as e:
            print(f"⚠️ Estatísticas indisponíveis: {e}")

import requests
import pandas as pd
import time
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, DateType, LongType
)
from delta import configure_spark_with_delta_pip

# Obter sessão Spark ativa (já disponível no Databricks)
spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("cotacoes_bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# SCHEMA EXPLÍCITO — define os tipos antes de criar o DataFrame Spark
# Evita inferência automática incorreta de tipos
# ──────────────────────────────────────────────────────────────────────────────
SCHEMA_BRONZE = StructType([
    StructField("data",          DateType(),      nullable=False),
    StructField("abertura",      DoubleType(),    nullable=True),
    StructField("alta",          DoubleType(),    nullable=True),
    StructField("baixa",         DoubleType(),    nullable=True),
    StructField("fechamento",    DoubleType(),    nullable=False),
    StructField("volume",        LongType(),      nullable=True),
    StructField("ticker",        StringType(),    nullable=False),
    StructField("data_ingestao", TimestampType(), nullable=False),
])

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# FUNÇÃO PRINCIPAL: BUSCAR DADOS DE UMA AÇÃO
# ──────────────────────────────────────────────────────────────────────────────
@retry(max_attempts=MAX_RETRIES, delay_seconds=RETRY_BACKOFF_SECONDS)
def buscar_dados_acao(ticker_b3: str, api_key: str) -> Optional[pd.DataFrame]:
    """
    Consulta a Alpha Vantage API e retorna um DataFrame Pandas
    com as cotações diárias do ticker informado.

    Parâmetros:
        ticker_b3 : Código da ação na B3 (ex: 'PETR4')
        api_key   : Chave da API Alpha Vantage

    Retorna:
        pd.DataFrame com colunas:
            data, abertura, alta, baixa, fechamento, volume, ticker, data_ingestao
        None em caso de erro não recuperável.

    Notas:
        - Ações da B3 precisam do sufixo .SA no símbolo (PETR4 → PETR4.SA)
        - outputsize='compact' retorna os últimos 100 pregões
        - O decorator @retry re-executa em caso de falha de rede
    """
    ticker_api = ticker_b3 + ".SA"
    logger.info(f"[{ticker_b3}] 📡 Buscando dados da API...")

    params = {
        "function":   API_FUNCTION,
        "symbol":     ticker_api,
        "apikey":     api_key,
        "outputsize": OUTPUT_SIZE,
        "datatype":   "json",
    }

    response = requests.get(API_BASE_URL, params=params, timeout=30)
    response.raise_for_status()  # lança exceção para erros HTTP (4xx, 5xx)

    data = response.json()

    # Validar estrutura da resposta (rate limit, ticker inválido, etc.)
    if not validate_api_response(data, ticker_b3):
        return None

    # ── Transformar JSON → Pandas DataFrame ──────────────────────────────────
    series = data["Time Series (Daily)"]

    df = pd.DataFrame.from_dict(series, orient="index")
    df.index.name = "data"
    df.reset_index(inplace=True)

    # Renomear colunas removendo o prefixo numérico da API ("1. open" → "abertura")
    df.columns = ["data", "abertura", "alta", "baixa", "fechamento", "volume"]

    # Converter tipos
    df["data"]        = pd.to_datetime(df["data"]).dt.date
    df["abertura"]    = df["abertura"].astype(float)
    df["alta"]        = df["alta"].astype(float)
    df["baixa"]       = df["baixa"].astype(float)
    df["fechamento"]  = df["fechamento"].astype(float)
    df["volume"]      = df["volume"].astype("int64")

    # Adicionar metadados de engenharia
    df["ticker"]        = ticker_b3
    df["data_ingestao"] = datetime.now()

    # Ordenar por data (mais antigo → mais recente)
    df = df.sort_values("data").reset_index(drop=True)

    logger.info(f"[{ticker_b3}] ✅ {len(df)} registros obtidos "
                f"({df['data'].min()} → {df['data'].max()})")
    return df

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# EXECUÇÃO: COLETAR TODOS OS TICKERS
# ──────────────────────────────────────────────────────────────────────────────
logger.info("=" * 60)
logger.info("🚀 INICIANDO INGESTÃO DE COTAÇÕES B3")
logger.info(f"   Tickers  : {tickers}")
logger.info(f"   Destino  : {table_bronze}")
logger.info("=" * 60)

dfs_coletados = []
tickers_com_erro = []

for i, ticker in enumerate(tickers):
    # Rate limit: aguardar entre requisições (exceto na primeira)
    if i > 0:
        rate_limit_wait(REQUEST_DELAY_SECONDS, ticker)

    try:
        df = buscar_dados_acao(ticker, API_KEY)
        if df is not None:
            dfs_coletados.append(df)
        else:
            tickers_com_erro.append(ticker)
    except Exception as e:
        logger.error(f"[{ticker}] ❌ Falha definitiva após retries: {e}")
        tickers_com_erro.append(ticker)

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# CONSOLIDAÇÃO E ESCRITA NO DELTA LAKE
# ──────────────────────────────────────────────────────────────────────────────
if not dfs_coletados:
    raise RuntimeError(
        "❌ Nenhum dado coletado. Verifique a API Key e os tickers configurados."
    )

# Concatenar todos os DataFrames Pandas em um único
df_consolidado = pd.concat(dfs_coletados, ignore_index=True)
logger.info(f"📦 Total consolidado: {len(df_consolidado):,} registros de "
            f"{df_consolidado['ticker'].nunique()} tickers")

# ── Converter Pandas → Spark DataFrame com schema explícito ──────────────────
df_spark = spark.createDataFrame(df_consolidado, schema=SCHEMA_BRONZE)

# Garantir particionamento por ticker para otimizar leituras futuras
df_spark = df_spark.repartition("ticker")

# ── Remover possíveis duplicatas antes de escrever ───────────────────────────
# (importante em execuções diárias: evita duplicar o dia atual)
df_spark = df_spark.dropDuplicates(["ticker", "data"])

logger.info(f"📊 Spark DataFrame: {df_spark.count():,} registros · "
            f"{len(df_spark.columns)} colunas")

# ── Escrita Delta Lake em modo APPEND ────────────────────────────────────────
_writer = (
    df_spark
    .write
    .format("delta")
    .mode("append")
    .partitionBy("ticker")
    .option("mergeSchema", "true")
    .option("delta.autoOptimize.optimizeWrite", "true")
    .option("delta.autoOptimize.autoCompact", "true")
)

if bronze_path:
    # Execução local: salva no caminho físico e registra no catálogo
    _writer.save(bronze_path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_bronze}
        USING DELTA
        LOCATION '{bronze_path}'
        COMMENT 'Cotações diárias de ações da B3 — dados brutos Alpha Vantage'
    """)
else:
    # Databricks Unity Catalog: tabela gerenciada (storage automático)
    _writer.saveAsTable(table_bronze)

logger.info(f"✅ Dados gravados com sucesso em: {table_bronze}")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# SUMÁRIO DA EXECUÇÃO
# ──────────────────────────────────────────────────────────────────────────────
logger.info("\n" + "=" * 60)
logger.info("📋 SUMÁRIO DA EXECUÇÃO")
logger.info("=" * 60)

log_table_stats(spark, table_bronze)

if tickers_com_erro:
    logger.warning(f"⚠️  Tickers com falha: {tickers_com_erro}")

# Verificar histórico de versões do Delta (time travel)
if bronze_path:
    spark.sql(f"DESCRIBE HISTORY delta.`{bronze_path}`").show(5, truncate=False)
else:
    spark.sql(f"DESCRIBE HISTORY {table_bronze}").show(5, truncate=False)

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# PREVIEW DOS DADOS INGERIDOS
# ──────────────────────────────────────────────────────────────────────────────
_df_preview = spark.sql(f"""
    SELECT ticker, data, abertura, alta, baixa, fechamento, volume, data_ingestao
    FROM {table_bronze}
    ORDER BY ticker, data DESC
    LIMIT 20
""")
try:
    display(_df_preview)  # Databricks
except NameError:
    _df_preview.show(20, truncate=False)  # execução local
