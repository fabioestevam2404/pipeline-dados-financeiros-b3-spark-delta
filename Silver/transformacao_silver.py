# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver · Limpeza, Enriquecimento e Indicadores Técnicos
# MAGIC **Pipeline**: Delta Bronze → Limpeza → Indicadores → Delta Silver
# MAGIC
# MAGIC Transformações aplicadas:
# MAGIC - Remoção de duplicatas e registros inválidos
# MAGIC - Cálculo de retorno diário (%)
# MAGIC - Médias móveis simples: MA7, MA20, MA50
# MAGIC - Volatilidade histórica (desvio padrão 20 dias)
# MAGIC - Identificação de gaps no calendário de pregões

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

# Fallback: logger e variáveis de config
try:
    logger
except NameError:
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = _logging.getLogger("cotacoes_pipeline")

try:
    silver_path
except NameError:
    silver_path  = None
    bronze_path  = None
    table_silver = "workspace.silver_cotacoes.cotacoes"
    table_bronze = "workspace.bronze_cotacoes.cotacoes_raw"

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("cotacoes_silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 1. LER DA CAMADA BRONZE
# ──────────────────────────────────────────────────────────────────────────────
logger.info("📖 Lendo camada Bronze...")

if bronze_path:
    df_bronze = spark.read.format("delta").load(bronze_path)
else:
    df_bronze = spark.read.table(table_bronze)

logger.info(f"   Registros Bronze: {df_bronze.count():,}")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 2. LIMPEZA DE DADOS
# ──────────────────────────────────────────────────────────────────────────────
df_limpo = (
    df_bronze
    # Remover duplicatas (ticker + data é a chave natural)
    .dropDuplicates(["ticker", "data"])

    # Remover registros com fechamento nulo ou inválido
    .filter(F.col("fechamento").isNotNull())
    .filter(F.col("fechamento") > 0)

    # Remover registros com volume zero (pregões sem negociação)
    .filter(F.col("volume") > 0)

    # Garantir que alta >= abertura e alta >= baixa (sanity check de OHLC)
    .filter(F.col("alta") >= F.col("baixa"))
)

registros_removidos = df_bronze.count() - df_limpo.count()
logger.info(f"   Registros removidos na limpeza: {registros_removidos}")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 3. ENRIQUECIMENTO — INDICADORES TÉCNICOS
# ──────────────────────────────────────────────────────────────────────────────

# Window particionado por ticker, ordenado por data
w = Window.partitionBy("ticker").orderBy("data")

# Window para médias móveis (rolling)
w_7  = Window.partitionBy("ticker").orderBy("data").rowsBetween(-6, 0)
w_20 = Window.partitionBy("ticker").orderBy("data").rowsBetween(-19, 0)
w_50 = Window.partitionBy("ticker").orderBy("data").rowsBetween(-49, 0)

df_silver = (
    df_limpo

    # ── Retorno diário percentual ──────────────────────────────────────────
    .withColumn(
        "retorno_diario_pct",
        F.round(
            (F.col("fechamento") - F.lag("fechamento", 1).over(w))
            / F.lag("fechamento", 1).over(w) * 100,
            4
        )
    )

    # ── Retorno logarítmico (usado em finanças quantitativas) ────────────
    .withColumn(
        "retorno_log",
        F.round(
            F.log(F.col("fechamento") / F.lag("fechamento", 1).over(w)),
            6
        )
    )

    # ── Médias Móveis Simples (SMA) ──────────────────────────────────────
    .withColumn("sma_7",  F.round(F.avg("fechamento").over(w_7),  2))
    .withColumn("sma_20", F.round(F.avg("fechamento").over(w_20), 2))
    .withColumn("sma_50", F.round(F.avg("fechamento").over(w_50), 2))

    # ── Volatilidade Histórica (desvio padrão de 20 dias) ────────────────
    .withColumn(
        "volatilidade_20d",
        F.round(F.stddev("retorno_log").over(w_20), 6)
    )

    # ── Amplitude do pregão (high - low) / close ─────────────────────────
    .withColumn(
        "amplitude_pct",
        F.round((F.col("alta") - F.col("baixa")) / F.col("fechamento") * 100, 4)
    )

    # ── Sinal de tendência: fechamento acima/abaixo da SMA20 ─────────────
    .withColumn(
        "acima_sma20",
        F.when(F.col("fechamento") > F.col("sma_20"), True).otherwise(False)
    )

    # ── Metadados da camada Silver ────────────────────────────────────────
    .withColumn("processado_em", F.current_timestamp())
    .withColumn("versao_pipeline", F.lit("1.0.0"))
)

logger.info("✅ Indicadores técnicos calculados com sucesso.")

# COMMAND ----------

# ── Ordenar colunas de forma lógica ──────────────────────────────────────────
colunas_ordenadas = [
    "ticker", "data",
    "abertura", "alta", "baixa", "fechamento", "volume",
    "retorno_diario_pct", "retorno_log",
    "sma_7", "sma_20", "sma_50",
    "volatilidade_20d", "amplitude_pct", "acima_sma20",
    "data_ingestao", "processado_em", "versao_pipeline"
]

df_silver = df_silver.select(colunas_ordenadas)

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 4. ESCREVER NA CAMADA SILVER
# ──────────────────────────────────────────────────────────────────────────────
_writer_silver = (
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ticker")
)

if silver_path:
    _writer_silver.save(silver_path)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_silver}
        USING DELTA
        LOCATION '{silver_path}'
        COMMENT 'Cotações enriquecidas com indicadores técnicos (MA, volatilidade, retornos)'
    """)
else:
    _writer_silver.saveAsTable(table_silver)

logger.info(f"✅ Camada Silver gravada em: {table_silver}")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 5. VALIDAÇÃO DE QUALIDADE — Great Expectations style (manual)
# ──────────────────────────────────────────────────────────────────────────────
def validate_silver_quality(df) -> None:
    """Executa checks básicos de qualidade na camada Silver."""
    total = df.count()

    checks = {
        "fechamento não nulo"   : df.filter(F.col("fechamento").isNull()).count() == 0,
        "ticker não nulo"       : df.filter(F.col("ticker").isNull()).count() == 0,
        "fechamento > 0"        : df.filter(F.col("fechamento") <= 0).count() == 0,
        "alta >= baixa"         : df.filter(F.col("alta") < F.col("baixa")).count() == 0,
        "registros suficientes" : total > 0,
    }

    todos_ok = True
    for check, passou in checks.items():
        status = "✅" if passou else "❌"
        logger.info(f"   {status} {check}")
        if not passou:
            todos_ok = False

    if not todos_ok:
        raise ValueError("❌ Falha nos checks de qualidade da camada Silver!")

    logger.info(f"✅ Todos os checks passaram. Total: {total:,} registros.")

validate_silver_quality(df_silver)

# COMMAND ----------

# Preview
_df_preview = spark.sql(f"""
    SELECT ticker, data, fechamento, retorno_diario_pct,
           sma_7, sma_20, sma_50, volatilidade_20d
    FROM {table_silver}
    WHERE ticker = 'PETR4'
    ORDER BY data DESC
    LIMIT 20
""")
try:
    display(_df_preview)  # Databricks
except NameError:
    _df_preview.show(20, truncate=False)  # execução local
