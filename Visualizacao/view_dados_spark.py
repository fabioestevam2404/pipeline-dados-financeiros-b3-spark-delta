# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Visualizacao · Análise de Cotações com Spark SQL
# MAGIC **Fonte**: Delta Lake Silver · `silver_cotacoes.cotacoes`

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

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("cotacoes_visualizacao")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# ──────────────────────────────────────────────────────────────────────────────
# 1. CARREGAR TABELA E REGISTRAR VIEW TEMPORÁRIA
# ──────────────────────────────────────────────────────────────────────────────
if silver_path:
    _silver_b3 = os.path.join(silver_path, "b3", "b3_silver.parquet").replace("\\", "/")
    if os.path.exists(_silver_b3):
        df_cotacoes = spark.read.parquet(_silver_b3)
    else:
        df_cotacoes = spark.read.format("delta").load(silver_path)
else:
    df_cotacoes = spark.read.table(table_silver)

df_cotacoes.createOrReplaceTempView("cotacoes")

print(f"✅ Tabela carregada: {df_cotacoes.count():,} registros")
print(f"   Período: {spark.sql('SELECT MIN(data), MAX(data) FROM cotacoes').collect()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Resumo Estatístico por Ação

# COMMAND ----------

print("\n── 1. RESUMO ESTATÍSTICO POR AÇÃO ──────────────────────────────────")
spark.sql("""
    SELECT
        ticker,
        COUNT(*)                              AS total_pregoes,
        MIN(data)                             AS primeira_data,
        MAX(data)                             AS ultima_data,
        ROUND(AVG(fechamento), 2)             AS media_fechamento,
        ROUND(MAX(fechamento), 2)             AS max_fechamento,
        ROUND(MIN(fechamento), 2)             AS min_fechamento,
        ROUND(STDDEV(fechamento), 2)          AS desvio_fechamento,
        ROUND(AVG(volume) / 1e6, 2)           AS volume_medio_mi,
        ROUND(AVG(volatilidade_20d) * 100, 4) AS volatilidade_media_pct
    FROM cotacoes
    GROUP BY ticker
    ORDER BY media_fechamento DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Últimas 10 Cotações por Ação

# COMMAND ----------

print("\n── 2. ÚLTIMAS 10 COTAÇÕES POR AÇÃO ─────────────────────────────────")
spark.sql("""
    SELECT
        ticker, data,
        ROUND(fechamento, 2)             AS fechamento,
        ROUND(retorno_diario_pct, 2)     AS retorno_pct,
        ROUND(sma_7, 2)                  AS sma_7,
        ROUND(sma_20, 2)                 AS sma_20,
        ROUND(sma_50, 2)                 AS sma_50,
        acima_sma20,
        ROUND(volatilidade_20d * 100, 4) AS vol_20d_pct,
        volume
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY data DESC) AS rn
        FROM cotacoes
    )
    WHERE rn <= 10
    ORDER BY ticker, data DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Ranking de Performance no Período

# COMMAND ----------

print("\n── 3. RANKING DE PERFORMANCE NO PERÍODO ────────────────────────────")
spark.sql("""
    WITH primeiros AS (
        SELECT ticker,
               FIRST_VALUE(fechamento) OVER (PARTITION BY ticker ORDER BY data ASC) AS preco_inicial,
               LAST_VALUE(fechamento)  OVER (PARTITION BY ticker ORDER BY data ASC
                                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS preco_final
        FROM cotacoes
    ),
    performance AS (
        SELECT DISTINCT ticker,
               ROUND(preco_inicial, 2) AS preco_inicial,
               ROUND(preco_final, 2)   AS preco_final,
               ROUND((preco_final - preco_inicial) / preco_inicial * 100, 2) AS retorno_periodo_pct
        FROM primeiros
    )
    SELECT *,
           RANK() OVER (ORDER BY retorno_periodo_pct DESC) AS ranking
    FROM performance
    ORDER BY ranking
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Dias de Alta Volatilidade (outliers)

# COMMAND ----------

print("\n── 4. DIAS DE ALTA VOLATILIDADE (OUTLIERS) ─────────────────────────")
spark.sql("""
    WITH stats AS (
        SELECT ticker,
               AVG(ABS(retorno_diario_pct)) AS media_ret,
               STDDEV(retorno_diario_pct)   AS desvio_ret
        FROM cotacoes
        WHERE retorno_diario_pct IS NOT NULL
        GROUP BY ticker
    )
    SELECT
        c.ticker, c.data,
        ROUND(c.fechamento, 2)         AS fechamento,
        ROUND(c.retorno_diario_pct, 2) AS retorno_pct,
        ROUND(c.volume / 1e6, 2)       AS volume_mi,
        ROUND(c.amplitude_pct, 2)      AS amplitude_pct
    FROM cotacoes c
    JOIN stats s ON c.ticker = s.ticker
    WHERE ABS(c.retorno_diario_pct) > (s.media_ret + 2 * s.desvio_ret)
    ORDER BY ABS(c.retorno_diario_pct) DESC
    LIMIT 30
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Correlação de Fechamento Entre Ativos

# COMMAND ----------

print("\n── 5. PIVOT DE FECHAMENTO POR DATA ──────────────────────────────────")
spark.sql("""
    SELECT
        data,
        MAX(CASE WHEN ticker = 'PETR4' THEN fechamento END) AS PETR4,
        MAX(CASE WHEN ticker = 'VALE3' THEN fechamento END) AS VALE3,
        MAX(CASE WHEN ticker = 'ITUB4' THEN fechamento END) AS ITUB4,
        MAX(CASE WHEN ticker = 'BBDC4' THEN fechamento END) AS BBDC4,
        MAX(CASE WHEN ticker = 'ABEV3' THEN fechamento END) AS ABEV3
    FROM cotacoes
    GROUP BY data
    ORDER BY data DESC
    LIMIT 30
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Sinais de Cruzamento de Médias (Golden/Death Cross)

# COMMAND ----------

print("\n── 6. SINAIS GOLDEN/DEATH CROSS ─────────────────────────────────────")
spark.sql("""
    WITH cruzamentos AS (
        SELECT
            ticker, data, fechamento,
            ROUND(sma_7, 2)  AS sma_7,
            ROUND(sma_20, 2) AS sma_20,
            LAG(sma_7)  OVER (PARTITION BY ticker ORDER BY data) AS sma_7_anterior,
            LAG(sma_20) OVER (PARTITION BY ticker ORDER BY data) AS sma_20_anterior
        FROM cotacoes
        WHERE sma_7 IS NOT NULL AND sma_20 IS NOT NULL
    )
    SELECT
        ticker, data, fechamento, sma_7, sma_20,
        CASE
            WHEN sma_7 > sma_20 AND sma_7_anterior <= sma_20_anterior THEN 'GOLDEN CROSS'
            WHEN sma_7 < sma_20 AND sma_7_anterior >= sma_20_anterior THEN 'DEATH CROSS'
        END AS sinal
    FROM cruzamentos
    WHERE
        (sma_7 > sma_20 AND sma_7_anterior <= sma_20_anterior)
        OR
        (sma_7 < sma_20 AND sma_7_anterior >= sma_20_anterior)
    ORDER BY data DESC
""").show(truncate=False)
