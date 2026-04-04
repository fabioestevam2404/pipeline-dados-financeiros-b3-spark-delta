# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold · Agregações para Dashboard
# MAGIC **Pipeline**: Delta Silver → Métricas Agregadas → Delta Gold
# MAGIC
# MAGIC Tabelas geradas:
# MAGIC - `gold_cotacoes.resumo_diario`     — OHLCV + indicadores do último pregão por ticker
# MAGIC - `gold_cotacoes.performance_mensal` — Retorno mensal e volatilidade por ticker/mês
# MAGIC - `gold_cotacoes.ranking_acoes`      — Ranking de performance no período total
# MAGIC - `gold_cotacoes.sinais_tecnicos`    — Golden/Death Cross e posição vs médias móveis

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
    gold_path
except NameError:
    silver_path  = None
    gold_path    = None
    table_silver = "workspace.silver_cotacoes.cotacoes"
    table_gold   = "workspace.gold_cotacoes.metricas_acoes"

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("cotacoes_gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 1. LER CAMADA SILVER
# ──────────────────────────────────────────────────────────────────────────────
logger.info("📖 Lendo camada Silver...")

if silver_path:
    df_silver = spark.read.format("delta").load(silver_path)
else:
    df_silver = spark.read.table(table_silver)

df_silver.createOrReplaceTempView("silver")
total = df_silver.count()
logger.info(f"   Registros Silver: {total:,}")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 2. RESUMO DIÁRIO — último pregão por ticker
#    Usado em: cards de cabeçalho do dashboard (preço atual, variação do dia)
# ──────────────────────────────────────────────────────────────────────────────
logger.info("🔨 Gerando tabela: resumo_diario...")

df_resumo_diario = spark.sql("""
    WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY data DESC) AS rn
        FROM silver
    )
    SELECT
        ticker,
        data                                            AS ultima_data,
        ROUND(abertura,  2)                             AS abertura,
        ROUND(alta,      2)                             AS alta,
        ROUND(baixa,     2)                             AS baixa,
        ROUND(fechamento, 2)                            AS fechamento,
        volume,
        ROUND(retorno_diario_pct, 4)                    AS retorno_diario_pct,
        ROUND(sma_7,  2)                                AS sma_7,
        ROUND(sma_20, 2)                                AS sma_20,
        ROUND(sma_50, 2)                                AS sma_50,
        ROUND(volatilidade_20d * 100, 4)                AS volatilidade_20d_pct,
        ROUND(amplitude_pct, 4)                         AS amplitude_pct,
        acima_sma20,
        CASE
            WHEN retorno_diario_pct > 0 THEN 'ALTA'
            WHEN retorno_diario_pct < 0 THEN 'BAIXA'
            ELSE 'NEUTRO'
        END                                             AS direcao_dia,
        current_timestamp()                             AS atualizado_em
    FROM ranked
    WHERE rn = 1
    ORDER BY ticker
""")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 3. PERFORMANCE MENSAL — retorno e volatilidade por ticker/mês
#    Usado em: gráfico de barras mensais, heatmap de retornos
# ──────────────────────────────────────────────────────────────────────────────
logger.info("🔨 Gerando tabela: performance_mensal...")

df_performance_mensal = spark.sql("""
    WITH base AS (
        SELECT
            ticker,
            DATE_FORMAT(data, 'yyyy-MM')            AS ano_mes,
            YEAR(data)                              AS ano,
            MONTH(data)                             AS mes,
            fechamento,
            retorno_diario_pct,
            volume,
            volatilidade_20d,
            FIRST_VALUE(fechamento) OVER (
                PARTITION BY ticker, DATE_FORMAT(data, 'yyyy-MM')
                ORDER BY data ASC
            )                                       AS fechamento_abertura_mes,
            LAST_VALUE(fechamento) OVER (
                PARTITION BY ticker, DATE_FORMAT(data, 'yyyy-MM')
                ORDER BY data ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                       AS fechamento_fechamento_mes
        FROM silver
    )
    SELECT
        ticker,
        ano_mes,
        ano,
        mes,
        COUNT(*)                                            AS pregoes_no_mes,
        ROUND(fechamento_abertura_mes,   2)                 AS preco_abertura_mes,
        ROUND(fechamento_fechamento_mes, 2)                 AS preco_fechamento_mes,
        ROUND(
            (fechamento_fechamento_mes - fechamento_abertura_mes)
            / fechamento_abertura_mes * 100, 4
        )                                                   AS retorno_mensal_pct,
        ROUND(MAX(fechamento),  2)                          AS max_mes,
        ROUND(MIN(fechamento),  2)                          AS min_mes,
        ROUND(AVG(volume) / 1e6, 2)                         AS volume_medio_mi,
        ROUND(SUM(volume)  / 1e6, 2)                        AS volume_total_mi,
        ROUND(AVG(ABS(retorno_diario_pct)), 4)              AS volatilidade_media_diaria_pct,
        ROUND(AVG(volatilidade_20d) * 100,  4)              AS volatilidade_20d_media_pct,
        current_timestamp()                                 AS atualizado_em
    FROM base
    GROUP BY
        ticker, ano_mes, ano, mes,
        fechamento_abertura_mes, fechamento_fechamento_mes
    ORDER BY ticker, ano_mes
""")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 4. RANKING DE AÇÕES — performance acumulada no período
#    Usado em: tabela de ranking, gráfico de barras horizontais
# ──────────────────────────────────────────────────────────────────────────────
logger.info("🔨 Gerando tabela: ranking_acoes...")

df_ranking = spark.sql("""
    WITH extremos AS (
        SELECT
            ticker,
            FIRST_VALUE(fechamento) OVER (
                PARTITION BY ticker ORDER BY data ASC
            )                                               AS preco_inicial,
            LAST_VALUE(fechamento)  OVER (
                PARTITION BY ticker ORDER BY data ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )                                               AS preco_final,
            MIN(data) OVER (PARTITION BY ticker)            AS data_inicio,
            MAX(data) OVER (PARTITION BY ticker)            AS data_fim
        FROM silver
    ),
    stats AS (
        SELECT
            ticker,
            COUNT(*)                                        AS total_pregoes,
            ROUND(AVG(retorno_diario_pct),  4)              AS retorno_medio_diario,
            ROUND(STDDEV(retorno_diario_pct), 4)            AS desvio_retorno,
            ROUND(AVG(volume) / 1e6, 2)                     AS volume_medio_mi,
            ROUND(AVG(volatilidade_20d) * 100, 4)           AS volatilidade_media_pct,
            SUM(CASE WHEN retorno_diario_pct > 0 THEN 1 ELSE 0 END) AS dias_alta,
            SUM(CASE WHEN retorno_diario_pct < 0 THEN 1 ELSE 0 END) AS dias_baixa
        FROM silver
        WHERE retorno_diario_pct IS NOT NULL
        GROUP BY ticker
    )
    SELECT DISTINCT
        e.ticker,
        e.data_inicio,
        e.data_fim,
        ROUND(e.preco_inicial, 2)                           AS preco_inicial,
        ROUND(e.preco_final,   2)                           AS preco_final,
        ROUND((e.preco_final - e.preco_inicial)
              / e.preco_inicial * 100, 2)                   AS retorno_periodo_pct,
        s.total_pregoes,
        s.retorno_medio_diario,
        s.desvio_retorno,
        s.volume_medio_mi,
        s.volatilidade_media_pct,
        s.dias_alta,
        s.dias_baixa,
        ROUND(s.dias_alta * 100.0 / (s.dias_alta + s.dias_baixa), 1) AS pct_dias_alta,
        ROUND(
            (e.preco_final - e.preco_inicial) / e.preco_inicial * 100
            / NULLIF(s.desvio_retorno, 0), 4
        )                                                   AS sharpe_proxy,
        RANK() OVER (
            ORDER BY (e.preco_final - e.preco_inicial) / e.preco_inicial DESC
        )                                                   AS ranking_retorno,
        current_timestamp()                                 AS atualizado_em
    FROM extremos e
    JOIN stats s ON e.ticker = s.ticker
    ORDER BY ranking_retorno
""")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 5. SINAIS TÉCNICOS — estado atual de cada ticker
#    Usado em: semáforo de sinais, tabela de alertas do dashboard
# ──────────────────────────────────────────────────────────────────────────────
logger.info("🔨 Gerando tabela: sinais_tecnicos...")

df_sinais = spark.sql("""
    WITH ultimos AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY data DESC) AS rn,
               LAG(sma_7)  OVER (PARTITION BY ticker ORDER BY data)       AS sma_7_ant,
               LAG(sma_20) OVER (PARTITION BY ticker ORDER BY data)       AS sma_20_ant,
               LAG(sma_50) OVER (PARTITION BY ticker ORDER BY data)       AS sma_50_ant
        FROM silver
        WHERE sma_7 IS NOT NULL AND sma_20 IS NOT NULL AND sma_50 IS NOT NULL
    )
    SELECT
        ticker,
        data                                                AS ultima_data,
        ROUND(fechamento, 2)                                AS fechamento,
        ROUND(sma_7,  2)                                    AS sma_7,
        ROUND(sma_20, 2)                                    AS sma_20,
        ROUND(sma_50, 2)                                    AS sma_50,

        -- Posição relativa às médias
        CASE
            WHEN fechamento > sma_7 AND fechamento > sma_20 AND fechamento > sma_50
                THEN 'ACIMA DE TODAS'
            WHEN fechamento < sma_7 AND fechamento < sma_20 AND fechamento < sma_50
                THEN 'ABAIXO DE TODAS'
            ELSE 'ENTRE MÉDIAS'
        END                                                 AS posicao_medias,

        -- Cruzamento de médias (7 x 20)
        CASE
            WHEN sma_7 > sma_20 AND sma_7_ant <= sma_20_ant THEN 'GOLDEN CROSS'
            WHEN sma_7 < sma_20 AND sma_7_ant >= sma_20_ant THEN 'DEATH CROSS'
            WHEN sma_7 > sma_20                              THEN 'TENDÊNCIA ALTA'
            ELSE                                                  'TENDÊNCIA BAIXA'
        END                                                 AS sinal_medias,

        -- Tendência de longo prazo (20 x 50)
        CASE
            WHEN sma_20 > sma_50 THEN 'BULL'
            ELSE                      'BEAR'
        END                                                 AS tendencia_longo_prazo,

        ROUND(volatilidade_20d * 100, 4)                    AS volatilidade_20d_pct,
        ROUND(retorno_diario_pct, 4)                        AS retorno_ultimo_dia_pct,
        acima_sma20,
        current_timestamp()                                 AS atualizado_em
    FROM ultimos
    WHERE rn = 1
    ORDER BY ticker
""")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 6. ESCREVER TODAS AS TABELAS GOLD
# ──────────────────────────────────────────────────────────────────────────────

def salvar_tabela_gold(df, nome_tabela, particionar_por=None):
    """Salva um DataFrame na camada Gold (Delta Lake)."""
    tabela_catalogo = f"{table_gold.rsplit('.', 1)[0]}.{nome_tabela}"

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if particionar_por:
        writer = writer.partitionBy(particionar_por)

    if gold_path:
        caminho = os.path.join(gold_path, nome_tabela).replace("\\", "/")
        writer.save(caminho)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {tabela_catalogo}
            USING DELTA
            LOCATION '{caminho}'
        """)
        logger.info(f"   ✅ {tabela_catalogo} → {caminho}")
    else:
        writer.saveAsTable(tabela_catalogo)
        logger.info(f"   ✅ {tabela_catalogo}")


logger.info("\n💾 Gravando tabelas Gold...")
salvar_tabela_gold(df_resumo_diario,      "resumo_diario")
salvar_tabela_gold(df_performance_mensal, "performance_mensal", particionar_por="ticker")
salvar_tabela_gold(df_ranking,            "ranking_acoes")
salvar_tabela_gold(df_sinais,             "sinais_tecnicos")

logger.info("✅ Camada Gold gravada com sucesso!")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# 7. PREVIEW DAS TABELAS GERADAS
# ──────────────────────────────────────────────────────────────────────────────
db_gold_name = table_gold.rsplit(".", 1)[0]

print("\n── RESUMO DIÁRIO ────────────────────────────────────────────────────")
_prev1 = spark.sql(f"SELECT ticker, ultima_data, fechamento, retorno_diario_pct, direcao_dia, sma_20, acima_sma20 FROM {db_gold_name}.resumo_diario ORDER BY ticker")
try:
    display(_prev1)
except NameError:
    _prev1.show(truncate=False)

print("\n── PERFORMANCE MENSAL (últimos 3 meses) ─────────────────────────────")
_prev2 = spark.sql(f"""
    SELECT ticker, ano_mes, retorno_mensal_pct, pregoes_no_mes, volume_total_mi, volatilidade_20d_media_pct
    FROM {db_gold_name}.performance_mensal
    ORDER BY ticker, ano_mes DESC
    LIMIT 15
""")
try:
    display(_prev2)
except NameError:
    _prev2.show(truncate=False)

print("\n── RANKING DE AÇÕES ─────────────────────────────────────────────────")
_prev3 = spark.sql(f"SELECT ranking_retorno, ticker, preco_inicial, preco_final, retorno_periodo_pct, pct_dias_alta, sharpe_proxy FROM {db_gold_name}.ranking_acoes ORDER BY ranking_retorno")
try:
    display(_prev3)
except NameError:
    _prev3.show(truncate=False)

print("\n── SINAIS TÉCNICOS ──────────────────────────────────────────────────")
_prev4 = spark.sql(f"SELECT ticker, ultima_data, fechamento, sinal_medias, posicao_medias, tendencia_longo_prazo, volatilidade_20d_pct FROM {db_gold_name}.sinais_tecnicos ORDER BY ticker")
try:
    display(_prev4)
except NameError:
    _prev4.show(truncate=False)
