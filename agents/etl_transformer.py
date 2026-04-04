"""
Agente: etl-transformer
Transformação Bronze -> Silver -> Gold usando PySpark + Delta Lake.
Processa dados BCB e B3, incluindo forward-fill para PIB trimestral.
"""

import os
import sys
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("etl-transformer")

# Configurar Hadoop para Windows antes de importar PySpark
if os.path.isdir(r"C:\hadoop"):
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_BCB = os.path.join(BASE_DIR, "data", "bronze", "bcb")
BRONZE_B3  = os.path.join(BASE_DIR, "data", "bronze", "b3")
SILVER_BCB = os.path.join(BASE_DIR, "data", "silver", "bcb")
SILVER_B3  = os.path.join(BASE_DIR, "data", "silver", "b3")
GOLD_PATH  = os.path.join(BASE_DIR, "data", "gold")


def _get_spark():
    """Inicializa sessão Spark com Delta Lake."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    return configure_spark_with_delta_pip(
        SparkSession.builder
        .appName("etl_transformer")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
    ).getOrCreate()


# ── Bronze → Silver BCB ────────────────────────────────────────────────────────

def transform_bcb_bronze_to_silver(spark) -> dict:
    """Lê todos os JSONs BCB Bronze e produz Silver BCB."""
    import pandas as pd
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, DateType
    )

    logger.info("Bronze → Silver BCB")
    os.makedirs(SILVER_BCB, exist_ok=True)

    bcb_files = [f for f in os.listdir(BRONZE_BCB) if f.endswith(".json")]
    if not bcb_files:
        logger.warning("Nenhum arquivo BCB Bronze encontrado.")
        return {"status": "skip", "registros": 0}

    registros = []
    for fname in sorted(bcb_files):
        fpath = os.path.join(BRONZE_BCB, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            payload = json.load(f)

        serie_name = payload.get("serie_name", "desconhecida")
        serie_code = payload.get("serie_code", 0)
        dados      = payload.get("data", [])

        for row in dados:
            val_str = str(row.get("valor", "")).replace(",", ".")
            try:
                val = float(val_str)
            except (ValueError, TypeError):
                val = None

            registros.append({
                "serie_name": serie_name,
                "serie_code": str(serie_code),
                "data_str":   row.get("data", ""),
                "valor":      val,
            })

    if not registros:
        return {"status": "vazio", "registros": 0}

    # Converter datas BCB (formato DD/MM/YYYY)
    df_pd = pd.DataFrame(registros)
    df_pd["data"] = pd.to_datetime(df_pd["data_str"], format="%d/%m/%Y", errors="coerce")
    df_pd = df_pd.dropna(subset=["data"])
    df_pd["data"] = df_pd["data"].dt.date
    df_pd = df_pd.drop(columns=["data_str"])
    df_pd = df_pd.sort_values(["serie_name", "data"]).reset_index(drop=True)

    schema = StructType([
        StructField("serie_name", StringType(), False),
        StructField("serie_code", StringType(), True),
        StructField("valor",      DoubleType(), True),
        StructField("data",       DateType(),   False),
    ])

    df_spark = spark.createDataFrame(df_pd, schema=schema)
    df_spark = df_spark.withColumn("processado_em", F.current_timestamp())

    # Forward-fill para PIB (trimestral → diário via última observação disponível)
    df_pib = df_spark.filter(F.col("serie_name") == "pib_variacao")
    df_outros = df_spark.filter(F.col("serie_name") != "pib_variacao")

    # PIB: expandir para todos os dias do período usando last obs forward fill
    if df_pib.count() > 0:
        from pyspark.sql import Window
        w_pib = Window.partitionBy("serie_name").orderBy("data")
        df_pib = df_pib.withColumn(
            "valor_ffill",
            F.last("valor", ignorenulls=True).over(
                w_pib.rowsBetween(Window.unboundedPreceding, 0)
            )
        ).withColumn("valor", F.col("valor_ffill")).drop("valor_ffill")

    df_silver = df_outros.union(df_pib)

    # Salvar Silver BCB como Delta
    (
        df_silver
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("serie_name")
        .save(SILVER_BCB.replace("\\", "/"))
    )

    n = df_silver.count()
    logger.info(f"Silver BCB gravado: {n:,} registros em {SILVER_BCB}")
    return {"status": "ok", "registros": n, "path": SILVER_BCB}


# ── Bronze → Silver B3 ─────────────────────────────────────────────────────────

def transform_b3_bronze_to_silver(spark) -> dict:
    """Lê JSONs B3 Bronze, aplica limpeza e indicadores técnicos → Silver B3."""
    import pandas as pd
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, DateType,
        LongType, TimestampType
    )

    logger.info("Bronze → Silver B3")
    os.makedirs(SILVER_B3, exist_ok=True)

    b3_files = [f for f in os.listdir(BRONZE_B3) if f.endswith(".json")]
    if not b3_files:
        logger.warning("Nenhum arquivo B3 Bronze encontrado.")
        return {"status": "skip", "registros": 0}

    registros = []
    for fname in sorted(b3_files):
        fpath = os.path.join(BRONZE_B3, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            payload = json.load(f)

        ticker   = payload.get("ticker", "")
        data_obj = payload.get("data", {})
        ts       = data_obj.get("time_series_daily", {}) if isinstance(data_obj, dict) else {}
        ingest   = payload.get("ingested_at", datetime.now().isoformat())

        for data_str, vals in ts.items():
            try:
                registros.append({
                    "ticker":       ticker,
                    "data":         pd.to_datetime(data_str).date(),
                    "abertura":     float(vals.get("1. open", 0)),
                    "alta":         float(vals.get("2. high", 0)),
                    "baixa":        float(vals.get("3. low", 0)),
                    "fechamento":   float(vals.get("4. close", 0)),
                    "volume":       int(float(vals.get("5. volume", 0))),
                    "data_ingestao": pd.to_datetime(ingest),
                })
            except Exception:
                pass

    if not registros:
        return {"status": "vazio", "registros": 0}

    df_pd = pd.DataFrame(registros)
    df_pd = df_pd.sort_values(["ticker", "data"]).reset_index(drop=True)

    schema = StructType([
        StructField("ticker",       StringType(),    False),
        StructField("data",         DateType(),      False),
        StructField("abertura",     DoubleType(),    True),
        StructField("alta",         DoubleType(),    True),
        StructField("baixa",        DoubleType(),    True),
        StructField("fechamento",   DoubleType(),    False),
        StructField("volume",       LongType(),      True),
        StructField("data_ingestao", TimestampType(), True),
    ])

    df_spark = spark.createDataFrame(df_pd, schema=schema)

    # Limpeza
    df_spark = (
        df_spark
        .dropDuplicates(["ticker", "data"])
        .filter(F.col("fechamento") > 0)
        .filter(F.col("volume") > 0)
        .filter(F.col("alta") >= F.col("baixa"))
    )

    # Indicadores técnicos
    w = Window.partitionBy("ticker").orderBy("data")
    w7  = Window.partitionBy("ticker").orderBy("data").rowsBetween(-6, 0)
    w20 = Window.partitionBy("ticker").orderBy("data").rowsBetween(-19, 0)
    w50 = Window.partitionBy("ticker").orderBy("data").rowsBetween(-49, 0)

    df_silver = (
        df_spark
        .withColumn("retorno_diario_pct",
            F.round(
                (F.col("fechamento") - F.lag("fechamento", 1).over(w))
                / F.lag("fechamento", 1).over(w) * 100, 4))
        .withColumn("retorno_log",
            F.round(F.log(F.col("fechamento") / F.lag("fechamento", 1).over(w)), 6))
        .withColumn("sma_7",  F.round(F.avg("fechamento").over(w7),  2))
        .withColumn("sma_20", F.round(F.avg("fechamento").over(w20), 2))
        .withColumn("sma_50", F.round(F.avg("fechamento").over(w50), 2))
        .withColumn("volatilidade_20d",
            F.round(F.stddev("retorno_log").over(w20), 6))
        .withColumn("amplitude_pct",
            F.round((F.col("alta") - F.col("baixa")) / F.col("fechamento") * 100, 4))
        .withColumn("acima_sma20",
            F.when(F.col("fechamento") > F.col("sma_20"), True).otherwise(False))
        .withColumn("processado_em", F.current_timestamp())
        .withColumn("versao_pipeline", F.lit("1.0.0"))
    )

    (
        df_silver
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("ticker")
        .save(SILVER_B3.replace("\\", "/"))
    )

    n = df_silver.count()
    logger.info(f"Silver B3 gravado: {n:,} registros em {SILVER_B3}")
    return {"status": "ok", "registros": n, "path": SILVER_B3}


# ── Silver → Gold ──────────────────────────────────────────────────────────────

def transform_silver_to_gold(spark) -> dict:
    """Cria camada Gold: métricas consolidadas de mercado + macro."""
    import pandas as pd
    from pyspark.sql import functions as F, Window

    logger.info("Silver → Gold")
    os.makedirs(GOLD_PATH, exist_ok=True)

    resultados = {}

    # Gold B3: métricas por ticker
    try:
        df_b3 = spark.read.format("delta").load(SILVER_B3.replace("\\", "/"))
        w_ticker = Window.partitionBy("ticker").orderBy("data")

        df_gold_b3 = (
            df_b3
            .withColumn("retorno_acumulado_pct",
                F.round(
                    (F.col("fechamento") / F.first("fechamento").over(
                        Window.partitionBy("ticker").orderBy("data")
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedPreceding)
                    ) - 1) * 100, 4))
            .withColumn("max_52s", F.max("alta").over(
                Window.partitionBy("ticker").orderBy("data").rowsBetween(-251, 0)))
            .withColumn("min_52s", F.min("baixa").over(
                Window.partitionBy("ticker").orderBy("data").rowsBetween(-251, 0)))
            .withColumn("distancia_max_52s_pct",
                F.round((F.col("fechamento") / F.col("max_52s") - 1) * 100, 2))
        )

        gold_b3_path = os.path.join(GOLD_PATH, "b3_metricas").replace("\\", "/")
        (
            df_gold_b3.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("ticker")
            .save(gold_b3_path)
        )
        n_b3 = df_gold_b3.count()
        resultados["gold_b3"] = {"status": "ok", "registros": n_b3, "path": gold_b3_path}
        logger.info(f"Gold B3: {n_b3:,} registros")
    except Exception as e:
        logger.warning(f"Gold B3 falhou: {e}")
        resultados["gold_b3"] = {"status": "skip", "erro": str(e)}

    # Gold BCB: pivot das séries macro
    try:
        df_bcb = spark.read.format("delta").load(SILVER_BCB.replace("\\", "/"))

        # Pivot: uma coluna por série
        df_pivot = df_bcb.groupBy("data").pivot("serie_name").agg(F.first("valor"))

        # Forward-fill do PIB (se coluna existir)
        if "pib_variacao" in df_pivot.columns:
            w_ffill = Window.orderBy("data").rowsBetween(Window.unboundedPreceding, 0)
            df_pivot = df_pivot.withColumn(
                "pib_variacao",
                F.last("pib_variacao", ignorenulls=True).over(w_ffill)
            )

        gold_bcb_path = os.path.join(GOLD_PATH, "bcb_macro_pivot").replace("\\", "/")
        (
            df_pivot.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(gold_bcb_path)
        )
        n_bcb = df_pivot.count()
        resultados["gold_bcb"] = {"status": "ok", "registros": n_bcb, "path": gold_bcb_path}
        logger.info(f"Gold BCB pivot: {n_bcb:,} registros")
    except Exception as e:
        logger.warning(f"Gold BCB falhou: {e}")
        resultados["gold_bcb"] = {"status": "skip", "erro": str(e)}

    return resultados


# ── Ponto de entrada ───────────────────────────────────────────────────────────

def run(start_date: str = "2015-01-01", end_date: str = "2026-03-22") -> dict:
    logger.info("=" * 60)
    logger.info("INICIANDO ETL TRANSFORMER")
    logger.info("=" * 60)

    spark = _get_spark()
    spark.sparkContext.setLogLevel("WARN")

    total_registros = 0
    etapas = {}

    # Bronze → Silver BCB
    try:
        res_bcb = transform_bcb_bronze_to_silver(spark)
        etapas["silver_bcb"] = res_bcb
        total_registros += res_bcb.get("registros", 0)
    except Exception as e:
        logger.error(f"Silver BCB falhou: {e}")
        etapas["silver_bcb"] = {"status": "erro", "erro": str(e)}

    # Bronze → Silver B3
    try:
        res_b3 = transform_b3_bronze_to_silver(spark)
        etapas["silver_b3"] = res_b3
        total_registros += res_b3.get("registros", 0)
    except Exception as e:
        logger.error(f"Silver B3 falhou: {e}")
        etapas["silver_b3"] = {"status": "erro", "erro": str(e)}

    # Silver → Gold
    try:
        res_gold = transform_silver_to_gold(spark)
        etapas["gold"] = res_gold
        for v in res_gold.values():
            total_registros += v.get("registros", 0)
    except Exception as e:
        logger.error(f"Gold falhou: {e}")
        etapas["gold"] = {"status": "erro", "erro": str(e)}

    logger.info("=" * 60)
    logger.info(f"ETL CONCLUIDO — total registros processados: {total_registros:,}")
    logger.info("=" * 60)

    return {
        "agent":           "etl-transformer",
        "status":          "ok",
        "total_registros": total_registros,
        "etapas":          etapas,
        "silver_bcb_path": SILVER_BCB,
        "silver_b3_path":  SILVER_B3,
        "gold_path":       GOLD_PATH,
        "executado_em":    datetime.now().isoformat(),
    }


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2015-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-22"
    resultado = run(start, end)
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
