"""
ETL Transformer — implementação Pandas (fallback local sem PySpark shuffle).
Produz Silver e Gold como Parquet + JSON (lidos pelos agentes macro e credit risk).
Bronze -> Silver -> Gold.
"""

import os
import sys
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("etl-transformer-pandas")

BASE_DIR   = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_BCB = os.path.join(BASE_DIR, "data", "bronze", "bcb")
BRONZE_B3  = os.path.join(BASE_DIR, "data", "bronze", "b3")
SILVER_BCB = os.path.join(BASE_DIR, "data", "silver", "bcb")
SILVER_B3  = os.path.join(BASE_DIR, "data", "silver", "b3")
GOLD_PATH  = os.path.join(BASE_DIR, "data", "gold")


# ── Bronze → Silver BCB ────────────────────────────────────────────────────────

def transform_bcb_bronze_to_silver() -> dict:
    """Lê Bronze BCB JSONs → Silver BCB (Parquet + JSON longo)."""
    os.makedirs(SILVER_BCB, exist_ok=True)
    logger.info("Bronze → Silver BCB (Pandas)")

    bcb_files = [f for f in os.listdir(BRONZE_BCB) if f.endswith(".json")]
    if not bcb_files:
        return {"status": "skip", "registros": 0}

    registros = []
    for fname in sorted(bcb_files):
        fpath = os.path.join(BRONZE_BCB, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            payload = json.load(f)
        serie_name = payload.get("serie_name", "")
        serie_code = str(payload.get("serie_code", ""))
        for row in payload.get("data", []):
            val_str = str(row.get("valor", "")).replace(",", ".")
            try:
                val = float(val_str)
            except (ValueError, TypeError):
                val = None
            try:
                dt = pd.to_datetime(row.get("data", ""), format="%d/%m/%Y")
            except Exception:
                continue
            registros.append({
                "serie_name": serie_name,
                "serie_code": serie_code,
                "data":       dt,
                "valor":      val,
            })

    if not registros:
        return {"status": "vazio", "registros": 0}

    df = pd.DataFrame(registros)
    df["data"] = pd.to_datetime(df["data"])
    df = df.dropna(subset=["data"]).sort_values(["serie_name", "data"]).reset_index(drop=True)
    df["processado_em"] = datetime.now().isoformat()

    # Forward-fill PIB trimestral
    pib_mask = df["serie_name"] == "pib_variacao"
    if pib_mask.any():
        df_pib = df[pib_mask].copy()
        df_pib = df_pib.sort_values("data")
        df_pib["valor"] = df_pib["valor"].ffill()
        df.loc[pib_mask, "valor"] = df_pib["valor"].values

    # Salvar como Parquet (por série) e JSON consolidado
    parquet_path = os.path.join(SILVER_BCB, "bcb_silver.parquet")
    df.to_parquet(parquet_path, index=False)

    json_path = os.path.join(SILVER_BCB, "bcb_silver.json")
    df.to_json(json_path, orient="records", date_format="iso", force_ascii=False, indent=2)

    n = len(df)
    logger.info(f"Silver BCB: {n:,} registros → {parquet_path}")
    return {"status": "ok", "registros": n, "path": SILVER_BCB,
            "parquet": parquet_path, "json": json_path}


# ── Bronze → Silver B3 ─────────────────────────────────────────────────────────

def transform_b3_bronze_to_silver() -> dict:
    """Lê Bronze B3 JSONs → Silver B3 (Parquet + indicadores técnicos)."""
    os.makedirs(SILVER_B3, exist_ok=True)
    logger.info("Bronze → Silver B3 (Pandas)")

    b3_files = [f for f in os.listdir(BRONZE_B3) if f.endswith(".json")]
    if not b3_files:
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
                    "data":         pd.to_datetime(data_str),
                    "abertura":     float(vals.get("1. open",   0)),
                    "alta":         float(vals.get("2. high",   0)),
                    "baixa":        float(vals.get("3. low",    0)),
                    "fechamento":   float(vals.get("4. close",  0)),
                    "volume":       int(float(vals.get("5. volume", 0))),
                    "data_ingestao": pd.to_datetime(ingest),
                })
            except Exception:
                pass

    if not registros:
        return {"status": "vazio", "registros": 0}

    df = pd.DataFrame(registros)
    df = df.sort_values(["ticker", "data"]).reset_index(drop=True)

    # Limpeza
    df = df[df["fechamento"] > 0]
    df = df[df["volume"] > 0]
    df = df[df["alta"] >= df["baixa"]]
    df = df.drop_duplicates(subset=["ticker", "data"])

    # Indicadores técnicos por ticker
    resultados = []
    for ticker, grp in df.groupby("ticker"):
        grp = grp.sort_values("data").reset_index(drop=True)
        grp["retorno_diario_pct"] = grp["fechamento"].pct_change() * 100
        grp["retorno_log"]        = np.log(grp["fechamento"] / grp["fechamento"].shift(1))
        grp["sma_7"]              = grp["fechamento"].rolling(7,  min_periods=1).mean().round(2)
        grp["sma_20"]             = grp["fechamento"].rolling(20, min_periods=1).mean().round(2)
        grp["sma_50"]             = grp["fechamento"].rolling(50, min_periods=1).mean().round(2)
        grp["volatilidade_20d"]   = grp["retorno_log"].rolling(20, min_periods=2).std().round(6)
        grp["amplitude_pct"]      = ((grp["alta"] - grp["baixa"]) / grp["fechamento"] * 100).round(4)
        grp["acima_sma20"]        = grp["fechamento"] > grp["sma_20"]
        grp["processado_em"]      = datetime.now().isoformat()
        grp["versao_pipeline"]    = "1.0.0"
        resultados.append(grp)

    df_silver = pd.concat(resultados, ignore_index=True)

    parquet_path = os.path.join(SILVER_B3, "b3_silver.parquet")
    df_silver.to_parquet(parquet_path, index=False)

    json_path = os.path.join(SILVER_B3, "b3_silver.json")
    df_silver.to_json(json_path, orient="records", date_format="iso",
                      force_ascii=False, indent=2)

    n = len(df_silver)
    logger.info(f"Silver B3: {n:,} registros → {parquet_path}")
    return {"status": "ok", "registros": n, "path": SILVER_B3,
            "parquet": parquet_path, "json": json_path}


# ── Silver → Gold ──────────────────────────────────────────────────────────────

def transform_silver_to_gold() -> dict:
    """Cria camada Gold a partir do Silver."""
    os.makedirs(GOLD_PATH, exist_ok=True)
    logger.info("Silver → Gold (Pandas)")
    resultados = {}

    # Gold B3: métricas por ticker
    try:
        parquet_b3 = os.path.join(SILVER_B3, "b3_silver.parquet")
        if os.path.exists(parquet_b3):
            df_b3 = pd.read_parquet(parquet_b3)
            gold_tickers = []
            for ticker, grp in df_b3.groupby("ticker"):
                grp = grp.sort_values("data").reset_index(drop=True)
                grp["max_52s"] = grp["alta"].rolling(252, min_periods=1).max()
                grp["min_52s"] = grp["baixa"].rolling(252, min_periods=1).min()
                grp["distancia_max_52s_pct"] = (
                    (grp["fechamento"] / grp["max_52s"] - 1) * 100).round(2)
                # Retorno acumulado desde início
                preco_inicial = grp["fechamento"].iloc[0]
                grp["retorno_acumulado_pct"] = (
                    (grp["fechamento"] / preco_inicial - 1) * 100).round(4)
                gold_tickers.append(grp)

            df_gold_b3 = pd.concat(gold_tickers, ignore_index=True)
            gold_b3_path = os.path.join(GOLD_PATH, "b3_metricas.parquet")
            df_gold_b3.to_parquet(gold_b3_path, index=False)
            n = len(df_gold_b3)
            resultados["gold_b3"] = {"status": "ok", "registros": n, "path": gold_b3_path}
            logger.info(f"Gold B3: {n:,} registros")
    except Exception as e:
        logger.warning(f"Gold B3 falhou: {e}")
        resultados["gold_b3"] = {"status": "skip", "erro": str(e)}

    # Gold BCB: pivot macro
    try:
        parquet_bcb = os.path.join(SILVER_BCB, "bcb_silver.parquet")
        if os.path.exists(parquet_bcb):
            df_bcb = pd.read_parquet(parquet_bcb)
            df_bcb["data"] = pd.to_datetime(df_bcb["data"])
            df_pivot = df_bcb.pivot_table(
                index="data", columns="serie_name", values="valor", aggfunc="mean"
            ).reset_index()
            # Forward-fill PIB
            if "pib_variacao" in df_pivot.columns:
                df_pivot["pib_variacao"] = df_pivot["pib_variacao"].ffill()

            gold_bcb_path = os.path.join(GOLD_PATH, "bcb_macro_pivot.parquet")
            df_pivot.to_parquet(gold_bcb_path, index=False)

            gold_bcb_json = os.path.join(GOLD_PATH, "bcb_macro_pivot.json")
            df_pivot.to_json(gold_bcb_json, orient="records",
                             date_format="iso", force_ascii=False, indent=2)

            n = len(df_pivot)
            resultados["gold_bcb"] = {
                "status": "ok", "registros": n,
                "path": gold_bcb_path, "json": gold_bcb_json
            }
            logger.info(f"Gold BCB pivot: {n:,} registros")
    except Exception as e:
        logger.warning(f"Gold BCB falhou: {e}")
        resultados["gold_bcb"] = {"status": "skip", "erro": str(e)}

    return resultados


def run(start_date: str = "2015-01-01", end_date: str = "2026-03-22") -> dict:
    logger.info("=" * 60)
    logger.info("INICIANDO ETL TRANSFORMER (Pandas)")
    logger.info("=" * 60)

    total_registros = 0
    etapas = {}

    for nome, func in [("silver_bcb", transform_bcb_bronze_to_silver),
                        ("silver_b3",  transform_b3_bronze_to_silver)]:
        try:
            res = func()
            etapas[nome] = res
            total_registros += res.get("registros", 0)
        except Exception as e:
            logger.error(f"{nome} falhou: {e}")
            etapas[nome] = {"status": "erro", "erro": str(e)}

    try:
        res_gold = transform_silver_to_gold()
        etapas["gold"] = res_gold
        for v in res_gold.values():
            total_registros += v.get("registros", 0)
    except Exception as e:
        logger.error(f"Gold falhou: {e}")
        etapas["gold"] = {"status": "erro", "erro": str(e)}

    logger.info("=" * 60)
    logger.info(f"ETL CONCLUIDO — {total_registros:,} registros processados")
    logger.info("=" * 60)

    return {
        "agent":           "etl-transformer",
        "engine":          "pandas",
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
