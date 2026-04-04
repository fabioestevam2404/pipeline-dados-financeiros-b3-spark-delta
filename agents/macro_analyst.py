"""
Agente: macro-analyst
Análise macroeconômica: correlações Pearson/Spearman + decomposição STL.
Consome dados Silver BCB e Silver B3.
Produz: docs/reports/macro_analise_{YYYYMMDD}.json
"""

import os
import sys
import json
import logging
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("macro-analyst")

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_BCB  = os.path.join(BASE_DIR, "data", "silver", "bcb")
SILVER_B3   = os.path.join(BASE_DIR, "data", "silver", "b3")
GOLD_PATH   = os.path.join(BASE_DIR, "data", "gold")
REPORTS_DIR = os.path.join(BASE_DIR, "docs", "reports")


def _load_bcb_silver_pandas() -> Optional[pd.DataFrame]:
    """Carrega Silver BCB via Delta ou fallback por JSONs Bronze."""
    # Tentar via Delta Lake
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        if os.path.isdir(SILVER_BCB):
            spark = configure_spark_with_delta_pip(
                SparkSession.builder.appName("macro_analyst_bcb")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.driver.memory", "2g")
            ).getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")

            df = spark.read.format("delta").load(SILVER_BCB.replace("\\", "/"))
            pdf = df.toPandas()
            logger.info(f"BCB Silver carregado via Delta: {len(pdf):,} linhas")
            return pdf
    except Exception as e:
        logger.warning(f"Delta BCB falhou, tentando Bronze JSON: {e}")

    # Fallback: ler Bronze JSONs
    bronze_bcb = os.path.join(BASE_DIR, "data", "bronze", "bcb")
    if not os.path.isdir(bronze_bcb):
        return None

    registros = []
    for fname in os.listdir(bronze_bcb):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(bronze_bcb, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            payload = json.load(f)
        serie_name = payload.get("serie_name", "")
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
            registros.append({"serie_name": serie_name, "data": dt, "valor": val})

    if not registros:
        return None

    df = pd.DataFrame(registros)
    df["data"] = pd.to_datetime(df["data"])
    logger.info(f"BCB carregado via Bronze JSON: {len(df):,} linhas")
    return df


def _load_b3_silver_pandas() -> Optional[pd.DataFrame]:
    """Carrega Silver B3 via Delta ou fallback por Bronze JSONs."""
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        if os.path.isdir(SILVER_B3):
            spark = configure_spark_with_delta_pip(
                SparkSession.builder.appName("macro_analyst_b3")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.driver.memory", "2g")
            ).getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")

            df = spark.read.format("delta").load(SILVER_B3.replace("\\", "/"))
            pdf = df.toPandas()
            logger.info(f"B3 Silver carregado via Delta: {len(pdf):,} linhas")
            return pdf
    except Exception as e:
        logger.warning(f"Delta B3 falhou, tentando Bronze JSON: {e}")

    # Fallback: Bronze JSONs
    bronze_b3 = os.path.join(BASE_DIR, "data", "bronze", "b3")
    if not os.path.isdir(bronze_b3):
        return None

    registros = []
    for fname in os.listdir(bronze_b3):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(bronze_b3, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            payload = json.load(f)
        ticker   = payload.get("ticker", "")
        data_obj = payload.get("data", {})
        ts       = data_obj.get("time_series_daily", {}) if isinstance(data_obj, dict) else {}
        for data_str, vals in ts.items():
            try:
                registros.append({
                    "ticker":     ticker,
                    "data":       pd.to_datetime(data_str),
                    "fechamento": float(vals.get("4. close", 0)),
                    "volume":     float(vals.get("5. volume", 0)),
                })
            except Exception:
                pass

    if not registros:
        return None

    df = pd.DataFrame(registros)
    logger.info(f"B3 carregado via Bronze JSON: {len(df):,} linhas")
    return df


def calcular_correlacoes(df_bcb: pd.DataFrame,
                          df_b3: Optional[pd.DataFrame]) -> dict:
    """Calcula matrizes de correlação Pearson e Spearman."""
    logger.info("Calculando correlacoes...")

    # Pivot BCB: data x série
    try:
        df_macro = df_bcb.pivot_table(
            index="data", columns="serie_name", values="valor", aggfunc="mean"
        ).reset_index()
        df_macro["data"] = pd.to_datetime(df_macro["data"])
        df_macro = df_macro.sort_values("data").set_index("data")
        # Resample mensal para uniformizar
        df_macro = df_macro.resample("ME").mean()
    except Exception as e:
        logger.warning(f"Pivot BCB falhou: {e}")
        return {}

    correlacoes = {}

    # Correlações entre séries macro
    macro_cols = [c for c in df_macro.columns if not df_macro[c].isna().all()]
    if len(macro_cols) >= 2:
        df_sub = df_macro[macro_cols].dropna(how="all")
        pearson  = df_sub.corr(method="pearson").round(4).to_dict()
        spearman = df_sub.corr(method="spearman").round(4).to_dict()
        correlacoes["macro_pearson"]  = pearson
        correlacoes["macro_spearman"] = spearman
        logger.info(f"Correlacoes macro: {len(macro_cols)} series")

    # Correlações B3 com macro (se disponível)
    if df_b3 is not None and len(df_b3) > 0:
        try:
            df_b3["data"] = pd.to_datetime(df_b3["data"])
            tickers = df_b3["ticker"].unique()[:5]  # máx 5

            b3_retornos = {}
            for ticker in tickers:
                sub = df_b3[df_b3["ticker"] == ticker][["data", "fechamento"]].copy()
                sub = sub.sort_values("data").set_index("data")
                sub["retorno"] = sub["fechamento"].pct_change()
                sub_m = sub["retorno"].resample("ME").mean()
                b3_retornos[f"ret_{ticker}"] = sub_m

            if b3_retornos:
                df_b3_ret = pd.DataFrame(b3_retornos)
                df_merged = df_macro.join(df_b3_ret, how="inner").dropna(how="all")
                if len(df_merged) > 10:
                    pearson_b3  = df_merged.corr(method="pearson").round(4).to_dict()
                    spearman_b3 = df_merged.corr(method="spearman").round(4).to_dict()
                    correlacoes["macro_b3_pearson"]  = pearson_b3
                    correlacoes["macro_b3_spearman"] = spearman_b3
                    logger.info("Correlacoes macro x B3 calculadas")
        except Exception as e:
            logger.warning(f"Correlacoes B3 falharam: {e}")

    return correlacoes


def calcular_stl(df_bcb: pd.DataFrame) -> dict:
    """Decomposição STL (Seasonal-Trend-Loess) para séries principais."""
    from statsmodels.tsa.seasonal import STL

    logger.info("Calculando decomposicao STL...")
    stl_results = {}

    series_para_stl = ["selic_meta", "ipca_mensal", "desemprego", "cambio_usd_brl"]

    for serie_name in series_para_stl:
        try:
            sub = df_bcb[df_bcb["serie_name"] == serie_name][["data", "valor"]].copy()
            sub["data"] = pd.to_datetime(sub["data"])
            sub = sub.dropna(subset=["valor"]).sort_values("data")

            if len(sub) < 24:
                logger.warning(f"STL {serie_name}: dados insuficientes ({len(sub)} obs).")
                continue

            # Resample mensal
            sub = sub.set_index("data")["valor"].resample("ME").mean().dropna()

            if len(sub) < 24:
                continue

            stl = STL(sub, period=12, robust=True)
            res = stl.fit()

            # Extrair componentes (últimos 24 meses)
            n = min(24, len(sub))
            trend   = res.trend[-n:].tolist()
            seasonal = res.seasonal[-n:].tolist()
            resid    = res.resid[-n:].tolist()
            datas    = [str(d.date()) for d in sub.index[-n:]]

            # Estatísticas
            tendencia_slope = float(np.polyfit(range(len(trend)), trend, 1)[0])
            forca_sazonal   = float(1 - np.var(resid) / np.var(np.array(seasonal) + np.array(resid)))

            stl_results[serie_name] = {
                "n_observacoes":   len(sub),
                "tendencia_slope": round(tendencia_slope, 6),
                "forca_sazonal":   round(max(0.0, forca_sazonal), 4),
                "ultimo_valor":    round(float(sub.iloc[-1]), 4),
                "media_periodo":   round(float(sub.mean()), 4),
                "std_periodo":     round(float(sub.std()), 4),
                "componentes_ultimos_24m": {
                    "datas":    datas,
                    "trend":    [round(v, 4) for v in trend],
                    "seasonal": [round(v, 4) for v in seasonal],
                    "resid":    [round(v, 4) for v in resid],
                },
            }
            logger.info(f"STL {serie_name}: tendencia={tendencia_slope:+.4f} "
                        f"forca_sazonal={forca_sazonal:.2%}")

        except Exception as e:
            logger.warning(f"STL {serie_name} falhou: {e}")

    return stl_results


def gerar_insights(correlacoes: dict, stl: dict, df_bcb: pd.DataFrame) -> list:
    """Gera insights textuais baseados nas análises."""
    insights = []

    # Insight SELIC
    if "selic_meta" in stl:
        slope = stl["selic_meta"]["tendencia_slope"]
        ultimo = stl["selic_meta"]["ultimo_valor"]
        direcao = "alta" if slope > 0 else "baixa"
        insights.append(
            f"SELIC: taxa atual de {ultimo:.2f}% a.a. com tendência de "
            f"{direcao} (slope={slope:+.4f}/mes)."
        )

    # Insight IPCA
    if "ipca_mensal" in stl:
        media = stl["ipca_mensal"]["media_periodo"]
        ultimo = stl["ipca_mensal"]["ultimo_valor"]
        insights.append(
            f"IPCA: variacao mensal recente de {ultimo:.2f}% "
            f"(media historica: {media:.2f}%/mes). "
            f"Acumulado anualizado estimado: {((1 + ultimo/100)**12 - 1)*100:.1f}%."
        )

    # Insight Desemprego
    if "desemprego" in stl:
        ultimo = stl["desemprego"]["ultimo_valor"]
        slope  = stl["desemprego"]["tendencia_slope"]
        direcao = "reducao" if slope < 0 else "aumento"
        insights.append(
            f"Desemprego: {ultimo:.1f}% com tendencia de {direcao}."
        )

    # Insight Cambio
    if "cambio_usd_brl" in stl:
        ultimo = stl["cambio_usd_brl"]["ultimo_valor"]
        std    = stl["cambio_usd_brl"]["std_periodo"]
        insights.append(
            f"Cambio USD/BRL: {ultimo:.2f} (volatilidade historica: {std:.2f})."
        )

    # Insight correlação SELIC-IPCA
    try:
        corr_selic_ipca = correlacoes.get("macro_pearson", {}).get(
            "selic_meta", {}).get("ipca_mensal", None)
        if corr_selic_ipca is not None:
            insights.append(
                f"Correlacao Pearson SELIC x IPCA: {corr_selic_ipca:.3f} "
                f"({'positiva' if corr_selic_ipca > 0 else 'negativa'})."
            )
    except Exception:
        pass

    return insights


def run(start_date: str = "2015-01-01", end_date: str = "2026-03-22") -> dict:
    """Executa análise macroeconômica completa."""
    logger.info("=" * 60)
    logger.info("INICIANDO MACRO ANALYST")
    logger.info(f"   Periodo : {start_date} a {end_date}")
    logger.info("=" * 60)

    os.makedirs(REPORTS_DIR, exist_ok=True)

    # Carregar dados
    df_bcb = _load_bcb_silver_pandas()
    df_b3  = _load_b3_silver_pandas()

    if df_bcb is None or len(df_bcb) == 0:
        logger.error("Sem dados BCB disponíveis.")
        return {"agent": "macro-analyst", "status": "erro",
                "erro": "Sem dados BCB"}

    logger.info(f"Dados BCB: {len(df_bcb):,} registros | "
                f"Series: {df_bcb['serie_name'].unique().tolist()}")
    if df_b3 is not None:
        logger.info(f"Dados B3: {len(df_b3):,} registros | "
                    f"Tickers: {df_b3['ticker'].unique().tolist() if 'ticker' in df_b3.columns else 'N/A'}")

    # Calcular análises
    correlacoes = calcular_correlacoes(df_bcb, df_b3)
    stl_results = calcular_stl(df_bcb)
    insights    = gerar_insights(correlacoes, stl_results, df_bcb)

    # Estatísticas descritivas por série
    stats_series = {}
    for serie in df_bcb["serie_name"].unique():
        sub = df_bcb[df_bcb["serie_name"] == serie]["valor"].dropna()
        if len(sub) > 0:
            stats_series[serie] = {
                "n":      int(len(sub)),
                "media":  round(float(sub.mean()), 4),
                "std":    round(float(sub.std()), 4),
                "min":    round(float(sub.min()), 4),
                "max":    round(float(sub.max()), 4),
                "ultimo": round(float(sub.iloc[-1]), 4),
            }

    # Montar resultado
    date_tag = datetime.now().strftime("%Y%m%d")
    resultado = {
        "agent":           "macro-analyst",
        "status":          "ok",
        "periodo":         {"inicio": start_date, "fim": end_date},
        "gerado_em":       datetime.now().isoformat(),
        "total_obs_bcb":   int(len(df_bcb)),
        "total_obs_b3":    int(len(df_b3)) if df_b3 is not None else 0,
        "estatisticas_series": stats_series,
        "correlacoes":     correlacoes,
        "stl_decomposicao": stl_results,
        "insights":        insights,
    }

    # Salvar relatório
    report_path = os.path.join(REPORTS_DIR, f"macro_analise_{date_tag}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    logger.info(f"Relatorio macro salvo em: {report_path}")
    logger.info("=" * 60)
    logger.info("MACRO ANALYST CONCLUIDO")
    logger.info(f"   Insights gerados: {len(insights)}")
    logger.info(f"   Series STL: {list(stl_results.keys())}")
    logger.info("=" * 60)

    resultado["report_path"] = report_path
    return resultado


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2015-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-22"
    resultado = run(start, end)
    # Não serializar conteúdo completo de correlações no stdout (muito grande)
    resumo = {k: v for k, v in resultado.items()
              if k not in ("correlacoes", "stl_decomposicao")}
    print(json.dumps(resumo, indent=2, ensure_ascii=False))
