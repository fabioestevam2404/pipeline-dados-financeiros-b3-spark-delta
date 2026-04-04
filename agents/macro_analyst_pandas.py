"""
Macro Analyst — versão Pandas pura (sem PySpark).
Lê Parquet Silver ou fallback Bronze JSON.
"""

import os, sys, json, logging, warnings
import numpy as np
import pandas as pd
from datetime import datetime

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("macro-analyst")

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_BCB  = os.path.join(BASE_DIR, "data", "silver", "bcb")
SILVER_B3   = os.path.join(BASE_DIR, "data", "silver", "b3")
BRONZE_BCB  = os.path.join(BASE_DIR, "data", "bronze", "bcb")
BRONZE_B3   = os.path.join(BASE_DIR, "data", "bronze", "b3")
REPORTS_DIR = os.path.join(BASE_DIR, "docs", "reports")


def _load_bcb() -> pd.DataFrame:
    """Carrega BCB: Parquet Silver, fallback Bronze JSON."""
    parquet_path = os.path.join(SILVER_BCB, "bcb_silver.parquet")
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
        df["data"] = pd.to_datetime(df["data"])
        logger.info(f"BCB Silver Parquet: {len(df):,} linhas")
        return df

    # Fallback Bronze JSON
    registros = []
    for fname in os.listdir(BRONZE_BCB):
        if not fname.endswith(".json"): continue
        with open(os.path.join(BRONZE_BCB, fname), encoding="utf-8") as f:
            payload = json.load(f)
        serie_name = payload.get("serie_name", "")
        for row in payload.get("data", []):
            val_str = str(row.get("valor", "")).replace(",", ".")
            try: val = float(val_str)
            except: val = None
            try: dt = pd.to_datetime(row["data"], format="%d/%m/%Y")
            except: continue
            registros.append({"serie_name": serie_name, "data": dt, "valor": val})
    df = pd.DataFrame(registros)
    df["data"] = pd.to_datetime(df["data"])
    logger.info(f"BCB Bronze JSON: {len(df):,} linhas")
    return df


def _load_b3() -> pd.DataFrame:
    """Carrega B3: Parquet Silver, fallback Bronze JSON."""
    parquet_path = os.path.join(SILVER_B3, "b3_silver.parquet")
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
        df["data"] = pd.to_datetime(df["data"])
        logger.info(f"B3 Silver Parquet: {len(df):,} linhas")
        return df

    registros = []
    for fname in os.listdir(BRONZE_B3):
        if not fname.endswith(".json"): continue
        with open(os.path.join(BRONZE_B3, fname), encoding="utf-8") as f:
            payload = json.load(f)
        ticker = payload.get("ticker", "")
        data_obj = payload.get("data", {})
        ts = data_obj.get("time_series_daily", {}) if isinstance(data_obj, dict) else {}
        for data_str, vals in ts.items():
            try:
                registros.append({"ticker": ticker, "data": pd.to_datetime(data_str),
                    "fechamento": float(vals.get("4. close", 0))})
            except: pass
    df = pd.DataFrame(registros)
    logger.info(f"B3 Bronze JSON: {len(df):,} linhas")
    return df


def calcular_correlacoes(df_bcb: pd.DataFrame, df_b3: pd.DataFrame) -> dict:
    logger.info("Calculando correlacoes...")
    try:
        df_m = df_bcb.pivot_table(index="data", columns="serie_name",
                                   values="valor", aggfunc="mean")
        df_m = df_m.resample("ME").mean()
        macro_cols = [c for c in df_m.columns if not df_m[c].isna().all()]
        df_sub = df_m[macro_cols].dropna(how="all")

        corr = {
            "macro_pearson":  df_sub.corr("pearson").round(4).to_dict(),
            "macro_spearman": df_sub.corr("spearman").round(4).to_dict(),
        }

        # Correlação B3 x macro
        if df_b3 is not None and len(df_b3) > 0:
            df_b3["data"] = pd.to_datetime(df_b3["data"])
            b3_ret = {}
            for ticker, grp in df_b3.groupby("ticker"):
                sub = grp.set_index("data")["fechamento"].sort_index()
                ret = sub.pct_change().resample("ME").mean()
                b3_ret[f"ret_{ticker}"] = ret
            if b3_ret:
                df_b3r = pd.DataFrame(b3_ret)
                merged = df_sub.join(df_b3r, how="inner").dropna(how="all")
                if len(merged) > 5:
                    corr["macro_b3_pearson"]  = merged.corr("pearson").round(4).to_dict()
                    corr["macro_b3_spearman"] = merged.corr("spearman").round(4).to_dict()
        logger.info(f"Correlacoes calculadas: {list(corr.keys())}")
        return corr
    except Exception as e:
        logger.warning(f"Correlacoes falharam: {e}")
        return {}


def calcular_stl(df_bcb: pd.DataFrame) -> dict:
    from statsmodels.tsa.seasonal import STL
    logger.info("Calculando STL...")
    results = {}
    for serie in ["ipca_mensal", "desemprego", "cambio_usd_brl", "selic_meta"]:
        try:
            sub = df_bcb[df_bcb["serie_name"] == serie][["data","valor"]].copy()
            sub["data"] = pd.to_datetime(sub["data"])
            sub = sub.dropna(subset=["valor"]).sort_values("data")
            sub = sub.set_index("data")["valor"].resample("ME").mean().dropna()
            if len(sub) < 24: continue
            res = STL(sub, period=12, robust=True).fit()
            n = min(24, len(sub))
            trend = res.trend[-n:].tolist()
            seasonal = res.seasonal[-n:].tolist()
            resid = res.resid[-n:].tolist()
            slope = float(np.polyfit(range(len(trend)), trend, 1)[0])
            forca = float(1 - np.var(resid) / max(np.var(np.array(seasonal)+np.array(resid)), 1e-10))
            results[serie] = {
                "n_observacoes": int(len(sub)), "tendencia_slope": round(slope, 6),
                "forca_sazonal": round(max(0, forca), 4),
                "ultimo_valor":  round(float(sub.iloc[-1]), 4),
                "media_periodo": round(float(sub.mean()), 4),
                "std_periodo":   round(float(sub.std()), 4),
                "componentes_ultimos_24m": {
                    "datas":    [str(d.date()) for d in sub.index[-n:]],
                    "trend":    [round(v, 4) for v in trend],
                    "seasonal": [round(v, 4) for v in seasonal],
                    "resid":    [round(v, 4) for v in resid],
                },
            }
            logger.info(f"STL {serie}: slope={slope:+.4f} forca={forca:.2%}")
        except Exception as e:
            logger.warning(f"STL {serie}: {e}")
    return results


def run(start_date="2015-01-01", end_date="2026-03-22") -> dict:
    logger.info("=" * 60)
    logger.info("INICIANDO MACRO ANALYST (Pandas)")
    logger.info("=" * 60)
    os.makedirs(REPORTS_DIR, exist_ok=True)

    df_bcb = _load_bcb()
    df_b3  = _load_b3()

    if df_bcb is None or len(df_bcb) == 0:
        return {"agent": "macro-analyst", "status": "erro", "erro": "Sem dados BCB"}

    correlacoes = calcular_correlacoes(df_bcb, df_b3)
    stl_results = calcular_stl(df_bcb)

    # Insights
    insights = []
    for serie, data in stl_results.items():
        slope  = data["tendencia_slope"]
        ultimo = data["ultimo_valor"]
        direcao = "alta" if slope > 0 else "baixa"
        insights.append(f"{serie}: valor={ultimo:.4f}, tendencia de {direcao} (slope={slope:+.6f}/mes).")

    # Correlacao SELIC x IPCA
    try:
        c = correlacoes.get("macro_pearson", {}).get("selic_meta", {}).get("ipca_mensal")
        if c is not None:
            insights.append(f"Correlacao Pearson SELIC x IPCA: {c:.3f}.")
    except Exception: pass

    # Correlacao IPCA x cambio
    try:
        c = correlacoes.get("macro_pearson", {}).get("ipca_mensal", {}).get("cambio_usd_brl")
        if c is not None:
            insights.append(f"Correlacao Pearson IPCA x Cambio: {c:.3f}.")
    except Exception: pass

    # Stats por série
    stats = {}
    for serie in df_bcb["serie_name"].unique():
        sub = df_bcb[df_bcb["serie_name"] == serie]["valor"].dropna()
        if len(sub) > 0:
            stats[serie] = {
                "n": int(len(sub)), "media": round(float(sub.mean()), 4),
                "std": round(float(sub.std()), 4), "min": round(float(sub.min()), 4),
                "max": round(float(sub.max()), 4), "ultimo": round(float(sub.iloc[-1]), 4),
            }

    date_tag = datetime.now().strftime("%Y%m%d")
    resultado = {
        "agent": "macro-analyst", "status": "ok",
        "periodo": {"inicio": start_date, "fim": end_date},
        "gerado_em": datetime.now().isoformat(),
        "total_obs_bcb": int(len(df_bcb)),
        "total_obs_b3":  int(len(df_b3)) if df_b3 is not None else 0,
        "estatisticas_series": stats,
        "correlacoes": correlacoes,
        "stl_decomposicao": stl_results,
        "insights": insights,
    }

    report_path = os.path.join(REPORTS_DIR, f"macro_analise_{date_tag}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    logger.info(f"Relatorio macro salvo: {report_path}")
    logger.info("=" * 60)
    logger.info("MACRO ANALYST CONCLUIDO")
    logger.info(f"   Insights : {len(insights)}")
    logger.info(f"   STL series: {list(stl_results.keys())}")
    logger.info("=" * 60)
    resultado["report_path"] = report_path
    return resultado


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2015-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-22"
    res = run(start, end)
    resumo = {k: v for k, v in res.items() if k not in ("correlacoes", "stl_decomposicao")}
    print(json.dumps(resumo, indent=2, ensure_ascii=False))
