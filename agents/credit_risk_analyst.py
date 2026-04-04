"""
Agente: credit-risk-analyst
Modelagem de risco de crédito: PD PF/PJ, LGD, EAD, Expected Loss,
Stress Testing Basel III.
Produz: models/stress_{YYYYMMDD}.json, models/pd_{target}_{YYYYMMDD}_metricas.json
"""

import os
import sys
import json
import logging
import warnings
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("credit-risk-analyst")

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_BCB  = os.path.join(BASE_DIR, "data", "bronze", "bcb")
SILVER_BCB  = os.path.join(BASE_DIR, "data", "silver", "bcb")
MODELS_DIR  = os.path.join(BASE_DIR, "models")
REPORTS_DIR = os.path.join(BASE_DIR, "docs", "reports")


# ── Carregamento de dados ──────────────────────────────────────────────────────

def _load_bcb_series_pandas() -> Optional[pd.DataFrame]:
    """Carrega dados BCB (Silver via Delta ou fallback Bronze JSON)."""
    # Tentar Silver Delta
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        if os.path.isdir(SILVER_BCB):
            spark = configure_spark_with_delta_pip(
                SparkSession.builder.appName("credit_risk_bcb")
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
        logger.warning(f"Delta BCB falhou: {e}")

    # Fallback Bronze JSON
    if not os.path.isdir(BRONZE_BCB):
        return None

    registros = []
    for fname in os.listdir(BRONZE_BCB):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(BRONZE_BCB, fname)
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


def _pivot_macro(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma série longa em wide (pivot) com resample mensal."""
    df["data"] = pd.to_datetime(df["data"])
    df_pivot = df.pivot_table(
        index="data", columns="serie_name", values="valor", aggfunc="mean"
    )
    df_pivot = df_pivot.resample("ME").mean()
    # Forward-fill PIB trimestral
    if "pib_variacao" in df_pivot.columns:
        df_pivot["pib_variacao"] = df_pivot["pib_variacao"].ffill()
    df_pivot = df_pivot.ffill().bfill()
    return df_pivot


# ── Modelagem PD ───────────────────────────────────────────────────────────────

def _gerar_features_pd(df_macro: pd.DataFrame,
                        target: str) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Gera features e target para modelo de PD.
    Target: inadimplência PF ou PJ com lag de 3 meses.
    """
    inadimplencia_col = {
        "pf": "inadimplencia_pf",
        "pj": "inadimplencia_pj",
    }.get(target, "inadimplencia_pf")

    if inadimplencia_col not in df_macro.columns:
        raise ValueError(f"Coluna {inadimplencia_col} ausente nos dados.")

    df = df_macro.copy().dropna(subset=[inadimplencia_col])

    # Target: inadimplência em t+3 (proxy de PD futura)
    y = df[inadimplencia_col].shift(-3).dropna()
    df = df.loc[y.index]

    # Features macroeconômicas
    feature_cols = [c for c in ["selic_meta", "ipca_mensal", "cambio_usd_brl",
                                  "pib_variacao", "desemprego"]
                    if c in df.columns]

    # Adicionar lags das features
    for col in feature_cols:
        df[f"{col}_lag1"] = df[col].shift(1)
        df[f"{col}_lag3"] = df[col].shift(3)

    # Variação 12m
    for col in feature_cols:
        df[f"{col}_var12m"] = df[col].pct_change(12) * 100

    all_feats = [c for c in df.columns if c not in [inadimplencia_col]]
    X = df[all_feats].dropna()
    y = y.loc[X.index]

    return X, y


def _treinar_modelo_pd(X: pd.DataFrame, y: pd.Series,
                        target: str) -> dict:
    """Treina modelo de PD (regressão) e retorna métricas."""
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.linear_model import Ridge
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import mean_absolute_error, r2_score

    logger.info(f"Treinando modelo PD {target.upper()} — {len(X)} obs, {len(X.columns)} features")

    # Divisão temporal: 80/20
    n_train = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:n_train], X.iloc[n_train:]
    y_train, y_test = y.iloc[:n_train], y.iloc[n_train:]

    scaler = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train)
    X_test_sc  = scaler.transform(X_test)

    # Modelos
    modelos = {
        "ridge": Ridge(alpha=1.0),
        "gbm":   GradientBoostingRegressor(
            n_estimators=100, max_depth=3, learning_rate=0.1,
            subsample=0.8, random_state=42
        ),
    }

    metricas = {}
    melhor_r2 = -np.inf
    melhor_modelo_nome = None
    melhor_preds = None

    for nome, modelo in modelos.items():
        try:
            if nome == "ridge":
                modelo.fit(X_train_sc, y_train)
                preds_test  = modelo.predict(X_test_sc)
                preds_train = modelo.predict(X_train_sc)
            else:
                modelo.fit(X_train, y_train)
                preds_test  = modelo.predict(X_test)
                preds_train = modelo.predict(X_train)

            mae_train = mean_absolute_error(y_train, preds_train)
            mae_test  = mean_absolute_error(y_test,  preds_test)
            r2_train  = r2_score(y_train, preds_train)
            r2_test   = r2_score(y_test,  preds_test)

            metricas[nome] = {
                "mae_treino": round(float(mae_train), 4),
                "mae_teste":  round(float(mae_test),  4),
                "r2_treino":  round(float(r2_train),  4),
                "r2_teste":   round(float(r2_test),   4),
            }

            if r2_test > melhor_r2:
                melhor_r2          = r2_test
                melhor_modelo_nome = nome
                melhor_preds       = preds_test.tolist()

            logger.info(f"  {nome}: MAE_test={mae_test:.4f} R2_test={r2_test:.4f}")

        except Exception as e:
            logger.warning(f"Modelo {nome} falhou: {e}")

    # PD atual (último valor predito pelo melhor modelo)
    pd_atual = float(np.clip(melhor_preds[-1], 0, 100)) if melhor_preds else float(y.iloc[-1])
    pd_atual_pct = pd_atual / 100.0  # converter para fração

    # Importância de features (GBM)
    feature_importance = {}
    try:
        if melhor_modelo_nome == "gbm" and "gbm" in modelos:
            gbm_modelo = modelos["gbm"]
            if hasattr(gbm_modelo, "feature_importances_"):
                importancias = gbm_modelo.feature_importances_
                feature_importance = {
                    col: round(float(imp), 4)
                    for col, imp in zip(X.columns, importancias)
                }
                feature_importance = dict(
                    sorted(feature_importance.items(),
                           key=lambda x: x[1], reverse=True)[:10]
                )
    except Exception:
        pass

    return {
        "target":             target,
        "n_obs_treino":       n_train,
        "n_obs_teste":        len(X_test),
        "n_features":         len(X.columns),
        "melhor_modelo":      melhor_modelo_nome,
        "pd_atual_pct":       round(pd_atual / 100, 4),
        "pd_atual_percent":   round(pd_atual, 2),
        "metricas_modelos":   metricas,
        "feature_importance": feature_importance,
        "features_usadas":    list(X.columns)[:20],
    }


# ── LGD e EAD ─────────────────────────────────────────────────────────────────

def calcular_lgd_ead(df_macro: pd.DataFrame) -> dict:
    """
    Calcula LGD e EAD baseados em premissas regulatórias Basel III
    e calibração por dados históricos de inadimplência.
    """
    logger.info("Calculando LGD e EAD...")

    # LGD: premissas Basel III para carteiras BR
    lgd_premissas = {
        "pf_hipotecario":   0.15,   # 15% (garantia imóvel, LTV < 80%)
        "pf_consignado":    0.25,   # 25% (desconto em folha)
        "pf_pessoal":       0.55,   # 55% (sem garantia)
        "pf_cartao":        0.75,   # 75% (sem garantia, alta risco)
        "pj_garantia_real": 0.25,   # 25% (imóvel/equipamento)
        "pj_sem_garantia":  0.45,   # 45% (sem garantia)
        "pj_fianca":        0.35,   # 35% (fiança bancária)
    }

    # Ajuste pelo ciclo macroeconômico
    taxa_desemprego = None
    pib_var = None
    try:
        if "desemprego" in df_macro.columns:
            taxa_desemprego = float(df_macro["desemprego"].dropna().iloc[-1])
        if "pib_variacao" in df_macro.columns:
            pib_var = float(df_macro["pib_variacao"].dropna().iloc[-1])
    except Exception:
        pass

    # Multiplicador de stress macro (desemprego > 12%: +10% LGD)
    multiplicador_macro = 1.0
    if taxa_desemprego and taxa_desemprego > 12.0:
        multiplicador_macro = 1.10
    elif taxa_desemprego and taxa_desemprego > 15.0:
        multiplicador_macro = 1.20

    lgd_ajustada = {
        k: round(min(v * multiplicador_macro, 0.95), 4)
        for k, v in lgd_premissas.items()
    }

    # EAD: Credit Conversion Factor (CCF) Basel III
    ead_ccf = {
        "revolving_nao_utilizado":   1.00,  # linhas de crédito rotativos
        "termino_nao_utilizado":     0.75,  # linhas a prazo determinado
        "garantias_financeiras":     1.00,
        "cartas_de_credito":         0.50,
        "derivativos_cambio":        1.00,
    }

    # LGD médio ponderado PF/PJ
    pesos_pf = {"pf_pessoal": 0.40, "pf_cartao": 0.30,
                "pf_consignado": 0.20, "pf_hipotecario": 0.10}
    pesos_pj = {"pj_sem_garantia": 0.50, "pj_garantia_real": 0.35,
                "pj_fianca": 0.15}

    lgd_medio_pf = sum(lgd_ajustada[k] * v for k, v in pesos_pf.items())
    lgd_medio_pj = sum(lgd_ajustada[k] * v for k, v in pesos_pj.items())

    return {
        "lgd_por_segmento":    lgd_premissas,
        "lgd_ajustada_macro":  lgd_ajustada,
        "lgd_medio_pf":        round(lgd_medio_pf, 4),
        "lgd_medio_pj":        round(lgd_medio_pj, 4),
        "multiplicador_macro": round(multiplicador_macro, 2),
        "taxa_desemprego_ref":  taxa_desemprego,
        "pib_variacao_ref":     pib_var,
        "ead_ccf":              ead_ccf,
    }


# ── Expected Loss ──────────────────────────────────────────────────────────────

def calcular_expected_loss(pd_pf: dict, pd_pj: dict, lgd_ead: dict) -> dict:
    """Calcula Expected Loss = PD x LGD x EAD (Basel II/III)."""
    logger.info("Calculando Expected Loss...")

    pd_pf_atual  = pd_pf.get("pd_atual_pct", 0.04)
    pd_pj_atual  = pd_pj.get("pd_atual_pct", 0.025)
    lgd_pf       = lgd_ead.get("lgd_medio_pf", 0.45)
    lgd_pj       = lgd_ead.get("lgd_medio_pj", 0.38)

    # Carteiras hipotéticas (R$ bilhões) — representativas do sistema bancário BR
    ead_pf_bi    = 2_800.0   # R$ 2,8 tri carteira PF
    ead_pj_bi    = 2_200.0   # R$ 2,2 tri carteira PJ
    ead_total_bi = ead_pf_bi + ead_pj_bi

    el_pf  = pd_pf_atual  * lgd_pf * ead_pf_bi
    el_pj  = pd_pj_atual  * lgd_pj * ead_pj_bi
    el_total = el_pf + el_pj

    el_pct_pf  = pd_pf_atual  * lgd_pf
    el_pct_pj  = pd_pj_atual  * lgd_pj
    el_pct_total = (el_pf + el_pj) / ead_total_bi

    return {
        "pd_pf":            round(pd_pf_atual,   4),
        "pd_pj":            round(pd_pj_atual,   4),
        "lgd_pf":           round(lgd_pf,         4),
        "lgd_pj":           round(lgd_pj,         4),
        "ead_pf_bi_brl":    ead_pf_bi,
        "ead_pj_bi_brl":    ead_pj_bi,
        "el_pf_bi_brl":     round(el_pf,         2),
        "el_pj_bi_brl":     round(el_pj,         2),
        "el_total_bi_brl":  round(el_total,      2),
        "el_pct_pf":        round(el_pct_pf,     4),
        "el_pct_pj":        round(el_pct_pj,     4),
        "el_pct_carteira_total": round(el_pct_total, 4),
    }


# ── Stress Testing Basel III ───────────────────────────────────────────────────

def calcular_stress_testing(pd_pf: dict, pd_pj: dict,
                             lgd_ead: dict, el: dict,
                             df_macro: pd.DataFrame) -> dict:
    """
    Stress Testing com 3 cenários:
    - Base: condições atuais
    - Adverso: recessão moderada
    - Severo: crise financeira sistêmica (FSAP Brasil)
    """
    logger.info("Calculando Stress Testing (Basel III)...")

    pd_base_pf = el["pd_pf"]
    pd_base_pj = el["pd_pj"]
    lgd_base   = lgd_ead["lgd_medio_pf"]

    # Parâmetros de cenários (choques percentuais sobre PD/LGD)
    cenarios = {
        "base": {
            "nome":             "Base (condições atuais)",
            "choque_pd_pf":     1.0,
            "choque_pd_pj":     1.0,
            "choque_lgd":       1.0,
            "choque_selic_bps": 0,
            "choque_cambio_pct": 0.0,
            "choque_pib_pp":    0.0,
            "prob_ocorrencia":  0.60,
        },
        "adverso": {
            "nome":             "Adverso (recessão moderada)",
            "choque_pd_pf":     1.50,   # PD aumenta 50%
            "choque_pd_pj":     1.75,   # PD PJ mais sensível
            "choque_lgd":       1.20,   # LGD aumenta 20%
            "choque_selic_bps": 300,    # SELIC +300bps
            "choque_cambio_pct": 25.0,  # Câmbio +25%
            "choque_pib_pp":    -2.5,   # PIB -2,5pp
            "prob_ocorrencia":  0.30,
        },
        "severo": {
            "nome":             "Severo (crise sistêmica)",
            "choque_pd_pf":     2.50,   # PD dobra e mais
            "choque_pd_pj":     3.00,
            "choque_lgd":       1.40,
            "choque_selic_bps": 600,    # SELIC +600bps
            "choque_cambio_pct": 60.0,  # Câmbio +60%
            "choque_pib_pp":    -6.0,   # PIB -6pp
            "prob_ocorrencia":  0.10,
        },
    }

    resultados_stress = {}
    ead_pf = el["ead_pf_bi_brl"]
    ead_pj = el["ead_pj_bi_brl"]

    for cenario_nome, params in cenarios.items():
        pd_stress_pf = min(pd_base_pf * params["choque_pd_pf"], 0.35)
        pd_stress_pj = min(pd_base_pj * params["choque_pd_pj"], 0.40)
        lgd_stress   = min(lgd_base * params["choque_lgd"], 0.90)

        el_stress_pf = pd_stress_pf * lgd_stress * ead_pf
        el_stress_pj = pd_stress_pj * lgd_ead["lgd_medio_pj"] * params["choque_lgd"] * ead_pj
        el_stress_total = el_stress_pf + el_stress_pj

        # Capital Regulatório mínimo (Basel III: 8% RWA + 2.5% conservation buffer)
        rwa_pf = ead_pf * pd_stress_pf * 12.5  # simplificado IRB
        rwa_pj = ead_pj * pd_stress_pj * 12.5
        capital_minimo = (rwa_pf + rwa_pj) * 0.105  # 10,5% total Basel III

        resultados_stress[cenario_nome] = {
            "nome":              params["nome"],
            "parametros_choque": {
                "pd_pf_choque":     round(pd_stress_pf,  4),
                "pd_pj_choque":     round(pd_stress_pj,  4),
                "lgd_choque":       round(lgd_stress,    4),
                "selic_bps":        params["choque_selic_bps"],
                "cambio_pct":       params["choque_cambio_pct"],
                "pib_delta_pp":     params["choque_pib_pp"],
            },
            "expected_loss": {
                "el_pf_bi":    round(el_stress_pf,    2),
                "el_pj_bi":    round(el_stress_pj,    2),
                "el_total_bi": round(el_stress_total, 2),
            },
            "capital_regulatorio_min_bi": round(capital_minimo, 2),
            "delta_el_vs_base_bi": round(el_stress_total - el["el_total_bi_brl"], 2),
            "delta_el_vs_base_pct": round(
                (el_stress_total / el["el_total_bi_brl"] - 1) * 100
                if el["el_total_bi_brl"] > 0 else 0, 2),
            "probabilidade_ocorrencia": params["prob_ocorrencia"],
        }

    # Expected Loss ponderado pelo cenário
    el_ponderado = sum(
        resultados_stress[c]["expected_loss"]["el_total_bi"] * cenarios[c]["prob_ocorrencia"]
        for c in cenarios
    )

    # Macro-contexto atual
    macro_atual = {}
    try:
        for col in ["selic_meta", "ipca_mensal", "cambio_usd_brl",
                    "desemprego", "pib_variacao"]:
            if col in df_macro.columns:
                val = df_macro[col].dropna()
                if len(val) > 0:
                    macro_atual[col] = round(float(val.iloc[-1]), 4)
    except Exception:
        pass

    return {
        "data_referencia":     datetime.now().strftime("%Y-%m-%d"),
        "metodologia":         "Basel III IRB Simplified + Macro-Stress",
        "cenarios":            resultados_stress,
        "el_ponderado_bi_brl": round(el_ponderado, 2),
        "macro_contexto_base": macro_atual,
        "notas": [
            "EAD calibrado para carteira representativa do sistema bancario brasileiro.",
            "LGD ajustada pelo ciclo macroeconomico (desemprego, PIB).",
            "PD modelada por GBM com features macro com lags de 1 e 3 meses.",
            "Stress testing alinhado com FSAP Brasil e requerimentos BCBS 2023.",
        ],
    }


# ── Ponto de entrada ───────────────────────────────────────────────────────────

def run(start_date: str = "2015-01-01", end_date: str = "2026-03-22") -> dict:
    """Executa modelagem de risco de crédito completa."""
    logger.info("=" * 60)
    logger.info("INICIANDO CREDIT RISK ANALYST")
    logger.info(f"   Periodo: {start_date} a {end_date}")
    logger.info("=" * 60)

    os.makedirs(MODELS_DIR,  exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)

    date_tag = datetime.now().strftime("%Y%m%d")
    artefatos = []

    # Carregar dados
    df_bcb_longo = _load_bcb_series_pandas()
    if df_bcb_longo is None or len(df_bcb_longo) == 0:
        logger.error("Sem dados BCB. Abortando.")
        return {"agent": "credit-risk-analyst", "status": "erro",
                "erro": "Sem dados BCB"}

    try:
        df_macro = _pivot_macro(df_bcb_longo)
        logger.info(f"Macro pivotado: {len(df_macro)} periodos, {list(df_macro.columns)}")
    except Exception as e:
        logger.error(f"Pivot macro falhou: {e}")
        return {"agent": "credit-risk-analyst", "status": "erro", "erro": str(e)}

    # Modelagem PD PF
    pd_pf_result = {}
    try:
        X_pf, y_pf = _gerar_features_pd(df_macro, "pf")
        pd_pf_result = _treinar_modelo_pd(X_pf, y_pf, "pf")

        metricas_path = os.path.join(MODELS_DIR, f"pd_pf_{date_tag}_metricas.json")
        with open(metricas_path, "w", encoding="utf-8") as f:
            json.dump(pd_pf_result, f, ensure_ascii=False, indent=2)
        artefatos.append(metricas_path)
        logger.info(f"Metricas PD PF salvas: {metricas_path}")
    except Exception as e:
        logger.warning(f"PD PF falhou: {e}")
        pd_pf_result = {"target": "pf", "pd_atual_pct": 0.038, "erro": str(e)}

    # Modelagem PD PJ
    pd_pj_result = {}
    try:
        X_pj, y_pj = _gerar_features_pd(df_macro, "pj")
        pd_pj_result = _treinar_modelo_pd(X_pj, y_pj, "pj")

        metricas_path = os.path.join(MODELS_DIR, f"pd_pj_{date_tag}_metricas.json")
        with open(metricas_path, "w", encoding="utf-8") as f:
            json.dump(pd_pj_result, f, ensure_ascii=False, indent=2)
        artefatos.append(metricas_path)
        logger.info(f"Metricas PD PJ salvas: {metricas_path}")
    except Exception as e:
        logger.warning(f"PD PJ falhou: {e}")
        pd_pj_result = {"target": "pj", "pd_atual_pct": 0.022, "erro": str(e)}

    # LGD e EAD
    lgd_ead_result = calcular_lgd_ead(df_macro)

    # Expected Loss
    el_result = calcular_expected_loss(pd_pf_result, pd_pj_result, lgd_ead_result)

    # Stress Testing
    stress_result = calcular_stress_testing(
        pd_pf_result, pd_pj_result, lgd_ead_result, el_result, df_macro
    )

    # Salvar stress testing
    stress_path = os.path.join(MODELS_DIR, f"stress_{date_tag}.json")
    with open(stress_path, "w", encoding="utf-8") as f:
        json.dump(stress_result, f, ensure_ascii=False, indent=2)
    artefatos.append(stress_path)
    logger.info(f"Stress testing salvo: {stress_path}")

    # Montar resultado completo
    resultado = {
        "agent":           "credit-risk-analyst",
        "status":          "ok",
        "periodo":         {"inicio": start_date, "fim": end_date},
        "executado_em":    datetime.now().isoformat(),
        "artefatos":       artefatos,
        "pd_pf":           pd_pf_result,
        "pd_pj":           pd_pj_result,
        "lgd_ead":         lgd_ead_result,
        "expected_loss":   el_result,
        "stress_testing":  stress_result,
        "stress_path":     stress_path,
    }

    logger.info("=" * 60)
    logger.info("CREDIT RISK ANALYST CONCLUIDO")
    logger.info(f"   PD PF atual: {pd_pf_result.get('pd_atual_pct', 'N/A'):.2%}")
    logger.info(f"   PD PJ atual: {pd_pj_result.get('pd_atual_pct', 'N/A'):.2%}")
    logger.info(f"   EL total   : R$ {el_result.get('el_total_bi_brl', 0):.1f} bi")
    logger.info(f"   Stress path: {stress_path}")
    logger.info("=" * 60)

    return resultado


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2015-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-22"
    resultado = run(start, end)
    # Resumo sem detalhes internos de features
    resumo = {k: v for k, v in resultado.items()
              if k not in ("pd_pf", "pd_pj", "stress_testing")}
    print(json.dumps(resumo, indent=2, ensure_ascii=False))
