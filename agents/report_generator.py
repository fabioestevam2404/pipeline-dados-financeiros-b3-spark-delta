"""
Agente: report-generator
Consolida todas as análises em relatório executivo.
Consome: stress_*.json, macro_analise_*.json, pd_*_metricas.json
Produz: docs/reports/relatorio_executivo_{YYYYMMDD}.json
"""

import os
import sys
import json
import glob
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("report-generator")

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODELS_DIR  = os.path.join(BASE_DIR, "models")
REPORTS_DIR = os.path.join(BASE_DIR, "docs", "reports")
LOGS_DIR    = os.path.join(BASE_DIR, "logs")


def _load_latest_json(pattern: str) -> Optional[dict]:
    """Carrega o JSON mais recente que corresponde ao padrão glob."""
    files = sorted(glob.glob(pattern))
    if not files:
        return None
    latest = files[-1]
    try:
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Carregado: {latest}")
        return data
    except Exception as e:
        logger.warning(f"Erro ao carregar {latest}: {e}")
        return None


def _resumo_stress(stress: dict) -> dict:
    """Extrai resumo executivo do stress testing."""
    if not stress:
        return {}

    cenarios = stress.get("cenarios", {})
    resumo = {
        "data_referencia":      stress.get("data_referencia"),
        "metodologia":          stress.get("metodologia"),
        "el_ponderado_bi_brl":  stress.get("el_ponderado_bi_brl"),
        "macro_base":           stress.get("macro_contexto_base", {}),
        "cenarios": {},
    }

    for nome, c in cenarios.items():
        resumo["cenarios"][nome] = {
            "nome":                c.get("nome"),
            "el_total_bi":         c.get("expected_loss", {}).get("el_total_bi"),
            "delta_vs_base_bi":    c.get("delta_el_vs_base_bi"),
            "delta_vs_base_pct":   c.get("delta_el_vs_base_pct"),
            "capital_minimo_bi":   c.get("capital_regulatorio_min_bi"),
            "probabilidade":       c.get("probabilidade_ocorrencia"),
        }

    return resumo


def _resumo_macro(macro: dict) -> dict:
    """Extrai resumo executivo da análise macro."""
    if not macro:
        return {}
    return {
        "periodo":          macro.get("periodo"),
        "total_obs_bcb":    macro.get("total_obs_bcb"),
        "insights":         macro.get("insights", []),
        "series_analisadas": list(macro.get("estatisticas_series", {}).keys()),
        "stl_series":       list(macro.get("stl_decomposicao", {}).keys()),
    }


def _resumo_pd(pd_pf: dict, pd_pj: dict) -> dict:
    """Extrai resumo dos modelos PD."""
    resultado = {}
    for nome, pd_data in [("pf", pd_pf), ("pj", pd_pj)]:
        if not pd_data:
            continue
        resultado[f"pd_{nome}"] = {
            "pd_atual_pct":     pd_data.get("pd_atual_pct"),
            "pd_atual_percent": pd_data.get("pd_atual_percent"),
            "melhor_modelo":    pd_data.get("melhor_modelo"),
            "n_obs_treino":     pd_data.get("n_obs_treino"),
            "metricas":         pd_data.get("metricas_modelos", {}),
        }
    return resultado


def _calcular_rating_risco(stress: dict, pd_pf: dict, pd_pj: dict) -> dict:
    """Calcula rating qualitativo de risco sistêmico."""
    score = 50  # base

    # Ajustar por PD
    pd_pf_val = pd_pf.get("pd_atual_pct", 0.04) if pd_pf else 0.04
    pd_pj_val = pd_pj.get("pd_atual_pct", 0.025) if pd_pj else 0.025

    if pd_pf_val > 0.08:    score -= 20
    elif pd_pf_val > 0.06:  score -= 10
    elif pd_pf_val < 0.03:  score += 10

    if pd_pj_val > 0.05:    score -= 15
    elif pd_pj_val < 0.015: score += 10

    # Ajustar por delta EL stress adverso
    if stress:
        delta_adverso = stress.get("cenarios", {}).get("adverso", {}).get(
            "delta_el_vs_base_pct", 0)
        if delta_adverso and delta_adverso > 100:   score -= 20
        elif delta_adverso and delta_adverso > 50:  score -= 10

    score = max(0, min(100, score))

    if score >= 75:   rating = "BAIXO"
    elif score >= 55: rating = "MODERADO"
    elif score >= 35: rating = "ELEVADO"
    else:             rating = "CRITICO"

    return {
        "score_risco":   score,
        "rating":        rating,
        "interpretacao": {
            "BAIXO":    "Sistema financeiro saudável. Indicadores dentro dos limites prudenciais.",
            "MODERADO": "Riscos presentes mas administráveis. Monitoramento ativo recomendado.",
            "ELEVADO":  "Pressões significativas. Medidas macroprudenciais podem ser necessárias.",
            "CRITICO":  "Estresse sistêmico elevado. Intervenção regulatória urgente.",
        }.get(rating, ""),
    }


def run(pipeline_log: dict = None,
        start_date: str = "2015-01-01",
        end_date: str   = "2026-03-22") -> dict:
    """Gera relatório executivo consolidado."""
    logger.info("=" * 60)
    logger.info("INICIANDO REPORT GENERATOR")
    logger.info("=" * 60)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    date_tag = datetime.now().strftime("%Y%m%d")

    # Carregar artefatos
    stress_data  = _load_latest_json(os.path.join(MODELS_DIR,  "stress_*.json"))
    macro_data   = _load_latest_json(os.path.join(REPORTS_DIR, "macro_analise_*.json"))
    pd_pf_data   = _load_latest_json(os.path.join(MODELS_DIR,  "pd_pf_*_metricas.json"))
    pd_pj_data   = _load_latest_json(os.path.join(MODELS_DIR,  "pd_pj_*_metricas.json"))

    # Resumos
    resumo_stress = _resumo_stress(stress_data)
    resumo_macro  = _resumo_macro(macro_data)
    resumo_pd     = _resumo_pd(pd_pf_data, pd_pj_data)
    rating_risco  = _calcular_rating_risco(stress_data, pd_pf_data, pd_pj_data)

    # Inventário de artefatos
    artefatos = {
        "stress_testing":   glob.glob(os.path.join(MODELS_DIR,  "stress_*.json")),
        "macro_analise":    glob.glob(os.path.join(REPORTS_DIR, "macro_analise_*.json")),
        "pd_pf_metricas":   glob.glob(os.path.join(MODELS_DIR,  "pd_pf_*_metricas.json")),
        "pd_pj_metricas":   glob.glob(os.path.join(MODELS_DIR,  "pd_pj_*_metricas.json")),
        "silver_bcb":       [os.path.join(BASE_DIR, "data", "silver", "bcb")]
                            if os.path.isdir(os.path.join(BASE_DIR, "data", "silver", "bcb")) else [],
        "silver_b3":        [os.path.join(BASE_DIR, "data", "silver", "b3")]
                            if os.path.isdir(os.path.join(BASE_DIR, "data", "silver", "b3")) else [],
        "gold":             [os.path.join(BASE_DIR, "data", "gold")]
                            if os.path.isdir(os.path.join(BASE_DIR, "data", "gold")) else [],
    }

    # Etapas do pipeline (do log se disponível)
    etapas_pipeline = {}
    if pipeline_log:
        etapas_pipeline = pipeline_log.get("etapas", {})

    # Construir relatório
    relatorio = {
        "titulo":            "Relatorio Executivo — Pipeline Financeiro Brasileiro",
        "versao":            "1.0.0",
        "gerado_em":         datetime.now().isoformat(),
        "periodo_analise":   {"inicio": start_date, "fim": end_date},
        "rating_risco_sistemico": rating_risco,
        "resumo_executivo": {
            "pd_pf_atual":       resumo_pd.get("pd_pf", {}).get("pd_atual_percent"),
            "pd_pj_atual":       resumo_pd.get("pd_pj", {}).get("pd_atual_percent"),
            "el_total_base_bi":  resumo_stress.get("cenarios", {}).get(
                                     "base", {}).get("el_total_bi"),
            "el_ponderado_bi":   resumo_stress.get("el_ponderado_bi_brl"),
            "insights_macro":    resumo_macro.get("insights", []),
            "macro_atual":       resumo_stress.get("macro_base", {}),
        },
        "analise_macro":     resumo_macro,
        "modelos_pd":        resumo_pd,
        "stress_testing":    resumo_stress,
        "artefatos_gerados": artefatos,
        "pipeline": {
            "etapas":          etapas_pipeline,
            "log_disponivel":  pipeline_log is not None,
        },
        "notas_metodologicas": [
            "Modelos PD treinados com Gradient Boosting (GBM) e Ridge Regression.",
            "Features: SELIC, IPCA, cambio, PIB, desemprego com lags de 1 e 3 meses.",
            "LGD calibrada por segmento com ajuste macroeconomico (Basel III).",
            "Stress testing com 3 cenarios: Base (60%), Adverso (30%), Severo (10%).",
            "EAD estimado para carteira representativa: R$ 5,0 tri (PF + PJ).",
            "Decomposicao STL com period=12 meses para series mensais.",
        ],
    }

    # Salvar relatório
    report_path = os.path.join(REPORTS_DIR, f"relatorio_executivo_{date_tag}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2)

    logger.info(f"Relatorio executivo salvo: {report_path}")
    logger.info("=" * 60)
    logger.info("REPORT GENERATOR CONCLUIDO")
    logger.info(f"   Rating Risco : {rating_risco['rating']} ({rating_risco['score_risco']}/100)")
    logger.info(f"   Relatorio    : {report_path}")
    logger.info("=" * 60)

    return {
        "agent":        "report-generator",
        "status":       "ok",
        "report_path":  report_path,
        "rating_risco": rating_risco,
        "artefatos":    artefatos,
        "gerado_em":    datetime.now().isoformat(),
    }


if __name__ == "__main__":
    resultado = run()
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
