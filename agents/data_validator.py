"""
Agente: data-validator
Valida qualidade dos dados Bronze (BCB + B3).
Score mínimo requerido: 70/100
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("data-validator")

BASE_DIR    = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_BCB  = os.path.join(BASE_DIR, "data", "bronze", "bcb")
BRONZE_B3   = os.path.join(BASE_DIR, "data", "bronze", "b3")
SCORE_MINIMO = 70


# ── Funções de validação ────────────────────────────────────────────────────────

def validate_bcb_file(filepath: str) -> dict:
    """Valida um arquivo JSON do BCB."""
    checks = {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        return {"status": "erro_leitura", "erro": str(e), "score": 0}

    # Check 1: estrutura mínima
    checks["tem_chave_data"]    = isinstance(payload.get("data"), list)
    checks["tem_serie_name"]    = bool(payload.get("serie_name"))
    checks["tem_serie_code"]    = bool(payload.get("serie_code"))
    checks["tem_ingested_at"]   = bool(payload.get("ingested_at"))

    dados = payload.get("data", [])

    # Check 2: volume de dados
    checks["tem_registros"]     = len(dados) > 0
    checks["volume_suficiente"] = len(dados) >= 12   # ao menos 12 pontos

    # Check 3: estrutura dos registros
    if dados:
        amostra = dados[:10]
        checks["tem_campo_data"]    = all("data" in r for r in amostra)
        checks["tem_campo_valor"]   = all("valor" in r for r in amostra)
        checks["valores_numericos"] = all(
            _is_numeric(r.get("valor")) for r in amostra if r.get("valor") not in (None, "")
        )
        # Check 4: ausência de nulos excessivos
        nulos = sum(1 for r in dados if r.get("valor") in (None, "", "null"))
        pct_nulos = nulos / len(dados) if dados else 1.0
        checks["nulos_aceitaveis"]  = pct_nulos < 0.20   # menos de 20% nulos
    else:
        checks["tem_campo_data"]    = False
        checks["tem_campo_valor"]   = False
        checks["valores_numericos"] = False
        checks["nulos_aceitaveis"]  = False

    score = int(sum(checks.values()) / len(checks) * 100)
    return {
        "status":  "ok" if score >= SCORE_MINIMO else "falha",
        "score":   score,
        "checks":  checks,
        "registros": len(dados),
        "arquivo": os.path.basename(filepath),
    }


def validate_b3_file(filepath: str) -> dict:
    """Valida um arquivo JSON da B3."""
    checks = {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:
        return {"status": "erro_leitura", "erro": str(e), "score": 0}

    # Check 1: estrutura mínima
    checks["tem_ticker"]       = bool(payload.get("ticker"))
    checks["tem_source"]       = bool(payload.get("source"))
    checks["tem_ingested_at"]  = bool(payload.get("ingested_at"))

    data_obj  = payload.get("data", {})
    ts_daily  = data_obj.get("time_series_daily", {}) if isinstance(data_obj, dict) else {}

    # Check 2: volume
    checks["tem_registros"]    = len(ts_daily) > 0
    checks["volume_suficiente"] = len(ts_daily) >= 50

    if ts_daily:
        amostra = list(ts_daily.values())[:5]
        checks["tem_open"]     = all("1. open" in r for r in amostra)
        checks["tem_close"]    = all("4. close" in r for r in amostra)
        checks["tem_volume"]   = all("5. volume" in r for r in amostra)
        # Preços positivos
        checks["precos_positivos"] = all(
            float(r.get("4. close", 0)) > 0 for r in amostra
        )
    else:
        checks["tem_open"]         = False
        checks["tem_close"]        = False
        checks["tem_volume"]       = False
        checks["precos_positivos"] = False

    score = int(sum(checks.values()) / len(checks) * 100)
    return {
        "status":    "ok" if score >= SCORE_MINIMO else "falha",
        "score":     score,
        "checks":    checks,
        "registros": len(ts_daily),
        "arquivo":   os.path.basename(filepath),
    }


def _is_numeric(val) -> bool:
    try:
        float(str(val).replace(",", "."))
        return True
    except (ValueError, TypeError):
        return False


def run(bcb_result: dict = None, b3_result: dict = None) -> dict:
    """Valida todos os arquivos Bronze disponíveis."""
    logger.info("=" * 60)
    logger.info("INICIANDO DATA VALIDATOR")
    logger.info(f"   Bronze BCB : {BRONZE_BCB}")
    logger.info(f"   Bronze B3  : {BRONZE_B3}")
    logger.info(f"   Score min  : {SCORE_MINIMO}")
    logger.info("=" * 60)

    validacoes_bcb = {}
    validacoes_b3  = {}
    todos_scores   = []
    erros_criticos = []

    # ── Validar arquivos BCB ──────────────────────────────────────────────────
    if os.path.isdir(BRONZE_BCB):
        bcb_files = [f for f in os.listdir(BRONZE_BCB) if f.endswith(".json")]
        logger.info(f"Validando {len(bcb_files)} arquivo(s) BCB...")

        for fname in sorted(bcb_files):
            fpath = os.path.join(BRONZE_BCB, fname)
            resultado = validate_bcb_file(fpath)
            validacoes_bcb[fname] = resultado
            todos_scores.append(resultado["score"])

            status_icon = "OK" if resultado["score"] >= SCORE_MINIMO else "FALHA"
            logger.info(f"  [{status_icon}] {fname} — score={resultado['score']} "
                        f"registros={resultado['registros']}")

            if resultado["score"] < SCORE_MINIMO:
                erros_criticos.append(f"BCB/{fname} score={resultado['score']}")
    else:
        logger.warning("Diretório Bronze BCB não encontrado.")

    # ── Validar arquivos B3 ───────────────────────────────────────────────────
    if os.path.isdir(BRONZE_B3):
        b3_files = [f for f in os.listdir(BRONZE_B3) if f.endswith(".json")]
        logger.info(f"Validando {len(b3_files)} arquivo(s) B3...")

        for fname in sorted(b3_files):
            fpath = os.path.join(BRONZE_B3, fname)
            resultado = validate_b3_file(fpath)
            validacoes_b3[fname] = resultado
            todos_scores.append(resultado["score"])

            status_icon = "OK" if resultado["score"] >= SCORE_MINIMO else "FALHA"
            logger.info(f"  [{status_icon}] {fname} — score={resultado['score']} "
                        f"registros={resultado['registros']}")

            if resultado["score"] < SCORE_MINIMO:
                erros_criticos.append(f"B3/{fname} score={resultado['score']}")
    else:
        logger.warning("Diretório Bronze B3 não encontrado.")

    # ── Score global ──────────────────────────────────────────────────────────
    score_global = int(sum(todos_scores) / len(todos_scores)) if todos_scores else 0
    aprovado     = score_global >= SCORE_MINIMO and len(erros_criticos) == 0

    logger.info("=" * 60)
    logger.info(f"SCORE GLOBAL: {score_global}/100")
    logger.info(f"STATUS: {'APROVADO - prosseguir para ETL' if aprovado else 'REPROVADO - pipeline interrompido'}")
    if erros_criticos:
        for e in erros_criticos:
            logger.error(f"  CRITICO: {e}")
    logger.info("=" * 60)

    total_registros_bcb = sum(v.get("registros", 0) for v in validacoes_bcb.values())
    total_registros_b3  = sum(v.get("registros", 0) for v in validacoes_b3.values())

    return {
        "agent":                 "data-validator",
        "status":                "aprovado" if aprovado else "reprovado",
        "score_global":          score_global,
        "score_minimo":          SCORE_MINIMO,
        "aprovado":              aprovado,
        "total_arquivos_bcb":    len(validacoes_bcb),
        "total_arquivos_b3":     len(validacoes_b3),
        "total_registros_bcb":   total_registros_bcb,
        "total_registros_b3":    total_registros_b3,
        "erros_criticos":        erros_criticos,
        "validacoes_bcb":        validacoes_bcb,
        "validacoes_b3":         validacoes_b3,
        "validado_em":           datetime.now().isoformat(),
    }


if __name__ == "__main__":
    resultado = run()
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
    if not resultado["aprovado"]:
        sys.exit(1)
