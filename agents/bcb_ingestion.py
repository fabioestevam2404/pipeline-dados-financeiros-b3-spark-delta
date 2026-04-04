"""
Agente: bcb-ingestion
Ingestão de séries históricas do BCB/SGS via API pública.
Séries: SELIC, IPCA, câmbio USD/BRL, inadimplência PF, inadimplência PJ, PIB, desemprego.
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime, date
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("bcb-ingestion")

# ── Séries BCB/SGS ─────────────────────────────────────────────────────────────
BCB_SERIES = {
    "selic_meta":         432,    # Taxa SELIC meta (% a.a.)
    "ipca_mensal":        433,    # IPCA variação mensal (%)
    "cambio_usd_brl":     1,      # Taxa de câmbio USD/BRL - compra
    "inadimplencia_pf":   21082,  # Inadimplência carteira PF (%)
    "inadimplencia_pj":   21083,  # Inadimplência carteira PJ (%)
    "pib_variacao":       4380,   # PIB variação trimestral (%)
    "desemprego":         24369,  # Taxa de desemprego PNAD (%)
}

BCB_API_BASE = os.getenv("BCB_API_BASE", "https://api.bcb.gov.br/dados/serie")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_BCB = os.path.join(BASE_DIR, "data", "bronze", "bcb")


def fetch_bcb_series(serie_name: str, serie_code: int,
                     start_date: str, end_date: str) -> Optional[list]:
    """Busca série temporal do BCB SGS."""
    url = f"{BCB_API_BASE}/bcdata.sgs.{serie_code}/dados"
    params = {
        "formato": "json",
        "dataInicial": datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y"),
        "dataFinal":   datetime.strptime(end_date,   "%Y-%m-%d").strftime("%d/%m/%Y"),
    }

    for attempt in range(1, 4):
        try:
            logger.info(f"[{serie_name}] Buscando série {serie_code} "
                        f"({start_date} → {end_date}) — tentativa {attempt}/3")
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            dados = resp.json()

            if not isinstance(dados, list) or len(dados) == 0:
                logger.warning(f"[{serie_name}] Resposta vazia ou inesperada.")
                return []

            logger.info(f"[{serie_name}] {len(dados)} registros obtidos.")
            return dados

        except requests.exceptions.HTTPError as e:
            logger.warning(f"[{serie_name}] HTTP {e.response.status_code}: {e}")
            if e.response.status_code == 404:
                logger.error(f"[{serie_name}] Série não encontrada (404).")
                return []
        except Exception as e:
            logger.warning(f"[{serie_name}] Erro na tentativa {attempt}: {e}")

        if attempt < 3:
            time.sleep(5 * attempt)

    logger.error(f"[{serie_name}] Falhou após 3 tentativas.")
    return None


def save_bronze(serie_name: str, serie_code: int, dados: list,
                run_date: str) -> str:
    """Salva dados brutos no diretório Bronze."""
    date_tag = run_date.replace("-", "")
    filename = f"serie_{serie_code}_{date_tag}.json"
    filepath = os.path.join(BRONZE_BCB, filename)

    payload = {
        "serie_name":    serie_name,
        "serie_code":    serie_code,
        "source":        "BCB/SGS",
        "ingested_at":   datetime.now().isoformat(),
        "total_records": len(dados),
        "data":          dados,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"[{serie_name}] Salvo em: {filepath}")
    return filepath


def run(start_date: str = "2015-01-01", end_date: str = "2026-03-22") -> dict:
    """Executa ingestão completa das séries BCB."""
    logger.info("=" * 60)
    logger.info("INICIANDO BCB INGESTION")
    logger.info(f"   Periodo : {start_date} a {end_date}")
    logger.info(f"   Series  : {list(BCB_SERIES.keys())}")
    logger.info("=" * 60)

    os.makedirs(BRONZE_BCB, exist_ok=True)
    run_date = end_date  # usa data final como tag do arquivo

    resultados = {}
    total_registros = 0
    arquivos_gerados = []
    erros = []

    for serie_name, serie_code in BCB_SERIES.items():
        dados = fetch_bcb_series(serie_name, serie_code, start_date, end_date)

        if dados is None:
            erros.append(serie_name)
            resultados[serie_name] = {"status": "erro", "registros": 0}
            continue

        filepath = save_bronze(serie_name, serie_code, dados, run_date)
        total_registros += len(dados)
        arquivos_gerados.append(filepath)
        resultados[serie_name] = {
            "status":    "ok",
            "registros": len(dados),
            "arquivo":   filepath,
            "serie_code": serie_code,
        }

        # Pequena pausa para não sobrecarregar a API
        time.sleep(1)

    logger.info("=" * 60)
    logger.info("BCB INGESTION CONCLUIDA")
    logger.info(f"   Total registros : {total_registros:,}")
    logger.info(f"   Arquivos gerados: {len(arquivos_gerados)}")
    if erros:
        logger.warning(f"   Series com erro : {erros}")
    logger.info("=" * 60)

    return {
        "agent":            "bcb-ingestion",
        "status":           "ok" if not erros else "parcial",
        "start_date":       start_date,
        "end_date":         end_date,
        "total_registros":  total_registros,
        "arquivos_gerados": arquivos_gerados,
        "series":           resultados,
        "erros":            erros,
    }


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2015-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-22"
    resultado = run(start, end)
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
