"""
Agente: b3-ingestion
Ingestão de cotações B3 via Alpha Vantage.
Tickers: PETR4, VALE3, ITUB4, BBDC4, ABEV3
"""

import os
import sys
import json
import time
import logging
import requests
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("b3-ingestion")

TICKERS = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"]

# A chave está presente no config.py do projeto
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "XF5JLDVTEE8RMIUN")
API_BASE_URL = "https://www.alphavantage.co/query"
REQUEST_DELAY_SECONDS = 13  # rate limit: ~5 req/min plano gratuito

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BRONZE_B3 = os.path.join(BASE_DIR, "data", "bronze", "b3")


def fetch_ticker(ticker: str, api_key: str,
                 start_date: str, end_date: str) -> Optional[dict]:
    """Busca cotações diárias de um ticker via Alpha Vantage."""
    ticker_api = ticker + ".SA"
    params = {
        "function":   "TIME_SERIES_DAILY",
        "symbol":     ticker_api,
        "apikey":     api_key,
        "outputsize": "full",   # histórico completo
        "datatype":   "json",
    }

    for attempt in range(1, 4):
        try:
            logger.info(f"[{ticker}] Buscando Alpha Vantage — tentativa {attempt}/3")
            resp = requests.get(API_BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            # Checar erros da API
            if "Note" in data:
                logger.warning(f"[{ticker}] Rate limit: {data['Note'][:80]}")
                time.sleep(60)
                continue
            if "Error Message" in data:
                logger.error(f"[{ticker}] Erro API: {data['Error Message']}")
                return None
            if "Information" in data:
                logger.warning(f"[{ticker}] Info API: {data['Information'][:80]}")
                return None
            if "Time Series (Daily)" not in data:
                logger.warning(f"[{ticker}] Sem 'Time Series (Daily)' na resposta.")
                return None

            # Filtrar por período
            series_raw = data["Time Series (Daily)"]
            series_filtered = {
                d: v for d, v in series_raw.items()
                if start_date <= d <= end_date
            }

            logger.info(f"[{ticker}] {len(series_filtered)} registros "
                        f"no período {start_date} a {end_date}.")
            return {
                "ticker":            ticker,
                "meta":              data.get("Meta Data", {}),
                "time_series_daily": series_filtered,
            }

        except Exception as e:
            logger.warning(f"[{ticker}] Tentativa {attempt} falhou: {e}")
            if attempt < 3:
                time.sleep(15 * attempt)

    logger.error(f"[{ticker}] Falhou após 3 tentativas.")
    return None


def save_bronze(ticker: str, dados: dict, run_date: str) -> str:
    """Salva dados brutos no diretório Bronze."""
    date_tag = run_date.replace("-", "")
    filename = f"{ticker}_{date_tag}.json"
    filepath = os.path.join(BRONZE_B3, filename)

    payload = {
        "ticker":        ticker,
        "source":        "Alpha Vantage",
        "ingested_at":   datetime.now().isoformat(),
        "total_records": len(dados.get("time_series_daily", {})),
        "data":          dados,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.info(f"[{ticker}] Salvo em: {filepath}")
    return filepath


def run(start_date: str = "2015-01-01", end_date: str = "2026-03-22") -> dict:
    """Executa ingestão completa dos tickers B3."""
    logger.info("=" * 60)
    logger.info("INICIANDO B3 INGESTION")
    logger.info(f"   Tickers : {TICKERS}")
    logger.info(f"   Periodo : {start_date} a {end_date}")
    logger.info(f"   API Key : {API_KEY[:4]}****")
    logger.info("=" * 60)

    os.makedirs(BRONZE_B3, exist_ok=True)

    resultados = {}
    total_registros = 0
    arquivos_gerados = []
    erros = []

    for i, ticker in enumerate(TICKERS):
        if i > 0:
            logger.info(f"Aguardando {REQUEST_DELAY_SECONDS}s (rate limit)...")
            time.sleep(REQUEST_DELAY_SECONDS)

        dados = fetch_ticker(ticker, API_KEY, start_date, end_date)

        if dados is None:
            erros.append(ticker)
            resultados[ticker] = {"status": "erro", "registros": 0}
            continue

        n_registros = len(dados.get("time_series_daily", {}))
        filepath = save_bronze(ticker, dados, end_date)
        total_registros += n_registros
        arquivos_gerados.append(filepath)
        resultados[ticker] = {
            "status":    "ok",
            "registros": n_registros,
            "arquivo":   filepath,
        }

    logger.info("=" * 60)
    logger.info("B3 INGESTION CONCLUIDA")
    logger.info(f"   Total registros : {total_registros:,}")
    logger.info(f"   Arquivos gerados: {len(arquivos_gerados)}")
    if erros:
        logger.warning(f"   Tickers com erro: {erros}")
    logger.info("=" * 60)

    return {
        "agent":            "b3-ingestion",
        "status":           "ok" if not erros else "parcial",
        "start_date":       start_date,
        "end_date":         end_date,
        "total_registros":  total_registros,
        "arquivos_gerados": arquivos_gerados,
        "tickers":          resultados,
        "erros":            erros,
    }


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2015-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-22"
    resultado = run(start, end)
    print(json.dumps(resultado, indent=2, ensure_ascii=False))
