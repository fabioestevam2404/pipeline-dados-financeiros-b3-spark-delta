# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ Utils — Funções Auxiliares
# MAGIC Retry logic, logging e helpers compartilhados entre os notebooks

# COMMAND ----------

import time
import logging
from functools import wraps
from typing import Callable, Any

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DE LOGGING
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("cotacoes_pipeline")


# ──────────────────────────────────────────────────────────────────────────────
# DECORATOR: RETRY COM BACKOFF EXPONENCIAL
# ──────────────────────────────────────────────────────────────────────────────
def retry(max_attempts: int = 3, delay_seconds: float = 30, backoff: float = 2.0):
    """
    Decorator que re-executa uma função em caso de falha.

    Parâmetros:
        max_attempts   : número máximo de tentativas
        delay_seconds  : segundos de espera antes da 1ª re-tentativa
        backoff        : multiplicador do delay a cada tentativa (exponencial)

    Uso:
        @retry(max_attempts=3, delay_seconds=30)
        def minha_funcao():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            wait = delay_seconds
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        logger.error(f"❌ [{func.__name__}] Falhou após {max_attempts} tentativas: {e}")
                        raise
                    logger.warning(
                        f"⚠️  [{func.__name__}] Tentativa {attempt}/{max_attempts} falhou: {e}. "
                        f"Aguardando {wait:.0f}s..."
                    )
                    time.sleep(wait)
                    wait *= backoff
        return wrapper
    return decorator


# ──────────────────────────────────────────────────────────────────────────────
# RATE LIMITER — respeitar limite de 5 req/min da API gratuita
# ──────────────────────────────────────────────────────────────────────────────
def rate_limit_wait(seconds: float, ticker: str) -> None:
    """Aguarda entre requisições para respeitar o rate limit da API."""
    logger.info(f"   ⏳ Rate limit: aguardando {seconds}s antes do próximo ticker ({ticker})...")
    time.sleep(seconds)


# ──────────────────────────────────────────────────────────────────────────────
# VALIDADOR DE RESPOSTA DA API
# ──────────────────────────────────────────────────────────────────────────────
def validate_api_response(data: dict, ticker: str) -> bool:
    """
    Valida se a resposta da API contém os dados esperados.

    Casos de falha comuns:
    - Rate limit atingido  → {"Note": "Thank you for using Alpha Vantage!..."}
    - Ticker inválido      → {"Error Message": "Invalid API call..."}
    - Sem dados            → resposta sem "Time Series (Daily)"
    """
    if "Note" in data:
        logger.warning(f"[{ticker}] ⚠️  Rate limit atingido: {data['Note'][:80]}...")
        return False

    if "Error Message" in data:
        logger.error(f"[{ticker}] ❌ Erro da API: {data['Error Message']}")
        return False

    if "Information" in data:
        logger.warning(f"[{ticker}] ℹ️  Aviso da API: {data['Information'][:80]}...")
        return False

    if "Time Series (Daily)" not in data:
        logger.warning(f"[{ticker}] ⚠️  Sem dados de série temporal na resposta.")
        return False

    return True


# ──────────────────────────────────────────────────────────────────────────────
# HELPER: VERIFICAR SE TABELA DELTA EXISTE
# ──────────────────────────────────────────────────────────────────────────────
def table_exists(spark, table_name: str) -> bool:
    """Verifica se uma tabela existe no catálogo Spark."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# HELPER: CONTAR REGISTROS POR TICKER
# ──────────────────────────────────────────────────────────────────────────────
def log_table_stats(spark, table_name: str) -> None:
    """Exibe estatísticas da tabela Delta após a escrita."""
    try:
        stats = spark.sql(f"""
            SELECT
                ticker,
                COUNT(*)    AS total_registros,
                MIN(data)   AS data_inicio,
                MAX(data)   AS data_fim
            FROM {table_name}
            GROUP BY ticker
            ORDER BY ticker
        """)
        logger.info(f"📊 Estatísticas da tabela [{table_name}]:")
        stats.show(truncate=False)
    except Exception as e:
        logger.warning(f"Não foi possível exibir estatísticas: {e}")


print("✅ Utils carregadas.")
