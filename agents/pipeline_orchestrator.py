"""
Agente: pipeline-orchestrator
Coordena o pipeline completo de dados financeiros brasileiros.

Fluxo:
  Etapa 1 (PARALELA)  : bcb-ingestion + b3-ingestion
  Etapa 2 (SEQUENCIAL): data-validator  (score >= 70 para prosseguir)
  Etapa 3 (SEQUENCIAL): etl-transformer (Bronze → Silver → Gold)
  Etapa 4 (PARALELA)  : macro-analyst + credit-risk-analyst
  Etapa 5 (SEQUENCIAL): report-generator
"""

import os
import sys
import json
import logging
import threading
import time
from datetime import datetime
from typing import Optional

# Adicionar diretório agents ao path
AGENTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, AGENTS_DIR)

if os.path.isdir(r"C:\hadoop"):
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("pipeline-orchestrator")

BASE_DIR = os.path.abspath(os.path.join(AGENTS_DIR, ".."))
LOGS_DIR = os.path.join(BASE_DIR, "logs")


# ── Utilitários ────────────────────────────────────────────────────────────────

def _run_in_thread(func, args, results_dict, key):
    """Executa função em thread separada, capturando resultado/erro."""
    try:
        results_dict[key] = func(*args)
    except Exception as e:
        logger.error(f"Thread [{key}] falhou: {e}")
        results_dict[key] = {"status": "erro", "erro": str(e), "agent": key}


def _run_parallel(tasks: list) -> dict:
    """
    Executa múltiplas tarefas em paralelo.
    tasks: lista de (nome, func, args)
    Retorna: dict {nome: resultado}
    """
    results = {}
    threads = []

    for nome, func, args in tasks:
        t = threading.Thread(
            target=_run_in_thread,
            args=(func, args, results, nome),
            daemon=True,
        )
        t.start()
        threads.append((nome, t))

    # Aguardar todas as threads (timeout: 10 min cada)
    for nome, t in threads:
        t.join(timeout=600)
        if t.is_alive():
            logger.warning(f"Thread [{nome}] excedeu timeout de 10 minutos.")

    return results


# ── Pipeline ───────────────────────────────────────────────────────────────────

def run_pipeline(start_date: str = "2015-01-01",
                 end_date: str   = "2026-03-22") -> dict:
    """Executa o pipeline completo."""

    pipeline_inicio = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(LOGS_DIR, exist_ok=True)

    log = {
        "pipeline_id":  timestamp,
        "inicio":       datetime.now().isoformat(),
        "start_date":   start_date,
        "end_date":     end_date,
        "etapas":       {},
        "status":       "iniciado",
    }

    logger.info("=" * 70)
    logger.info("PIPELINE FINANCEIRO BRASILEIRO — INICIO")
    logger.info(f"   Periodo : {start_date} a {end_date}")
    logger.info(f"   ID      : {timestamp}")
    logger.info("=" * 70)

    # ── ETAPA 1: Ingestão (PARALELA) ──────────────────────────────────────────
    logger.info("\n--- ETAPA 1: INGESTAO (paralela) ---")
    t1_inicio = time.time()

    import bcb_ingestion
    import b3_ingestion

    resultados_etapa1 = _run_parallel([
        ("bcb_ingestion", bcb_ingestion.run,  [start_date, end_date]),
        ("b3_ingestion",  b3_ingestion.run,   [start_date, end_date]),
    ])

    t1_fim = time.time()
    log["etapas"]["etapa1_ingestao"] = {
        "duracao_s":     round(t1_fim - t1_inicio, 1),
        "bcb_ingestion": resultados_etapa1.get("bcb_ingestion", {}),
        "b3_ingestion":  resultados_etapa1.get("b3_ingestion",  {}),
    }

    bcb_ok = resultados_etapa1.get("bcb_ingestion", {}).get("status") in ("ok", "parcial")
    b3_ok  = resultados_etapa1.get("b3_ingestion",  {}).get("status") in ("ok", "parcial")

    logger.info(f"Etapa 1 concluida em {t1_fim - t1_inicio:.1f}s | "
                f"BCB: {'OK' if bcb_ok else 'ERRO'} | B3: {'OK' if b3_ok else 'ERRO'}")

    if not bcb_ok:
        logger.error("Ingestao BCB falhou criticamente. Pipeline interrompido.")
        log["status"] = "erro_ingestao_bcb"
        return _finalizar_log(log, pipeline_inicio, LOGS_DIR, timestamp)

    # ── ETAPA 2: Validação (SEQUENCIAL, OBRIGATÓRIA) ──────────────────────────
    logger.info("\n--- ETAPA 2: VALIDACAO DE DADOS BRONZE ---")
    t2_inicio = time.time()

    import data_validator

    resultado_validacao = data_validator.run(
        bcb_result=resultados_etapa1.get("bcb_ingestion"),
        b3_result=resultados_etapa1.get("b3_ingestion"),
    )

    t2_fim = time.time()
    log["etapas"]["etapa2_validacao"] = {
        "duracao_s": round(t2_fim - t2_inicio, 1),
        **resultado_validacao,
    }

    score_global = resultado_validacao.get("score_global", 0)
    aprovado     = resultado_validacao.get("aprovado", False)

    logger.info(f"Etapa 2 concluida em {t2_fim - t2_inicio:.1f}s | "
                f"Score: {score_global}/100 | "
                f"{'APROVADO' if aprovado else 'REPROVADO'}")

    if not aprovado:
        erros = resultado_validacao.get("erros_criticos", [])
        logger.error(f"Validacao REPROVADA (score={score_global} < 70). Pipeline interrompido.")
        for e in erros:
            logger.error(f"  - {e}")
        log["status"] = "reprovado_validacao"
        return _finalizar_log(log, pipeline_inicio, LOGS_DIR, timestamp)

    # ── ETAPA 3: Transformação ETL (SEQUENCIAL) ────────────────────────────────
    logger.info("\n--- ETAPA 3: TRANSFORMACAO ETL (Bronze -> Silver -> Gold) ---")
    t3_inicio = time.time()

    import etl_transformer

    resultado_etl = etl_transformer.run(start_date, end_date)

    t3_fim = time.time()
    log["etapas"]["etapa3_etl"] = {
        "duracao_s": round(t3_fim - t3_inicio, 1),
        **resultado_etl,
    }

    logger.info(f"Etapa 3 concluida em {t3_fim - t3_inicio:.1f}s | "
                f"Registros: {resultado_etl.get('total_registros', 0):,}")

    # ── ETAPA 4: Análise (PARALELA) ────────────────────────────────────────────
    logger.info("\n--- ETAPA 4: ANALISE (paralela) ---")
    t4_inicio = time.time()

    import macro_analyst
    import credit_risk_analyst

    resultados_etapa4 = _run_parallel([
        ("macro_analyst",       macro_analyst.run,       [start_date, end_date]),
        ("credit_risk_analyst", credit_risk_analyst.run, [start_date, end_date]),
    ])

    t4_fim = time.time()
    log["etapas"]["etapa4_analise"] = {
        "duracao_s":            round(t4_fim - t4_inicio, 1),
        "macro_analyst":        resultados_etapa4.get("macro_analyst",       {}),
        "credit_risk_analyst":  resultados_etapa4.get("credit_risk_analyst", {}),
    }

    macro_ok  = resultados_etapa4.get("macro_analyst",       {}).get("status") == "ok"
    credit_ok = resultados_etapa4.get("credit_risk_analyst", {}).get("status") == "ok"

    logger.info(f"Etapa 4 concluida em {t4_fim - t4_inicio:.1f}s | "
                f"Macro: {'OK' if macro_ok else 'ERRO'} | "
                f"Credit: {'OK' if credit_ok else 'ERRO'}")

    # ── ETAPA 5: Relatório (SEQUENCIAL) ────────────────────────────────────────
    logger.info("\n--- ETAPA 5: RELATORIO EXECUTIVO ---")
    t5_inicio = time.time()

    import report_generator

    resultado_report = report_generator.run(
        pipeline_log=log,
        start_date=start_date,
        end_date=end_date,
    )

    t5_fim = time.time()
    log["etapas"]["etapa5_relatorio"] = {
        "duracao_s": round(t5_fim - t5_inicio, 1),
        **resultado_report,
    }

    logger.info(f"Etapa 5 concluida em {t5_fim - t5_inicio:.1f}s | "
                f"Report: {resultado_report.get('report_path', 'N/A')}")

    # ── Finalizar ──────────────────────────────────────────────────────────────
    log["status"] = "concluido"
    return _finalizar_log(log, pipeline_inicio, LOGS_DIR, timestamp,
                          resultado_report=resultado_report,
                          resultados_etapa1=resultados_etapa1,
                          resultado_etl=resultado_etl,
                          resultados_etapa4=resultados_etapa4)


def _finalizar_log(log: dict, pipeline_inicio: float, logs_dir: str,
                   timestamp: str, **kwargs) -> dict:
    """Finaliza e persiste o log do pipeline."""
    duracao_total = time.time() - pipeline_inicio
    log["fim"]           = datetime.now().isoformat()
    log["duracao_total_s"] = round(duracao_total, 1)

    # Consolidar totais
    total_registros = 0
    try:
        re1 = kwargs.get("resultados_etapa1", {})
        total_registros += re1.get("bcb_ingestion", {}).get("total_registros", 0)
        total_registros += re1.get("b3_ingestion",  {}).get("total_registros", 0)

        etl = kwargs.get("resultado_etl", {})
        total_registros += etl.get("total_registros", 0)
    except Exception:
        pass

    log["total_registros_processados"] = total_registros

    # Artefatos gerados
    artefatos = {}
    try:
        report = kwargs.get("resultado_report", {})
        artefatos = report.get("artefatos", {})
    except Exception:
        pass
    log["artefatos_gerados"] = artefatos

    # Salvar log
    log_path = os.path.join(logs_dir, f"pipeline_{timestamp}.json")
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE FINANCEIRO BRASILEIRO — SUMARIO FINAL")
    logger.info("=" * 70)
    logger.info(f"   Status         : {log['status'].upper()}")
    logger.info(f"   Duracao total  : {duracao_total:.1f}s ({duracao_total/60:.1f} min)")
    logger.info(f"   Total registros: {total_registros:,}")
    logger.info(f"   Log salvo em   : {log_path}")

    # Etapas
    for etapa, dados in log.get("etapas", {}).items():
        dur = dados.get("duracao_s", 0)
        logger.info(f"   {etapa:<30} {dur:>6.1f}s")

    # Artefatos
    logger.info("\nArtefatos gerados:")
    for tipo, paths in artefatos.items():
        if paths:
            for p in (paths if isinstance(paths, list) else [paths]):
                logger.info(f"   [{tipo}] {p}")

    logger.info("=" * 70)

    log["log_path"] = log_path
    return log


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else "2015-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-03-22"
    resultado = run_pipeline(start, end)
    print("\n" + "=" * 70)
    print(json.dumps({
        "status":                    resultado.get("status"),
        "duracao_total_s":           resultado.get("duracao_total_s"),
        "total_registros_processados": resultado.get("total_registros_processados"),
        "log_path":                  resultado.get("log_path"),
    }, indent=2, ensure_ascii=False))
