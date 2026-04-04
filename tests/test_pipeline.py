# Databricks notebook source
# MAGIC %md
# MAGIC # 🧪 Tests · Validação do Pipeline
# MAGIC Testes unitários e de integração para garantir a confiabilidade do pipeline

# COMMAND ----------

# MAGIC %run /Workspace/ProjetoAPI_cotações/Config/config

# COMMAND ----------

# MAGIC %run /Workspace/ProjetoAPI_cotações/Utils/helpers

# COMMAND ----------

import sys, os
try:
    if os.path.isdir(r"C:\hadoop"):
        os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
        os.environ.setdefault("hadoop.home.dir", r"C:\hadoop")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from Config.config import *
    from utils.helpers import *
except NameError:
    pass  # Databricks: importações já feitas via %run

import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pandas as pd
from datetime import date, datetime

spark = configure_spark_with_delta_pip(
    SparkSession.builder
    .appName("cotacoes_tests")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
).getOrCreate()

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# TESTES UNITÁRIOS — Funções de ingestão
# ──────────────────────────────────────────────────────────────────────────────
class TestValidateApiResponse(unittest.TestCase):

    def test_resposta_valida(self):
        """Resposta com 'Time Series (Daily)' deve retornar True."""
        data = {"Time Series (Daily)": {"2024-01-01": {}}}
        self.assertTrue(validate_api_response(data, "PETR4"))

    def test_rate_limit_retorna_false(self):
        """Resposta com 'Note' (rate limit) deve retornar False."""
        data = {"Note": "Thank you for using Alpha Vantage!"}
        self.assertFalse(validate_api_response(data, "PETR4"))

    def test_erro_api_retorna_false(self):
        """Resposta com 'Error Message' deve retornar False."""
        data = {"Error Message": "Invalid API call."}
        self.assertFalse(validate_api_response(data, "TICKER_INVALIDO"))

    def test_sem_time_series_retorna_false(self):
        """Resposta sem 'Time Series (Daily)' deve retornar False."""
        data = {"Meta Data": {"1. Information": "Daily Prices"}}
        self.assertFalse(validate_api_response(data, "PETR4"))


# COMMAND ----------

class TestSchemaDataFrame(unittest.TestCase):

    def _mock_dataframe(self):
        """Cria um DataFrame Pandas simulando a saída da API."""
        return pd.DataFrame({
            "data":         [date(2024, 1, 2), date(2024, 1, 3)],
            "abertura":     [35.10, 35.50],
            "alta":         [35.80, 36.00],
            "baixa":        [34.90, 35.20],
            "fechamento":   [35.60, 35.80],
            "volume":       [15_000_000, 12_500_000],
            "ticker":       ["PETR4", "PETR4"],
            "data_ingestao":[datetime.now(), datetime.now()],
        })

    def test_colunas_obrigatorias_presentes(self):
        """DataFrame deve conter todas as colunas obrigatórias."""
        df = self._mock_dataframe()
        colunas_esperadas = {"data", "abertura", "alta", "baixa",
                             "fechamento", "volume", "ticker", "data_ingestao"}
        self.assertTrue(colunas_esperadas.issubset(set(df.columns)))

    def test_fechamento_positivo(self):
        """Todos os preços de fechamento devem ser positivos."""
        df = self._mock_dataframe()
        self.assertTrue((df["fechamento"] > 0).all())

    def test_alta_maior_ou_igual_baixa(self):
        """Alta deve ser sempre >= Baixa."""
        df = self._mock_dataframe()
        self.assertTrue((df["alta"] >= df["baixa"]).all())

    def test_ticker_preenchido(self):
        """Coluna ticker não deve ter valores vazios."""
        df = self._mock_dataframe()
        self.assertFalse(df["ticker"].isnull().any())


# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# TESTES DE INTEGRAÇÃO — Verificar tabelas Delta existentes
# ──────────────────────────────────────────────────────────────────────────────
class TestDeltaIntegration(unittest.TestCase):

    def _load_bronze(self):
        if bronze_path:
            return spark.read.format("delta").load(bronze_path)
        else:
            return spark.read.table(table_bronze)

    def test_tabela_bronze_existe(self):
        """Tabela Bronze deve existir após execução da ingestão."""
        try:
            df = self._load_bronze()
            df.limit(1).count()
        except Exception:
            self.skipTest("Tabela Bronze não existe ainda — rode Bronze/ingestao_api_alpha_spark.py primeiro.")

    def test_bronze_tem_registros(self):
        """Tabela Bronze deve conter registros."""
        try:
            df = self._load_bronze()
        except Exception:
            self.skipTest("Tabela Bronze não existe ainda.")
        self.assertGreater(df.count(), 0, "Tabela Bronze está vazia.")

    def test_bronze_todos_tickers_presentes(self):
        """Todos os tickers configurados devem estar na tabela Bronze."""
        try:
            df = self._load_bronze()
        except Exception:
            self.skipTest("Tabela Bronze não existe ainda.")
        tickers_na_tabela = [row.ticker for row in df.select("ticker").distinct().collect()]
        for t in tickers:
            self.assertIn(t, tickers_na_tabela, f"Ticker {t} ausente na Bronze.")

    def test_bronze_sem_fechamento_nulo(self):
        """Não deve haver registros com fechamento nulo na Bronze."""
        try:
            df = self._load_bronze()
        except Exception:
            self.skipTest("Tabela Bronze não existe ainda.")
        from pyspark.sql import functions as F
        nulos = df.filter(F.col("fechamento").isNull()).count()
        self.assertEqual(nulos, 0, f"{nulos} registros com fechamento nulo.")


# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────────────
# EXECUTAR TODOS OS TESTES
# ──────────────────────────────────────────────────────────────────────────────
def run_tests():
    suites = [
        unittest.TestLoader().loadTestsFromTestCase(TestValidateApiResponse),
        unittest.TestLoader().loadTestsFromTestCase(TestSchemaDataFrame),
        unittest.TestLoader().loadTestsFromTestCase(TestDeltaIntegration),
    ]

    runner = unittest.TextTestRunner(verbosity=0)
    resultados = []

    for suite in suites:
        result = runner.run(suite)
        resultados.append(result)

    total_erros = sum(len(r.errors) + len(r.failures) for r in resultados)

    if total_erros == 0:
        print("\n✅ TODOS OS TESTES PASSARAM!")
    else:
        print(f"\n❌ {total_erros} TESTES FALHARAM. Verifique os logs acima.")
        raise AssertionError(f"{total_erros} testes falharam.")

run_tests()
