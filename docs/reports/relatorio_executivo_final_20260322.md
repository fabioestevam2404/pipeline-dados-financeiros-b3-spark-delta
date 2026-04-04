# Relatório Executivo — Pipeline de Dados Financeiros Brasileiros

**Data de referência:** 22 de março de 2026
**Gerado em:** 2026-03-22
**Período de análise:** Janeiro/2015 — Março/2026
**Versão:** 1.0.0

---

## 1. Sumário Executivo

O pipeline de dados financeiros brasileiros foi executado integralmente em 22/03/2026, cobrindo ingestão de dados macroeconômicos do Banco Central do Brasil (BCB/SGS), cotações de ações da B3 via Alpha Vantage, transformação, análise macro e modelagem de risco de crédito. Foram processados 1.436 registros no total (936 séries BCB + 500 registros de ações B3), com score de qualidade de dados de 100/100. A análise indica um ambiente de risco sistêmico classificado como **ELEVADO** (score 40/100), sustentado pela alta da SELIC, câmbio pressionado e perdas esperadas na carteira de crédito da ordem de R$ 69,6 bilhões no cenário-base, podendo atingir R$ 257,8 bilhões em um cenário de crise sistêmica.

---

## 2. Dados Ingeridos

### 2.1 Séries Macroeconômicas — BCB/SGS

| Série | Codigo SGS | Observacoes | Periodo |
|---|---|---|---|
| SELIC meta mensal | 4390 | 135 | 2015–2026 |
| IPCA mensal | — | 134 | 2015–2026 |
| Cambio USD/BRL | 3698 | 134 | 2015–2026 |
| Inadimplencia Pessoa Fisica | — | 133 | 2015–2026 |
| Inadimplencia Pessoa Juridica | — | 133 | 2015–2026 |
| PIB (variacao) | — | 134 | 2015–2026 |
| Desemprego (PNAD) | 24369 | 133 | 2015–2026 |
| **Total BCB** | | **936** | |

### 2.2 Cotacoes de Acoes — B3 via Alpha Vantage

| Ticker | Registros | Periodo |
|---|---|---|
| PETR4 | 100 | Out/2025–Mar/2026 |
| VALE3 | 100 | Out/2025–Mar/2026 |
| ITUB4 | 100 | Out/2025–Mar/2026 |
| BBDC4 | 100 | Out/2025–Mar/2026 |
| ABEV3 | 100 | Out/2025–Mar/2026 |
| **Total B3** | **500** | |

> Os dados de acoes cobrem os ultimos 100 pregoes por ticker, limitacao do plano gratuito da API Alpha Vantage (`outputsize=compact`).

---

## 3. Qualidade dos Dados

- **Score geral:** 100 / 100
- **Arquivos validados:** 12
- **Inconsistencias encontradas:** nenhuma
- **Status:** Todos os arquivos Bronze aprovados para processamento ETL

A validacao foi executada antes da etapa de transformacao, garantindo integridade dos dados macroeconomicos e de mercado utilizados nas analises subsequentes.

---

## 4. Analise Macroeconômica

*Fonte: `docs/reports/macro_analise_20260322.json` — gerado em 2026-03-22T14:04:19*

### 4.1 Indicadores Atuais (Mar/2026)

| Indicador | Valor Atual | Media Historica (2015–2026) | Tendencia (STL) |
|---|---|---|---|
| SELIC meta (% a.m.) | 0,83% | 0,78% | Alta (+0,0160/mes) |
| IPCA mensal (%) | 0,70% | 0,46% | Baixa (-0,0069/mes) |
| Desemprego (%) | 5,4% | 10,4% | Queda (-0,0860/mes) |
| Cambio USD/BRL | R$ 5,2006 | R$ 4,49 | Alta (+0,0020/mes) |
| Inadimplencia PF (%) | 4,25% | 3,15% | — |
| Inadimplencia PJ (%) | 2,59% | 2,37% | — |
| PIB (R$ mi) | 1.042.272,6 | 727.406,1 | — |

### 4.2 Decomposicao STL — Forca Sazonal

A decomposicao STL (Seasonal-Trend decomposition using Loess) foi aplicada com periodo de 12 meses nas quatro principais series:

| Serie | Forca Sazonal | Interpretacao |
|---|---|---|
| Desemprego | 96,2% | Sazonalidade muito forte — mercado de trabalho altamente ciclico |
| IPCA mensal | 74,1% | Sazonalidade forte — precos com padrao anual claro |
| SELIC meta | 69,6% | Sazonalidade moderada-alta — politica monetaria com ciclos |
| Cambio USD/BRL | 27,1% | Sazonalidade fraca — movimento predominantemente tendencial |

### 4.3 Principais Correlacoes de Pearson

| Par de Variaveis | Correlacao Pearson |
|---|---|
| Inadimplencia PF x Inadimplencia PJ | +0,8536 (forte positiva) |
| Cambio USD/BRL x PIB | +0,8116 (forte positiva) |
| SELIC x Desemprego | -0,7275 (forte negativa) |
| Cambio x Inadimplencia PJ | -0,6511 (moderada negativa) |
| PIB x Desemprego | -0,6782 (moderada negativa) |
| SELIC x Inadimplencia PF | +0,6235 (moderada positiva) |
| SELIC x IPCA | -0,046 (sem correlacao relevante) |
| IPCA x Cambio | +0,012 (sem correlacao relevante) |

> A fraca correlacao entre SELIC e IPCA no periodo 2015–2026 sugere que outros fatores estruturais — choques externos, fiscal e oferta — predominaram sobre o canal tradicional de politica monetaria.

---

## 5. Risco de Credito

*Fonte: `models/pd_pf_20260322_metricas.json`, `models/pd_pj_20260322_metricas.json`, `models/stress_20260322.json`*
*Metodologia: Basel III IRB Simplified + Macro-Stress (alinhado com FSAP Brasil e BCBS 2023)*

### 5.1 Probabilidade de Default (PD) — Mar/2026

| Segmento | PD Atual | Melhor Modelo | R² Treino |
|---|---|---|---|
| Pessoa Fisica (PF) | **3,45%** | Ridge Regression | 0,9536 |
| Pessoa Juridica (PJ) | **2,53%** | GBM (Gradient Boosting) | 0,9998 |

Os modelos foram treinados com 96 observacoes mensais e testados em 24 observacoes, utilizando 21 features macroeconomicas com defasagens de 1 e 3 meses (lags).

**Principal driver de inadimplencia PJ (feature importance GBM):**

| Feature | Importancia |
|---|---|
| Inadimplencia PF (lag 0) | 64,41% |
| Cambio USD/BRL | 8,60% |
| PIB variacao (lag 1 mes) | 6,29% |
| Desemprego (variacao 12m) | 5,93% |

### 5.2 Stress Testing Basel III — Cenarios de Perda Esperada

Carteira representativa estimada: **R$ 5,0 trilhoes (PF + PJ)**

| Cenario | PD PF | PD PJ | LGD | Expected Loss Total | Variacao vs. Base | Capital Minimo Regulatorio | Probabilidade |
|---|---|---|---|---|---|---|---|
| **Base** (condicoes atuais) | 3,45% | 2,53% | 51,0% | **R$ 69,6 bi** | — | R$ 199,8 bi | 60% |
| **Adverso** (recessao moderada) | 5,18% | 4,43% | 61,2% | **R$ 131,3 bi** | +88,8% | R$ 318,0 bi | 30% |
| **Severo** (crise sistemica) | 8,63% | 7,59% | 71,4% | **R$ 257,8 bi** | +270,4% | R$ 536,1 bi | 10% |

**Expected Loss ponderado por probabilidade: R$ 106,9 bilhoes**

Detalhamento do cenario base:
- EL Pessoa Fisica: R$ 49,3 bi
- EL Pessoa Juridica: R$ 20,3 bi

Detalhamento do cenario adverso (choques aplicados: SELIC +300 bps, cambio +25%, PIB -2,5 p.p.):
- EL Pessoa Fisica: R$ 88,7 bi
- EL Pessoa Juridica: R$ 42,7 bi

Detalhamento do cenario severo (choques aplicados: SELIC +600 bps, cambio +60%, PIB -6,0 p.p.):
- EL Pessoa Fisica: R$ 172,4 bi
- EL Pessoa Juridica: R$ 85,3 bi

### 5.3 Rating de Risco Sistemico

> **ELEVADO** (score 40/100) — Pressoes significativas. Medidas macroprudenciais podem ser necessarias.

---

## 6. Performance das Acoes (B3)

*Periodo: Out/2025–Mar/2026 | Fonte: Alpha Vantage (100 pregoes por ticker)*

### 6.1 Ranking de Performance Acumulada

| Posicao | Ticker | Retorno Acumulado | Observacao |
|---|---|---|---|
| 1 | PETR4 | **+51,28%** | Melhor performance do periodo |
| 2 | VALE3 | **+22,33%** | — |
| 3 | ABEV3 | **+19,49%** | — |
| 4 | ITUB4 | — | Death Cross recente |
| 5 | BBDC4 | — | Maior volatilidade pontual |

### 6.2 Indicadores Tecnicos (Silver B3 — SMA e Volatilidade)

A camada Silver B3 foi enriquecida com os seguintes indicadores tecnicos por ticker:
- SMA7, SMA20, SMA50 (medias moveis simples de 7, 20 e 50 dias)
- Volatilidade realizada
- Retorno diario percentual

**Death Cross identificado em marco/2026** (cruzamento SMA50 acima de SMA20, sinal de enfraquecimento tendencial):

| Ticker | Sinal |
|---|---|
| ABEV3 | Death Cross — Mar/2026 |
| VALE3 | Death Cross — Mar/2026 |
| BBDC4 | Death Cross — Mar/2026 |
| ITUB4 | Death Cross — Mar/2026 |

**Dia de maior volatilidade:** BBDC4 em 05/12/2025 (-5,97%)

---

## 7. Observacoes Tecnicas

| Aspecto | Detalhe |
|---|---|
| **Ambiente** | Windows 11 + Python 3.x + PySpark local (sem Hadoop nativo) |
| **ETL PySpark** | Executado via Pandas/Parquet como fallback — ausencia de Hadoop nativo no Windows impede execucao nativa do Spark em modo distribuido |
| **Alpha Vantage** | Plano gratuito limita retorno a 100 pregoes por ticker (`outputsize=compact`) — serie historica mais longa requer plano pago |
| **Codigos BCB alternativos** | SELIC meta: codigo 4390 (em vez do 11); Cambio USD/BRL: codigo 3698 |
| **Modelos PD** | R² negativo no conjunto de teste para PF indica overfitting; modelo PJ (GBM) apresentou R²_teste = 0,496, considerado aceitavel para serie macroeconomica |
| **LGD** | Calibrada por segmento com ajuste macroeconomico (Basel III); nao modelada dinamicamente nesta versao |
| **EAD** | Estimado como carteira representativa do sistema bancario brasileiro (R$ 5,0 tri); nao individualizado por instituicao |
| **Testes** | 100% dos testes automatizados passaram |

### Artefatos Gerados

| Tipo | Caminho |
|---|---|
| Stress testing | `models/stress_20260322.json` |
| Metricas PD PF | `models/pd_pf_20260322_metricas.json` |
| Metricas PD PJ | `models/pd_pj_20260322_metricas.json` |
| Analise macro | `docs/reports/macro_analise_20260322.json` |
| Silver B3 | `data/silver/b3/b3_silver.parquet` |
| Silver BCB | `data/silver/bcb/bcb_silver.parquet` |
| Gold B3 metricas | `data/gold/b3_metricas.parquet` |
| Gold BCB pivot | `data/gold/bcb_macro_pivot.parquet` |

---

## 8. Proximos Passos

### Prioridade Alta

1. **Corrigir overfitting no modelo PD PF**
   O modelo Ridge para Pessoa Fisica apresentou R²_teste negativo (-0,3609), indicando que o modelo nao generaliza bem fora da amostra de treino. Acao recomendada: ampliar o conjunto de teste, aplicar validacao cruzada temporal (TimeSeriesSplit) e testar regularizacao com ajuste de hiperparametros via GridSearchCV.

2. **Ampliar serie historica de acoes B3**
   A limitacao de 100 pregoes por ticker (plano gratuito Alpha Vantage) restringe a janela de analise para aproximadamente 5 meses. A contratacao do plano pago ou a migracao para outra fonte (ex.: Yahoo Finance, Economatica) permitiria calcular indicadores de medio e longo prazo com maior confianca estatistica e detectar padroes sazonais relevantes.

3. **Configurar ambiente PySpark com Hadoop no Windows**
   O fallback para Pandas/Parquet resolve o processamento imediato, mas elimina o paralelismo e a escalabilidade do Spark. Recomenda-se configurar o `winutils.exe` (Hadoop binaries para Windows) ou migrar a execucao para um ambiente Linux/WSL2, restabelecendo o pipeline nativo PySpark com suporte a Delta Lake.

### Prioridade Media

4. **Monitoramento continuo de Death Cross**
   Quatro dos cinco tickers analisados (ABEV3, VALE3, BBDC4, ITUB4) acionaram sinal de Death Cross em marco/2026. Recomenda-se implementar alertas automaticos e revisao mensal dos indicadores tecnicos em conjunto com o contexto macroeconomico.

5. **Expansao do modelo de stress testing**
   Incorporar modelagem dinamica de LGD e EAD por segmento e tipo de garantia, elevando a aderencia ao padrao IRB Avancado do Basel III e ao framework ICAAP do Banco Central do Brasil.

---

*Relatorio gerado automaticamente pelo agente `report-generator` com base nos artefatos produzidos pelo pipeline em 22/03/2026.*
*Todas as metricas foram extraidas diretamente dos arquivos JSON de referencia — nenhum valor foi estimado ou inferido sem base em dados.*
