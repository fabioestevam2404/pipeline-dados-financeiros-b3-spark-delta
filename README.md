# Pipeline de Dados Financeiros Brasileiros 🇧🇷

> Ingestão automatizada de cotações B3 e indicadores BCB · Medallion Architecture · PySpark · Delta Lake · Databricks · Power BI

---

## Sobre o projeto

Pipeline de dados end-to-end que transforma dados brutos de múltiplas fontes financeiras brasileiras em um dashboard analítico interativo. O projeto cobre desde a ingestão via APIs até a visualização no Power BI, seguindo boas práticas de engenharia de dados com Medallion Architecture e Spec-Driven Development.

**Período histórico:** Janeiro 2015 — Março 2026  
**Ativos monitorados:** PETR4, VALE3, ITUB4, BBDC4, ABEV3  
**Séries macroeconômicas:** SELIC, IPCA, câmbio USD/BRL, PIB, desemprego, inadimplência PF e PJ

---

## Arquitetura

```
Alpha Vantage API          SGS / BCB API
(cotações OHLCV B3)        (7 séries macro)
        |                        |
        +----------+-------------+
                   |
            Python · requests · pandas
                   |
        ┌──────────▼──────────┐
        │       BRONZE        │
        │  Delta Lake · ACID  │
        │  Append incremental │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │       SILVER        │
        │  SMA 7/20/50        │
        │  Volatilidade 20d   │
        │  Golden/Death Cross │
        │  BULL/BEAR          │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │        GOLD         │
        │  resumo_diario      │
        │  performance_mensal │
        │  ranking_acoes      │
        │  sinais_tecnicos    │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────┐
        │      POWER BI       │
        │  3 páginas          │
        │  DirectQuery        │
        └─────────────────────┘
```

**Plataforma:** Databricks Community Edition  
**Processamento:** Apache Spark 4.1 + PySpark  
**Armazenamento:** Delta Lake (Unity Catalog)  
**Orquestração:** notebook `pipeline_orquestrador` com `%run`

---

## Estrutura do projeto

```
ProjetoAPI_cotacoes/
├── Config/
│   └── config.py              # Parâmetros globais · API Key · databases
├── Bronze/
│   ├── SPEC.md                # Especificação da camada
│   └── ingestao_api_alpha_spark.py
├── Silver/
│   ├── SPEC.md
│   └── transformacao_silver.py
├── Gold/
│   ├── SPEC.md
│   └── agregacao_gold.py
├── tests/
│   └── test_pipeline.py       # 11 testes automatizados
├── utils/
│   └── helpers.py             # Funções auxiliares · retry · rate limit
├── pipeline_orquestrador      # Notebook que executa o pipeline completo
├── CLAUDE.md                  # Context Engineering para Claude Code
├── PRD.md                     # Requisitos do produto
└── docs/
    ├── ARCHITECTURE.md
    └── ADR/                   # Architecture Decision Records
```

---

## Tabelas Gold geradas

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `bronze_cotacoes.cotacoes_raw` | 500 | Dados brutos OHLCV por ticker/dia |
| `silver_cotacoes.cotacoes` | 515 | Dados limpos + indicadores técnicos |
| `gold_cotacoes.resumo_diario` | 515 | Visão operacional diária por ticker |
| `gold_cotacoes.performance_mensal` | 30 | Retorno e volume agrupados por mês |
| `gold_cotacoes.ranking_acoes` | 5 | Ranking por retorno acumulado |
| `gold_cotacoes.sinais_tecnicos` | 515 | Golden/Death Cross · BULL/BEAR |

---

## Indicadores calculados (camada Silver)

- **SMA 7, 20 e 50 dias** — médias móveis simples via Spark Window Functions
- **Retorno diário %** — variação de fechamento dia a dia
- **Retorno logarítmico** — base para cálculo de volatilidade
- **Volatilidade histórica 20d** — desvio padrão dos retornos log
- **Amplitude do pregão** — (máxima − mínima) / fechamento
- **Golden Cross / Death Cross** — cruzamento SMA7 × SMA20
- **Tendência BULL / BEAR** — fechamento acima/abaixo da SMA50

---

## Dashboard Power BI

**3 páginas interativas via DirectQuery ao Databricks:**

**Página 1 — Visão Geral**
- 6 cards de KPI: Preço Atual, Retorno Dia %, Volatilidade 20d, Ativos BULL, Golden Cross, Melhor Ação
- Tabela de sinais técnicos com formatação condicional (BULL verde / BEAR vermelho / Golden Cross dourado)
- Ranking de ações com retorno acumulado, % dias de alta e Sharpe proxy

**Página 2 — Performance Mensal**
- Gráfico de barras agrupadas: retorno mensal por ativo
- 4 KPI cards: retorno médio, melhor mês, pior mês, volume total

**Página 3 — Detalhes Diários**
- Segmentação interativa por ticker
- Tabela completa com 11 colunas: OHLCV, retorno, SMA20, SMA50, direção do dia

---

## Como executar

### Pré-requisitos

- Conta no [Databricks Community Edition](https://community.cloud.databricks.com)
- Chave gratuita da [Alpha Vantage API](https://www.alphavantage.co/support/#api-key)
- Python 3.12+

### Configuração

1. Clone o repositório e importe os notebooks para o Databricks Workspace
2. Abra `Config/config.py` e substitua a API Key:

```python
API_KEY = _os_env.environ.get("ALPHA_VANTAGE_API_KEY", "SUA_CHAVE_AQUI")
```

3. Execute o notebook `pipeline_orquestrador` com **Run all**

### Ordem de execução

```
Config → Bronze → Silver → Gold → Tests
```

O notebook `pipeline_orquestrador` executa tudo automaticamente via `%run`.

### Agendamento (Databricks Community)

No próprio notebook `pipeline_orquestrador`: clique em **Schedule** → configure o horário desejado (recomendado: 20h BRT, após fechamento do pregão).

---

## Stack tecnológica

| Ferramenta | Função |
|-----------|--------|
| Python 3.12 | Linguagem principal |
| Apache Spark 4.1 | Processamento distribuído |
| PySpark | API Python do Spark |
| Delta Lake | Armazenamento ACID com time travel |
| Databricks Community | Plataforma de execução |
| Alpha Vantage API | Cotações OHLCV da B3 |
| SGS / BCB | Indicadores macroeconômicos oficiais |
| Power BI Desktop | Dashboard via DirectQuery |
| unittest | 11 testes automatizados de validação |

---

## Metodologia

O projeto segue **Spec-Driven Development** — cada camada tem um `SPEC.md` como fonte de verdade antes de qualquer código. O `CLAUDE.md` carrega o contexto completo no Claude Code, garantindo consistência em todas as sessões de desenvolvimento.

**Architecture Decision Records (ADRs):**
- ADR-001: Delta Lake como formato de armazenamento
- ADR-002: Medallion Architecture (Bronze/Silver/Gold)
- ADR-003: Alpha Vantage como fonte de dados B3

---

## Autor

**Fabio Estevam**  
Engenheiro de Dados | [LinkedIn](https://linkedin.com/in/fabioestevam) | [GitHub](https://github.com/fabioestevam2404)

---

*Projeto desenvolvido para portfolio de engenharia de dados — 2026*
