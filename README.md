# Pipeline B3 — Cotações e Análise Financeira

Pipeline de dados financeiros brasileiros de ponta a ponta: ingestão de cotações da B3 e indicadores do Banco Central, processamento distribuído com **PySpark + Delta Lake** seguindo a **Medallion Architecture** (Bronze → Silver → Gold), modelagem de risco de crédito Basel III e dashboard no **Power BI** via Databricks.

---

## Arquitetura

```
  Alpha Vantage API          API SGS / BCB
  (cotações OHLCV B3)        (SELIC, IPCA, câmbio, PIB, desemprego, inadimplência)
          |                        |
          +----------+-------------+
                     |
                     v
          +-----------------------+
          |       BRONZE          |
          |   dados brutos        |
          |   Delta Lake          |
          |   append diário       |
          +-----------------------+
                     |
                     v
          +-----------------------+
          |       SILVER          |
          |   SMA7 / SMA20 / SMA50|
          |   volatilidade 20d    |
          |   retorno diário %    |
          |   flag acima_sma20    |
          +-----------------------+
                     |
                     v
          +-----------------------+
          |        GOLD           |
          |   resumo_diario       |
          |   performance_mensal  |
          |   ranking_acoes       |
          |   sinais_tecnicos     |
          +-----------------------+
                     |
                     v
          +-----------------------+
          |      POWER BI         |
          |   3 páginas           |
          |   DirectQuery         |
          |   Databricks (AWS)    |
          +-----------------------+
```

---

## Stack Tecnológica

| Tecnologia | Função |
|---|---|
| Apache Spark (PySpark) | Processamento distribuído |
| Delta Lake | Armazenamento ACID com time travel |
| Databricks (AWS) | Plataforma de execução + Unity Catalog |
| Alpha Vantage API | Cotações históricas OHLCV da B3 |
| API SGS/BCB | Indicadores macroeconômicos oficiais |
| Python 3.11 | Linguagem principal |
| Power BI | Dashboard via DirectQuery ao Databricks |
| Databricks Workflows | Agendamento diário às 20h BRT |
| unittest | 11 testes unitários e de integração |

---

## Estrutura do Projeto

```
pipeline-b3-cotacoes-spark-delta/
├── Config/
│   └── config.py                     # Parâmetros globais, API key, paths, schemas
├── Bronze/
│   └── ingestao_api_alpha_spark.py   # Ingestão Alpha Vantage → Delta Bronze
├── Silver/
│   └── transformacao_silver.py       # Limpeza + indicadores técnicos → Delta Silver
├── Gold/
│   └── agregacao_gold.py             # Agregações para dashboard → Delta Gold
├── Visualizacao/
│   └── view_dados_spark.py           # Análises exploratórias com Spark SQL
├── PowerBI/
│   └── gerar_pbit.py                 # Gerador do template Power BI (.pbit)
├── utils/
│   └── helpers.py                    # Retry, rate limit, validação, logging
├── tests/
│   └── test_pipeline.py              # 11 testes unitários e de integração
├── agents/                           # Agentes especializados do pipeline
├── docs/reports/                     # Relatórios gerados automaticamente
├── job_config.json                   # Configuração do Job Databricks Workflows
└── README.md
```

---

## Tabelas Delta Lake

| Tabela | Camada | Descrição |
|---|---|---|
| `bronze_cotacoes.cotacoes_raw` | Bronze | Dados brutos da API, particionado por ticker |
| `silver_cotacoes.cotacoes` | Silver | Dados limpos + SMA7/20/50 + volatilidade + retornos |
| `gold_cotacoes.resumo_diario` | Gold | Fechamento, variação e indicadores do último pregão |
| `gold_cotacoes.performance_mensal` | Gold | Retorno e volatilidade por ticker/mês |
| `gold_cotacoes.ranking_acoes` | Gold | Ranking de performance acumulada no período |
| `gold_cotacoes.sinais_tecnicos` | Gold | Golden/Death Cross e tendência BULL/BEAR |

---

## Guia de Implantação

### Pré-requisitos

| Requisito | Versão mínima |
|---|---|
| Databricks Runtime | 12.2 LTS+ |
| Apache Spark | 3.3+ |
| Delta Lake | 2.2+ |
| Python | 3.9+ |

### 1 — Obter API Key Alpha Vantage

1. Acesse https://www.alphavantage.co/support/#api-key
2. Preencha o formulário — chave enviada por e-mail
3. Plano gratuito: **25 requests/dia** · **5 requests/minuto**

### 2 — Configurar a API Key no Databricks (recomendado)

```bash
databricks secrets create-scope --scope cotacoes
databricks secrets put --scope cotacoes --key alpha_vantage_key
```

O `Config/config.py` já lê o secret automaticamente. Para execução local, defina a variável de ambiente:

```bash
export ALPHA_VANTAGE_API_KEY="sua_chave_aqui"
```

### 3 — Importar os Notebooks no Databricks

1. No Databricks Workspace, navegue até a pasta desejada
2. Clique em **Import** e importe os arquivos `.py` mantendo a estrutura:

```
/Workspace/ProjetoAPI_cotações/Config/config
/Workspace/ProjetoAPI_cotações/Bronze/ingestao_api_alpha_spark
/Workspace/ProjetoAPI_cotações/Silver/transformacao_silver
/Workspace/ProjetoAPI_cotações/Gold/agregacao_gold
/Workspace/ProjetoAPI_cotações/Visualizacao/view_dados_spark
/Workspace/ProjetoAPI_cotações/Utils/helpers
/Workspace/ProjetoAPI_cotações/tests/test_pipeline
```

### 4 — Executar em Ordem

```
1. Config/config                      → cria schemas no Unity Catalog
2. Bronze/ingestao_api_alpha_spark    → ingere cotações da API
3. Silver/transformacao_silver        → limpa e calcula indicadores
4. Gold/agregacao_gold                → gera tabelas para dashboard
5. tests/test_pipeline                → valida todas as camadas (11 testes)
```

### 5 — Agendar com Databricks Workflows

Importe o `job_config.json` no Databricks Workflows ou configure manualmente:

| Task | Notebook | Depende de |
|---|---|---|
| config | Config/config | — |
| bronze | Bronze/ingestao_api_alpha_spark | config |
| silver | Silver/transformacao_silver | bronze |
| gold | Gold/agregacao_gold | silver |
| tests | tests/test_pipeline | gold |

Agendamento: **20h BRT** (após fechamento do pregão às 18h)

### 6 — Conectar ao Power BI

1. Abra o Power BI Desktop → **Obter dados → Azure Databricks**
2. Server: `seu-workspace.cloud.databricks.com`
3. HTTP Path: `/sql/1.0/warehouses/seu_warehouse_id`
4. Autenticação: **Token** (Personal Access Token do Databricks)
5. Selecione as 4 tabelas `gold_cotacoes.*`

Ou gere o template automaticamente:

```bash
cd PowerBI
python gerar_pbit.py --server SEU_SERVER --http-path SEU_HTTP_PATH
```

---

## Consultas Úteis

```sql
-- Verificar histórico de versões (time travel)
DESCRIBE HISTORY gold_cotacoes.resumo_diario

-- Restaurar versão anterior após erro
RESTORE TABLE bronze_cotacoes.cotacoes_raw TO VERSION AS OF 3

-- Otimizar performance
OPTIMIZE silver_cotacoes.cotacoes ZORDER BY (ticker, data)

-- Consultar estado em data específica
SELECT * FROM bronze_cotacoes.cotacoes_raw
TIMESTAMP AS OF '2024-06-01'
WHERE ticker = 'PETR4'
```

---

## Customizações

### Adicionar tickers
```python
# Config/config.py
tickers = ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "WEGE3", "RENT3", "RADL3"]
```

### Buscar histórico completo
```python
# Config/config.py
OUTPUT_SIZE = "full"   # retorna todo o histórico disponível (~20 anos)
```

---

## Limitações do Plano Gratuito Alpha Vantage

| Limite | Valor |
|---|---|
| Requests por dia | 25 |
| Requests por minuto | 5 |
| Solução implementada | `time.sleep(13)` entre tickers |
| Upgrade | Premium: 75 req/min ($50/mês) |

---

*Portfólio de Data Engineering · PySpark · Delta Lake · Databricks · Medallion Architecture · Basel III*
