# Documentação Técnica — Pipeline de Dados Financeiros Brasileiros

**Data de referência dos dados:** 2026-03-22
**Gerado em:** 2026-03-23
**Versão:** 1.0.0

---

## BLOCO 1 — Documentação Técnica do Projeto

---

### 1. O que é o projeto

O **Pipeline B3 — Cotações e Análise Financeira** é um sistema de engenharia de dados de ponta a ponta construído para ingerir, transformar, analisar e visualizar dados do mercado de capitais brasileiro e indicadores macroeconômicos. O pipeline consome cotações históricas OHLCV (Open, High, Low, Close, Volume) de sete ativos da B3 via Alpha Vantage API — PETR4, VALE3, ITUB4, BBDC4, ABEV3, WEGE3 e MGLU3 — e combina esses dados com sete séries do Sistema Gerenciador de Séries Temporais (SGS) do Banco Central do Brasil: SELIC, IPCA, câmbio USD/BRL, inadimplência de pessoas físicas, inadimplência de pessoas jurídicas, PIB e desemprego. Sobre essa base integrada, o sistema calcula indicadores técnicos, modela risco de crédito e entrega resultados prontos para consumo em dashboards analíticos.

O projeto cobre o período de janeiro de 2015 a março de 2026, totalizando 936 observações de indicadores macroeconômicos BCB e 500 observações de cotações B3.

---

### 2. Para que serve

#### Casos de uso

**Análise técnica de mercado**
Profissionais de investimento e analistas quantitativos acessam métricas de médias móveis (SMA7, SMA20, SMA50), volatilidade de 20 dias, retorno diário e sinais de cruzamento (Golden Cross / Death Cross) para embasar decisões de entrada e saída em posições.

**Monitoramento macroeconômico**
Economistas e gestores de risco acompanham a evolução de indicadores do Banco Central com decomposição de tendência e sazonalidade via STL, além de matrizes de correlação Pearson e Spearman entre SELIC, IPCA, câmbio, PIB, desemprego e inadimplência.

**Modelagem de risco de crédito**
Times de risco em bancos e fintechs utilizam os modelos de Probability of Default (PD) para Pessoas Físicas e Jurídicas, calibrados com variáveis macro e seus defasamentos de 1 e 3 meses, para estimar perdas esperadas e requerimentos de capital regulatório.

**Stress testing regulatório**
A metodologia Basel III IRB Simplified com choques macroeconômicos reais permite simular o impacto de cenários adversos e severos sobre a carteira de crédito, em linha com os requerimentos do BCBS 2023 e do FSAP Brasil.

#### Valor gerado

- Elimina coleta manual de dados dispersos em múltiplas fontes (API privada + API pública do BCB)
- Garante rastreabilidade completa de cada dado via Delta Lake com time travel
- Produz 4 tabelas Gold diretamente consumíveis pelo Power BI, reduzindo o tempo entre ingestão e insight
- Automatiza toda a cadeia — da ingestão ao dashboard — com execução diária às 20h BRT sem intervenção humana

#### Público-alvo

Engenheiros de dados, analistas quantitativos, gestores de risco de crédito, economistas e profissionais de investimento que precisam de dados financeiros brasileiros confiáveis, atualizados e prontos para análise.

---

### 3. Arquitetura — Medallion Architecture

O pipeline segue o padrão **Medallion Architecture**, que organiza os dados em três camadas progressivas de qualidade e agregação. Cada camada resolve um problema específico e serve a um propósito distinto.

```
  Alpha Vantage API          API SGS / BCB
  (cotações OHLCV B3)        (7 séries macro)
          |                        |
          +----------+-------------+
                     |
                     v
          +-----------------------+
          |       BRONZE          |
          |   (dados brutos)      |
          |   Delta Lake / JSON   |
          |   Append diário       |
          +-----------------------+
                     |
                     v
          +-----------------------+
          |       SILVER          |
          |  (dados limpos e      |
          |   enriquecidos)       |
          |  SMA7/20/50           |
          |  Volatilidade 20d     |
          |  Retorno diário %     |
          |  Amplitude %          |
          |  Flag acima_sma20     |
          +-----------------------+
                     |
                     v
          +-----------------------+
          |        GOLD           |
          |  (métricas de negócio)|
          |  resumo_diario        |
          |  performance_mensal   |
          |  ranking_acoes        |
          |  sinais_tecnicos      |
          +-----------------------+
                     |
                     v
          +-----------------------+
          |      POWER BI         |
          |  (3 páginas de        |
          |   dashboard)          |
          |  DirectQuery          |
          |  Databricks           |
          +-----------------------+
```

#### Camada Bronze — Fidelidade ao dado original

A camada Bronze armazena os dados exatamente como chegam das APIs, sem qualquer transformação. O objetivo é preservar a fonte de verdade. Todo dado que chega é persistido em Delta Lake com append incremental diário, garantindo que nenhuma informação histórica seja perdida. Isso permite auditoria completa e reprocessamento retroativo caso a lógica de negócio mude.

Artefatos gerados:
- `data/bronze/bcb/serie_{codigo}_{YYYYMMDD}.json` — séries BCB por código SGS
- `data/bronze/b3/{ticker}_{YYYYMMDD}.json` — cotações diárias por ticker

#### Camada Silver — Qualidade e enriquecimento

A camada Silver recebe os dados brutos, aplica validações de tipo, trata valores ausentes e calcula indicadores técnicos. É aqui que o dado bruto se torna analiticamente utilizável. Os indicadores calculados são:

| Indicador         | Descrição                                      |
|-------------------|------------------------------------------------|
| SMA7              | Média móvel simples de 7 dias                  |
| SMA20             | Média móvel simples de 20 dias                 |
| SMA50             | Média móvel simples de 50 dias                 |
| Volatilidade 20d  | Desvio padrão do retorno nos últimos 20 dias   |
| Retorno diário %  | Variação percentual do fechamento dia a dia    |
| Amplitude %       | (High - Low) / Close * 100                     |
| Flag acima_sma20  | Booleano: fechamento acima da SMA20            |

#### Camada Gold — Agregações para decisão

A camada Gold transforma os dados enriquecidos em métricas de negócio diretamente consumíveis. São quatro tabelas:

| Tabela               | Conteúdo                                                   |
|----------------------|------------------------------------------------------------|
| `resumo_diario`      | Fechamento, variação, volume e indicadores por ticker/dia  |
| `performance_mensal` | Retorno acumulado, volatilidade e volume médio por mês     |
| `ranking_acoes`      | Classificação dos ativos por performance no período        |
| `sinais_tecnicos`    | Golden Cross, Death Cross, tendência BULL/BEAR por ativo   |

---

### 4. Stack tecnológica

#### Apache Spark (PySpark) — Processamento distribuído

**O que é:** Framework open-source de processamento distribuído de dados, mantido pela Apache Software Foundation. Permite processar grandes volumes de dados em paralelo usando múltiplos nós de computação.

**Por que foi escolhido:** O volume de dados combinados (cotações históricas de 7 ativos + 7 séries macro desde 2015) cresce diariamente. Spark permite escalar horizontalmente sem reescrever o código. Além disso, a integração nativa com Delta Lake e Databricks elimina a necessidade de conectores externos. A API PySpark permite escrever transformações em Python com a expressividade do SQL distribuído.

#### Delta Lake — Formato de tabela ACID

**O que é:** Camada de armazenamento open-source que adiciona garantias ACID (Atomicidade, Consistência, Isolamento, Durabilidade) sobre arquivos Parquet armazenados em sistemas de arquivos distribuídos como S3 ou DBFS.

**Por que foi escolhido:** O pipeline precisa fazer append diário sem corromper dados históricos. O Delta Lake resolve isso com controle transacional. O recurso de **time travel** (versionamento automático de cada escrita) permite consultar o estado da tabela em qualquer data passada, o que é crítico para auditoria de dados financeiros. A funcionalidade `OPTIMIZE` com `ZORDER BY` mantém a performance de leitura mesmo com crescimento do volume.

Exemplos de uso no projeto:
```sql
-- Consultar estado da tabela em data específica
SELECT * FROM bronze_cotacoes.cotacoes_raw
TIMESTAMP AS OF '2024-06-01'
WHERE ticker = 'PETR4'

-- Restaurar versão anterior após erro de ingestão
RESTORE TABLE bronze_cotacoes.cotacoes_raw TO VERSION AS OF 3
```

#### Databricks (AWS) — Plataforma de execução com Unity Catalog

**O que é:** Plataforma de lakehouse unificada que combina um ambiente gerenciado de Apache Spark com ferramentas de orquestração, catálogo de dados, segurança e colaboração. Roda sobre AWS, Azure ou GCP.

**Por que foi escolhido:** Elimina a complexidade de gerenciar infraestrutura Spark manualmente (clusters, dependências, escalonamento). O **Unity Catalog** centraliza o controle de acesso a todas as tabelas Delta. O **Databricks Workflows** (Jobs) permite agendar o pipeline com dependências entre tarefas, alertas por e-mail em caso de falha e monitoramento de execução, tudo sem código adicional de orquestração.

#### Alpha Vantage API — Dados de mercado B3

**O que é:** API REST de dados financeiros que fornece cotações históricas e em tempo real de ações, câmbio e criptomoedas. Oferece plano gratuito com 25 requisições/dia e 5 requisições/minuto.

**Por que foi escolhida:** É uma das poucas APIs que disponibiliza cotações históricas da B3 (bolsa brasileira) em formato padronizado sem exigir assinatura de provedores premium como Bloomberg ou Refinitiv. O pipeline implementa controle de rate limit com `time.sleep(13)` entre requisições de tickers para respeitar o limite de 5 req/min do plano gratuito.

#### API SGS/BCB — Indicadores macroeconômicos

**O que é:** Sistema Gerenciador de Séries Temporais do Banco Central do Brasil, que disponibiliza gratuitamente via API REST mais de 180.000 séries temporais de indicadores econômicos brasileiros.

**Por que foi escolhida:** É a fonte oficial e primária dos indicadores macroeconômicos brasileiros. Dados de SELIC, IPCA, câmbio e inadimplência provenientes diretamente do BCB têm credibilidade regulatória, o que é indispensável para modelos de risco que precisam ser auditáveis por órgãos reguladores.

#### Python 3.11 — Linguagem principal

**O que é:** Linguagem de programação de alto nível, amplamente adotada em ciência de dados e engenharia de dados.

**Por que foi escolhida:** Versão 3.11 traz melhorias de desempenho de até 25% sobre a 3.10. O ecossistema Python é o mais completo para o problema: PySpark para processamento, `requests` para consumo de APIs, `statsmodels` para decomposição STL, `scikit-learn` para modelagem PD, e `unittest` para testes automatizados.

#### Power BI — Visualização e dashboard

**O que é:** Plataforma de business intelligence da Microsoft para criação de relatórios interativos e dashboards com suporte a múltiplas fontes de dados.

**Por que foi escolhido:** A conexão nativa **DirectQuery ao Databricks** via conector ODBC elimina a necessidade de exportar dados para um banco intermediário. Cada atualização do dashboard consulta diretamente a camada Gold do Delta Lake. As 16 medidas DAX pré-configuradas encapsulam cálculos complexos (como retorno acumulado e volatilidade histórica) dentro do próprio modelo semântico do Power BI, tornando o dashboard auto-suficiente.

#### Databricks Workflows (Jobs) — Agendamento

**O que é:** Sistema nativo de orquestração do Databricks que permite criar fluxos de trabalho com múltiplas tarefas, dependências, agendamentos e alertas.

**Por que foi escolhido:** Integrado nativamente ao ambiente onde o código já roda, sem necessidade de ferramentas externas como Apache Airflow ou Prefect. O agendamento para as **20h BRT** garante que os dados do pregão (encerrado às 18h) já estejam disponíveis quando o job executa.

#### unittest (Python) — Testes automatizados

**O que é:** Framework nativo de testes do Python, sem dependências externas.

**Por que foi escolhido:** Onze testes (unitários e de integração) validam automaticamente o pipeline a cada execução no Databricks Workflows, garantindo que transformações Silver e agregações Gold produzam resultados corretos antes que os dados cheguem ao dashboard.

---

### 5. Fluxo do pipeline — do dado bruto ao dashboard

O pipeline executa em cinco etapas sequenciais, agendadas diariamente às 20h BRT via Databricks Workflows.

#### Etapa 1 — Configuração (Config/config.py)

Inicializa os databases Delta no Unity Catalog, define os parâmetros globais (lista de tickers, caminhos de armazenamento, chave de API via Databricks Secrets) e cria as estruturas de diretório necessárias. Esta etapa não acessa APIs externas.

#### Etapa 2 — Ingestão Bronze (Bronze/ingestao_api_alpha_spark.py)

Para cada ticker na lista de ativos monitorados, realiza uma chamada à Alpha Vantage API solicitando o histórico completo de cotações diárias OHLCV. O módulo `utils/helpers.py` aplica retry automático com backoff exponencial em caso de falha de rede e controle de rate limit (`time.sleep(13)`) entre requisições para respeitar o limite de 5 req/min.

Simultaneamente, o agente `bcb-ingestion` consulta as 7 séries do SGS/BCB. Os dados brutos de ambas as fontes são gravados em Delta Lake na camada Bronze via PySpark, com particionamento por `ticker` (B3) ou código de série (BCB).

Resultado: tabela `bronze_cotacoes.cotacoes_raw` atualizada com os dados do dia.

#### Etapa 3 — Transformação Silver (Silver/transformacao_silver.py)

Lê a camada Bronze via Spark DataFrame, aplica o seguinte pipeline de transformação:

1. Conversão de tipos (datas como `DateType`, preços como `DoubleType`)
2. Remoção de duplicatas por `(ticker, data)`
3. Ordenação temporal por ticker
4. Cálculo de SMA7, SMA20 e SMA50 via funções de janela (`Window.partitionBy("ticker").orderBy("data").rowsBetween(...)`)
5. Cálculo de volatilidade de 20 dias (desvio padrão do retorno sobre janela deslizante)
6. Cálculo do retorno diário percentual e amplitude percentual
7. Flag booleana `acima_sma20`

O resultado é gravado na tabela `silver_cotacoes.cotacoes` com modo `overwrite` para garantir que recalculações retroativas sejam corretamente aplicadas.

#### Etapa 4 — Agregação Gold

Com base na camada Silver, quatro transformações independentes produzem as tabelas Gold:

- **resumo_diario:** seleção das colunas relevantes para visão operacional diária
- **performance_mensal:** `GROUP BY ticker, year(data), month(data)` com retorno acumulado do mês, volatilidade e volume médio
- **ranking_acoes:** ordenação dos ativos por retorno acumulado no período analisado
- **sinais_tecnicos:** lógica condicional para detectar Golden Cross (SMA7 cruza SMA20 de baixo para cima), Death Cross (cruzamento inverso) e classificação de tendência BULL (fechamento > SMA50) ou BEAR (fechamento < SMA50)

#### Etapa 5 — Validação e entrega (tests/test_pipeline.py + Power BI)

Os 11 testes automatizados validam:
- Contagem de registros nas tabelas Bronze, Silver e Gold
- Ausência de valores nulos em colunas críticas (ticker, data, close)
- Consistência dos indicadores calculados (SMA20 >= 0, volatilidade >= 0)
- Integridade referencial entre camadas

Após a validação, as tabelas Gold ficam disponíveis no Unity Catalog para o Power BI via **DirectQuery**. O dashboard de 3 páginas (Visão Geral, Performance Mensal, Detalhes Diários) é atualizado automaticamente quando o usuário abre ou recarrega o relatório.

#### Diagrama de dependências do Job Databricks

```
config
  |
  v
bronze (ingestão Alpha Vantage + BCB)
  |
  v
silver (limpeza + indicadores técnicos)
  |
  v
gold (agregações para dashboard)
  |
  v
tests (11 testes de validação)
  |
  v
Power BI (DirectQuery — disponível automaticamente)
```

---

### Apêndice A — Métricas dos Modelos de Risco de Crédito

*Data de referência: 2026-03-22. Fonte: models/pd_pf_20260322_metricas.json e models/pd_pj_20260322_metricas.json*

#### Modelo PD — Pessoas Físicas

| Parametro           | Valor                       |
|---------------------|-----------------------------|
| Melhor modelo       | Ridge Regression            |
| Observacoes treino  | 96 meses                    |
| Observacoes teste   | 24 meses                    |
| Features            | 21 (variaveis macro + lags) |
| PD atual estimada   | 3,45%                       |
| MAE treino (Ridge)  | 0,0822                      |
| MAE teste (Ridge)   | 0,3751                      |
| R2 treino (Ridge)   | 0,9536                      |

#### Modelo PD — Pessoas Juridicas

| Parametro           | Valor                         |
|---------------------|-------------------------------|
| Melhor modelo       | Gradient Boosting (GBM)       |
| Observacoes treino  | 96 meses                      |
| Observacoes teste   | 24 meses                      |
| Features            | 21 (variaveis macro + lags)   |
| PD atual estimada   | 2,53%                         |
| MAE treino (GBM)    | 0,0097                        |
| MAE teste (GBM)     | 0,0812                        |
| R2 treino (GBM)     | 0,9998                        |
| R2 teste (GBM)      | 0,4960                        |

A feature mais relevante para o modelo PJ foi `inadimplencia_pf` (importancia relativa: 0,6441), seguida de `cambio_usd_brl` (0,0860) e `pib_variacao_lag1` (0,0629).

#### Stress Testing — Basel III IRB Simplified

*Metodologia: Basel III IRB Simplified + Macro-Stress. EAD calibrado para carteira representativa de R$ 5,0 tri (PF + PJ)*

| Cenario            | EL Total (R$ bi) | Delta vs. Base | Capital Minimo (R$ bi) | Probabilidade |
|--------------------|-----------------|----------------|------------------------|---------------|
| Base               | 69,58           | --             | 199,84                 | 60%           |
| Adverso            | 131,34          | +88,76%        | 318,03                 | 30%           |
| Severo             | 257,76          | +270,45%       | 536,13                 | 10%           |
| Ponderado          | 106,93          | --             | --                     | --            |

Choques aplicados no cenario severo: SELIC +600 bps, cambio +60%, PIB -6,0 pp, resultando em PD PF de 8,63% e PD PJ de 7,59%.

---

### Apêndice B — Indicadores Macroeconomicos (Referencia: 2026-03-22)

*Fonte: docs/reports/macro_analise_20260322.json. Periodo: 2015-01-01 a 2026-03-22*

| Serie              | Ultimo Valor | Media Historica | Desvio Padrao | Tendencia (slope/mes) |
|--------------------|-------------|-----------------|---------------|-----------------------|
| SELIC meta (% a.m.)| 0,83        | 0,7796          | 0,3246        | +0,016039 (alta)      |
| IPCA mensal (%)    | 0,70        | 0,4578          | 0,4001        | -0,006908 (baixa)     |
| Cambio USD/BRL     | 5,2006      | 4,4937          | 0,9471        | +0,002026 (alta)      |
| Desemprego (%)     | 5,4         | 10,3662         | 2,7657        | -0,085976 (baixa)     |
| Inadimplencia PF (%| 4,25        | 3,1501          | 0,4803        | --                    |
| Inadimplencia PJ (%| 2,59        | 2,3718          | 0,6361        | --                    |

Principais correlacoes Pearson identificadas:
- PIB x Cambio USD/BRL: **+0,8116** (forte positiva)
- Inadimplencia PF x Inadimplencia PJ: **+0,8536** (forte positiva)
- SELIC x Desemprego: **-0,7275** (forte negativa)
- PIB x Desemprego: **-0,6782** (moderada negativa)

Forca sazonal STL por serie:
- Desemprego: **0,9616** (sazonalidade muito forte)
- IPCA mensal: **0,7406** (sazonalidade forte)
- SELIC meta: **0,6956** (sazonalidade moderada)
- Cambio USD/BRL: **0,2705** (sazonalidade fraca)

---

## BLOCO 2 — Post LinkedIn

---

Construi um pipeline completo de dados financeiros brasileiros do zero — do BCB e da B3 ao Power BI — rodando em producao no Databricks.

O mercado financeiro brasileiro gera dados dispersos em multiplas fontes. Cotacoes da B3 em uma API, SELIC e IPCA no Banco Central, inadimplencia em outro endpoint. Ninguem te entrega isso integrado e pronto para analisar.

Entao montei a estrutura inteira:

- Ingestao diaria de 7 ativos B3 (PETR4, VALE3, ITUB4, BBDC4, ABEV3, WEGE3, MGLU3) via Alpha Vantage API
- Consumo de 7 series macroeconomicas do SGS/BCB (SELIC, IPCA, cambio, PIB, desemprego, inadimplencia PF e PJ)
- Medallion Architecture em Delta Lake: Bronze (dado bruto) → Silver (SMA7/20/50, volatilidade 20d, retorno diario) → Gold (4 tabelas para dashboard)
- Modelagem de risco de credito com PD para PF (3,45%) e PJ (2,53%), stress testing Basel III com 3 cenarios de choque macro
- Decomposicao STL e correlacoes Pearson/Spearman entre indicadores do BCB
- Job Databricks agendado para as 20h com 11 testes automatizados validando cada camada
- Dashboard Power BI com 3 paginas e 16 medidas DAX via DirectQuery ao Databricks

O mais interessante foi descobrir que a correlacao SELIC x desemprego e de -0,73 (Pearson) — fortemente negativa ao longo de 11 anos de dados. E que no cenario de crise sistemica (SELIC +600 bps, cambio +60%, PIB -6%), a Expected Loss sobe 270% sobre o cenario base.

Engenharia de dados nao e so mover arquivos. E construir infraestrutura que transforma dado bruto em decisao.

Se voce trabalha com dados financeiros no Brasil, fico feliz em trocar experiencia.

#DataEngineering #PySpark #DeltaLake #Databricks #Python #BCB #B3 #RiscoDeCredito #PowerBI #MedallionArchitecture #FinancasBrasileiras #MachineLearning
