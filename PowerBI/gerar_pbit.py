"""
Gerador de template Power BI (.pbit) para o pipeline B3
Conecta às 4 tabelas Gold do Databricks e cria medidas DAX pré-configuradas.

Uso:
    pip install pyyaml
    python gerar_pbit.py --server SEU_SERVER --http-path SEU_HTTP_PATH

Exemplo:
    python gerar_pbit.py \
        --server dbc-999a1703-9e43.cloud.databricks.com \
        --http-path /sql/1.0/warehouses/810a5e82b5ca7158
"""

import argparse
import json
import zipfile
import os
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# ARGUMENTOS
# ──────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Gera template Power BI para pipeline B3")
parser.add_argument("--server",    default="SEU_SERVER.azuredatabricks.net",
                    help="Hostname do Databricks SQL Warehouse")
parser.add_argument("--http-path", default="/sql/1.0/warehouses/SEU_HTTP_PATH",
                    help="HTTP Path do SQL Warehouse")
parser.add_argument("--catalog",   default="workspace",
                    help="Catálogo Unity Catalog (padrão: workspace)")
parser.add_argument("--output",    default="Dashboard_B3_Cotacoes.pbit",
                    help="Nome do arquivo de saída")
args = parser.parse_args()

CATALOG   = args.catalog
SERVER    = args.server
HTTP_PATH = args.http_path
OUTPUT    = args.output

# ──────────────────────────────────────────────────────────────────────────────
# QUERIES POWER QUERY M — uma por tabela Gold
# ──────────────────────────────────────────────────────────────────────────────
def m_query(schema, table):
    return (
        f'let\n'
        f'    Fonte = Databricks.Catalogs("{SERVER}", "{HTTP_PATH}", []),\n'
        f'    NavCatalog = Fonte{{[Name="{CATALOG}",Kind="Catalog"]}}[Data],\n'
        f'    NavSchema  = NavCatalog{{[Name="{schema}",Kind="Schema"]}}[Data],\n'
        f'    NavTable   = NavSchema{{[Name="{table}",Kind="Table"]}}[Data]\n'
        f'in\n'
        f'    NavTable'
    )

TABELAS = [
    ("gold_cotacoes", "resumo_diario",      "ResumoDiario"),
    ("gold_cotacoes", "performance_mensal", "PerformanceMensal"),
    ("gold_cotacoes", "ranking_acoes",      "RankingAcoes"),
    ("gold_cotacoes", "sinais_tecnicos",    "SinaisTecnicos"),
]

# ──────────────────────────────────────────────────────────────────────────────
# MEDIDAS DAX
# ──────────────────────────────────────────────────────────────────────────────
MEDIDAS = [
    # ── ResumoDiario ──────────────────────────────────────────────────────────
    {
        "table": "ResumoDiario",
        "name": "Preço Atual",
        "expression": "SELECTEDVALUE(ResumoDiario[fechamento], BLANK())",
        "formatString": "R$ #,##0.00",
        "description": "Fechamento do último pregão"
    },
    {
        "table": "ResumoDiario",
        "name": "Retorno Dia %",
        "expression": "SELECTEDVALUE(ResumoDiario[retorno_diario_pct], BLANK())",
        "formatString": "#,##0.00%",
        "description": "Variação percentual do dia"
    },
    {
        "table": "ResumoDiario",
        "name": "Volatilidade 20d %",
        "expression": "AVERAGE(ResumoDiario[volatilidade_20d_pct])",
        "formatString": "#,##0.0000%",
        "description": "Volatilidade histórica média de 20 dias"
    },
    {
        "table": "ResumoDiario",
        "name": "Qtd Acima SMA20",
        "expression": "CALCULATE(COUNTROWS(ResumoDiario), ResumoDiario[acima_sma20] = TRUE())",
        "formatString": "0",
        "description": "Quantidade de ativos acima da SMA20"
    },

    # ── PerformanceMensal ─────────────────────────────────────────────────────
    {
        "table": "PerformanceMensal",
        "name": "Retorno Mensal Médio %",
        "expression": "AVERAGE(PerformanceMensal[retorno_mensal_pct])",
        "formatString": "#,##0.00%",
        "description": "Retorno mensal médio do período selecionado"
    },
    {
        "table": "PerformanceMensal",
        "name": "Volume Total (Mi)",
        "expression": "SUM(PerformanceMensal[volume_total_mi])",
        "formatString": "#,##0.00",
        "description": "Volume financeiro total em milhões"
    },
    {
        "table": "PerformanceMensal",
        "name": "Melhor Mês %",
        "expression": "MAXX(PerformanceMensal, PerformanceMensal[retorno_mensal_pct])",
        "formatString": "#,##0.00%",
        "description": "Maior retorno mensal no período"
    },
    {
        "table": "PerformanceMensal",
        "name": "Pior Mês %",
        "expression": "MINX(PerformanceMensal, PerformanceMensal[retorno_mensal_pct])",
        "formatString": "#,##0.00%",
        "description": "Menor retorno mensal no período"
    },

    # ── RankingAcoes ──────────────────────────────────────────────────────────
    {
        "table": "RankingAcoes",
        "name": "Retorno Período %",
        "expression": "SELECTEDVALUE(RankingAcoes[retorno_periodo_pct], BLANK())",
        "formatString": "#,##0.00%",
        "description": "Retorno acumulado no período completo"
    },
    {
        "table": "RankingAcoes",
        "name": "Sharpe Proxy",
        "expression": "SELECTEDVALUE(RankingAcoes[sharpe_proxy], BLANK())",
        "formatString": "#,##0.0000",
        "description": "Proxy do índice Sharpe (retorno / desvio)"
    },
    {
        "table": "RankingAcoes",
        "name": "% Dias de Alta",
        "expression": "SELECTEDVALUE(RankingAcoes[pct_dias_alta], BLANK())",
        "formatString": "#,##0.0%",
        "description": "Percentual de pregões com retorno positivo"
    },
    {
        "table": "RankingAcoes",
        "name": "Melhor Ação",
        "expression": (
            "VAR _t = TOPN(1, RankingAcoes, RankingAcoes[retorno_periodo_pct], DESC)\n"
            "RETURN MAXX(_t, RankingAcoes[ticker])"
        ),
        "formatString": "@",
        "description": "Ação com maior retorno no período"
    },

    # ── SinaisTecnicos ────────────────────────────────────────────────────────
    {
        "table": "SinaisTecnicos",
        "name": "Qtd Golden Cross",
        "expression": "CALCULATE(COUNTROWS(SinaisTecnicos), SinaisTecnicos[sinal_medias] = \"GOLDEN CROSS\")",
        "formatString": "0",
        "description": "Quantidade de ativos com Golden Cross ativo"
    },
    {
        "table": "SinaisTecnicos",
        "name": "Qtd Death Cross",
        "expression": "CALCULATE(COUNTROWS(SinaisTecnicos), SinaisTecnicos[sinal_medias] = \"DEATH CROSS\")",
        "formatString": "0",
        "description": "Quantidade de ativos com Death Cross ativo"
    },
    {
        "table": "SinaisTecnicos",
        "name": "Qtd Bull",
        "expression": "CALCULATE(COUNTROWS(SinaisTecnicos), SinaisTecnicos[tendencia_longo_prazo] = \"BULL\")",
        "formatString": "0",
        "description": "Quantidade de ativos em tendência de alta (SMA20 > SMA50)"
    },
    {
        "table": "SinaisTecnicos",
        "name": "Qtd Bear",
        "expression": "CALCULATE(COUNTROWS(SinaisTecnicos), SinaisTecnicos[tendencia_longo_prazo] = \"BEAR\")",
        "formatString": "0",
        "description": "Quantidade de ativos em tendência de baixa (SMA20 < SMA50)"
    },
]

# ──────────────────────────────────────────────────────────────────────────────
# CONTENT TYPES
# ──────────────────────────────────────────────────────────────────────────────
CONTENT_TYPES = """<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="json" ContentType="application/json" />
  <Default Extension="xml"  ContentType="application/xml" />
  <Override PartName="/DataModelSchema"    ContentType="application/json" />
  <Override PartName="/DiagramLayout"      ContentType="application/json" />
  <Override PartName="/Report/Layout"      ContentType="application/json" />
  <Override PartName="/SecurityBindings"   ContentType="application/json" />
  <Override PartName="/Connections"        ContentType="application/json" />
  <Override PartName="/Version"            ContentType="application/json" />
</Types>"""

# ──────────────────────────────────────────────────────────────────────────────
# DATA MODEL SCHEMA (Tabular Model)
# ──────────────────────────────────────────────────────────────────────────────
def build_data_model():
    tables = []

    for schema, table_src, table_name in TABELAS:
        # Medidas desta tabela
        medidas_tabela = [m for m in MEDIDAS if m["table"] == table_name]
        measures = [
            {
                "name": m["name"],
                "expression": m["expression"],
                "formatString": m["formatString"],
                "description": m["description"],
                "annotations": [
                    {"name": "PBI_FormatHint", "value": "{\"isGeneralNumber\":true}"}
                ]
            }
            for m in medidas_tabela
        ]

        tables.append({
            "name": table_name,
            "description": f"Tabela Gold: {schema}.{table_src}",
            "columns": [],       # Power BI infere colunas no DirectQuery
            "measures": measures,
            "partitions": [
                {
                    "name": table_name,
                    "mode": "directQuery",
                    "source": {
                        "type": "m",
                        "expression": m_query(schema, table_src)
                    }
                }
            ]
        })

    return {
        "name": "SemanticModel",
        "compatibilityLevel": 1567,
        "model": {
            "culture": "pt-BR",
            "dataAccessOptions": {
                "legacyRedirects": True,
                "returnErrorValuesAsNull": True
            },
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "pt-BR",
            "tables": tables,
            "relationships": [],
            "annotations": [
                {"name": "PBIDesktopVersion", "value": "2.128.0"},
                {"name": "__PBI_TimeIntelligenceEnabled", "value": "1"},
            ]
        }
    }

# ──────────────────────────────────────────────────────────────────────────────
# DIAGRAM LAYOUT
# ──────────────────────────────────────────────────────────────────────────────
def build_diagram_layout():
    nodes = []
    x_positions = [0, 400, 800, 1200]
    for i, (_, _, table_name) in enumerate(TABELAS):
        nodes.append({
            "NodeIndex": f"{table_name}",
            "Location": {"x": x_positions[i], "y": 0},
            "Size": {"width": 300, "height": 200},
            "ZIndex": i,
            "collapsed": False
        })
    return {
        "version": "1.3.0",
        "diagrams": [
            {
                "ordinal": 0,
                "nodes": nodes,
                "scrollPosition": {"x": 0, "y": 0}
            }
        ]
    }

# ──────────────────────────────────────────────────────────────────────────────
# REPORT LAYOUT — 3 páginas
# ──────────────────────────────────────────────────────────────────────────────
def build_report_layout():
    return {
        "id": 0,
        "resourcePackages": [],
        "sections": [
            {
                "id": 0,
                "name": "Visão Geral",
                "displayName": "📊 Visão Geral",
                "ordinal": 0,
                "visualContainers": [
                    # Título
                    _visual_text_box(
                        x=20, y=10, w=1240, h=60,
                        text="Pipeline B3 · Cotações em Tempo Real",
                        font_size=24, bold=True
                    ),
                    # Cards — última linha
                    _visual_card(x=20,  y=90, w=180, h=110, measure="[Preço Atual]",       table="ResumoDiario",  title="Preço Atual"),
                    _visual_card(x=220, y=90, w=180, h=110, measure="[Retorno Dia %]",     table="ResumoDiario",  title="Retorno Dia"),
                    _visual_card(x=420, y=90, w=180, h=110, measure="[Volatilidade 20d %]",table="ResumoDiario",  title="Volatilidade 20d"),
                    _visual_card(x=620, y=90, w=180, h=110, measure="[Qtd Bull]",          table="SinaisTecnicos",title="Ativos BULL"),
                    _visual_card(x=820, y=90, w=180, h=110, measure="[Qtd Bear]",          table="SinaisTecnicos",title="Ativos BEAR"),
                    _visual_card(x=1020,y=90, w=180, h=110, measure="[Melhor Ação]",       table="RankingAcoes",  title="Melhor Ação"),
                    # Tabela sinais técnicos
                    _visual_table(
                        x=20, y=220, w=600, h=400,
                        table="SinaisTecnicos",
                        columns=["ticker", "fechamento", "sinal_medias", "posicao_medias", "tendencia_longo_prazo"],
                        title="Sinais Técnicos"
                    ),
                    # Tabela ranking
                    _visual_table(
                        x=640, y=220, w=600, h=400,
                        table="RankingAcoes",
                        columns=["ranking_retorno", "ticker", "retorno_periodo_pct", "pct_dias_alta", "sharpe_proxy"],
                        title="Ranking de Performance"
                    ),
                ],
                "config": json.dumps({
                    "defaultFilterActionIsDataFilter": True,
                    "filters": "[]"
                })
            },
            {
                "id": 1,
                "name": "Performance Mensal",
                "displayName": "📅 Performance Mensal",
                "ordinal": 1,
                "visualContainers": [
                    _visual_text_box(
                        x=20, y=10, w=1240, h=60,
                        text="Performance Mensal por Ativo",
                        font_size=20, bold=True
                    ),
                    _visual_card(x=20,  y=90, w=220, h=110, measure="[Retorno Mensal Médio %]", table="PerformanceMensal", title="Retorno Médio"),
                    _visual_card(x=260, y=90, w=220, h=110, measure="[Melhor Mês %]",           table="PerformanceMensal", title="Melhor Mês"),
                    _visual_card(x=500, y=90, w=220, h=110, measure="[Pior Mês %]",             table="PerformanceMensal", title="Pior Mês"),
                    _visual_card(x=740, y=90, w=220, h=110, measure="[Volume Total (Mi)]",      table="PerformanceMensal", title="Volume Total (Mi)"),
                    _visual_clustered_bar(
                        x=20, y=220, w=1240, h=400,
                        table="PerformanceMensal",
                        axis="ano_mes",
                        values="retorno_mensal_pct",
                        legend="ticker",
                        title="Retorno Mensal por Ativo (%)"
                    ),
                ],
                "config": json.dumps({
                    "defaultFilterActionIsDataFilter": True,
                    "filters": "[]"
                })
            },
            {
                "id": 2,
                "name": "Detalhes Diários",
                "displayName": "📈 Detalhes Diários",
                "ordinal": 2,
                "visualContainers": [
                    _visual_text_box(
                        x=20, y=10, w=1240, h=60,
                        text="Detalhes do Último Pregão",
                        font_size=20, bold=True
                    ),
                    _visual_table(
                        x=20, y=90, w=1240, h=500,
                        table="ResumoDiario",
                        columns=["ticker", "ultima_data", "abertura", "alta", "baixa",
                                 "fechamento", "volume", "retorno_diario_pct",
                                 "sma_7", "sma_20", "sma_50", "volatilidade_20d_pct",
                                 "direcao_dia"],
                        title="Cotações — Último Pregão"
                    ),
                ],
                "config": json.dumps({
                    "defaultFilterActionIsDataFilter": True,
                    "filters": "[]"
                })
            }
        ],
        "config": json.dumps({
            "version": "5.54",
            "themeCollection": {
                "baseTheme": {
                    "name": "CY24SU10",
                    "version": "5.54",
                    "type": 2
                }
            }
        }),
        "filters": "[]",
        "theme": "CY24SU10",
        "customTheme": {},
        "resourcePackages": []
    }


# ─── Helpers de visuais ───────────────────────────────────────────────────────

def _base_visual(visual_type, x, y, w, h):
    return {
        "x": x, "y": y, "z": 1,
        "width": w, "height": h,
        "config": json.dumps({"name": f"v_{x}_{y}", "layouts": [{"id": 0, "position": {"x": x, "y": y, "z": 1, "width": w, "height": h, "tabOrder": 1000}}], "singleVisual": {"visualType": visual_type, "projectionActiveItems": {}}}),
        "filters": "[]",
        "query": "{}",
        "dataTransforms": "{}"
    }

def _visual_text_box(x, y, w, h, text, font_size=14, bold=False):
    v = _base_visual("textbox", x, y, w, h)
    cfg = json.loads(v["config"])
    cfg["singleVisual"]["objects"] = {
        "general": [{"properties": {"paragraphs": [{"textRuns": [{"value": text, "textStyle": {"fontWeight": "bold" if bold else "normal", "fontSize": f"{font_size}pt"}}], "horizontalTextAlignment": "left"}]}}]
    }
    v["config"] = json.dumps(cfg)
    return v

def _visual_card(x, y, w, h, measure, table, title):
    v = _base_visual("card", x, y, w, h)
    cfg = json.loads(v["config"])
    cfg["singleVisual"]["prototypeQuery"] = {
        "Version": 2,
        "From": [{"Name": "t", "Entity": table, "Type": 0}],
        "Select": [{"Measure": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": measure.strip("[]")}, "Name": f"t.{measure.strip('[]')}"}]
    }
    cfg["singleVisual"]["objects"] = {
        "title": [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}, "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}, "fontSize": {"expr": {"Literal": {"Value": "11D"}}}}}]
    }
    v["config"] = json.dumps(cfg)
    return v

def _visual_table(x, y, w, h, table, columns, title):
    v = _base_visual("tableEx", x, y, w, h)
    cfg = json.loads(v["config"])
    select = [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": col}, "Name": f"t.{col}"} for col in columns]
    cfg["singleVisual"]["prototypeQuery"] = {
        "Version": 2,
        "From": [{"Name": "t", "Entity": table, "Type": 0}],
        "Select": select
    }
    cfg["singleVisual"]["objects"] = {
        "title": [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}, "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}}}]
    }
    v["config"] = json.dumps(cfg)
    return v

def _visual_clustered_bar(x, y, w, h, table, axis, values, legend, title):
    v = _base_visual("clusteredBarChart", x, y, w, h)
    cfg = json.loads(v["config"])
    cfg["singleVisual"]["prototypeQuery"] = {
        "Version": 2,
        "From": [{"Name": "t", "Entity": table, "Type": 0}],
        "Select": [
            {"Column":  {"Expression": {"SourceRef": {"Source": "t"}}, "Property": axis},   "Name": f"t.{axis}"},
            {"Measure": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": values}, "Name": f"t.{values}"},
            {"Column":  {"Expression": {"SourceRef": {"Source": "t"}}, "Property": legend}, "Name": f"t.{legend}"},
        ]
    }
    cfg["singleVisual"]["objects"] = {
        "title": [{"properties": {"show": {"expr": {"Literal": {"Value": "true"}}}, "text": {"expr": {"Literal": {"Value": f"'{title}'"}}}}}]
    }
    v["config"] = json.dumps(cfg)
    return v


# ──────────────────────────────────────────────────────────────────────────────
# CONNECTIONS
# ──────────────────────────────────────────────────────────────────────────────
def build_connections():
    return {
        "Version": 3,
        "Connections": [
            {
                "Name": "Databricks_B3",
                "ConnectionString": (
                    f"Provider=MSOLEDBSQL;"
                    f"Data Source={SERVER};"
                    f"Initial Catalog={CATALOG};"
                    f"HTTPPath={HTTP_PATH};"
                ),
                "ConnectionType": "pbiServiceLive",
                "PbiServiceModelId": 0,
                "PbiModelDatabaseName": "SemanticModel"
            }
        ]
    }

# ──────────────────────────────────────────────────────────────────────────────
# GERAR .PBIT
# ──────────────────────────────────────────────────────────────────────────────
output_path = os.path.join(os.path.dirname(__file__), OUTPUT)

with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("[Content_Types].xml",  CONTENT_TYPES)
    zf.writestr("Version",              json.dumps({"version": "5.54"}))
    zf.writestr("DataModelSchema",      json.dumps(build_data_model(),    ensure_ascii=False, indent=2))
    zf.writestr("DiagramLayout",        json.dumps(build_diagram_layout(), ensure_ascii=False, indent=2))
    zf.writestr("Report/Layout",        json.dumps(build_report_layout(),  ensure_ascii=False, indent=2))
    zf.writestr("SecurityBindings",     json.dumps({}))
    zf.writestr("Connections",          json.dumps(build_connections(),   ensure_ascii=False, indent=2))

print(f"[OK] Template gerado: {output_path}")
print(f"\nTabelas incluidas:")
for _, src, name in TABELAS:
    print(f"   {name} <- gold_cotacoes.{src}")
print(f"\nMedidas DAX criadas: {len(MEDIDAS)}")
print(f"\nPaginas do relatorio:")
print(f"   - Visao Geral  : cards KPI + sinais tecnicos + ranking")
print(f"   - Performance Mensal : grafico de barras por mes/ativo")
print(f"   - Detalhes Diarios   : tabela completa do ultimo pregao")
print(f"\nProximo passo:")
print(f"   1. Abra '{OUTPUT}' no Power BI Desktop")
print(f"   2. Insira o Personal Access Token do Databricks quando solicitado")
print(f"   3. Clique em Atualizar para carregar os dados")
