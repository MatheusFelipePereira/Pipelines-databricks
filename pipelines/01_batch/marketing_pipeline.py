"""
Marketing & Sales — Batch pipeline (Medallion: Bronze -> Silver -> Gold)

Lakeflow Declarative Pipelines (Databricks), PySpark.
Source : workspace.default.marketing_vendas_10000_linhas (Delta table created from a CSV upload)
Targets: workspace.default.marketing_bronze
         workspace.default.marketing_silver
         workspace.default.marketing_gold

Status: executed successfully, layers validated with SQL (see /sql).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.default.marketing_vendas_10000_linhas"


# ---------------------------------------------------------------------------
# BRONZE — raw data, as it arrives
# Reads a registered table (catalog.schema.table) with spark.read.table().
# spark.read.csv()/.load() would require an absolute file path (/Volumes/...).
# ---------------------------------------------------------------------------
@dp.materialized_view(name="marketing_bronze")
def marketing_bronze():
    return spark.read.table(SOURCE_TABLE)


# ---------------------------------------------------------------------------
# SILVER — typed, cleaned and validated data
# Expectations drop rows that break business rules (negative clicks/revenue).
# ---------------------------------------------------------------------------
@dp.materialized_view(name="marketing_silver")
@dp.expect_or_drop("cliques_validos", "cliques >= 0")
@dp.expect_or_drop("receita_valida", "receita_brl >= 0")
def marketing_silver():
    return (
        spark.read.table("marketing_bronze")
        .withColumn("data", F.to_date("data"))
        .withColumn("impressoes", F.col("impressoes").cast("int"))
        .withColumn("cliques", F.col("cliques").cast("int"))
        .withColumn("leads", F.col("leads").cast("int"))
        .withColumn("pedidos", F.col("pedidos").cast("int"))
        .withColumn("investimento_brl", F.col("investimento_brl").cast("double"))
        .withColumn("receita_brl", F.col("receita_brl").cast("double"))
        .dropDuplicates()
        .filter(F.col("data").isNotNull())
    )


# ---------------------------------------------------------------------------
# GOLD — business KPIs per marketing channel (revenue, spend, orders, ROAS)
# ---------------------------------------------------------------------------
@dp.materialized_view(name="marketing_gold")
def marketing_gold():
    return (
        spark.read.table("marketing_silver")
        .groupBy("canal")
        .agg(
            F.sum("receita_brl").alias("receita_total"),
            F.sum("investimento_brl").alias("investimento_total"),
            F.sum("pedidos").alias("total_pedidos"),
            F.sum("cliques").alias("total_cliques"),
        )
        .withColumn(
            "roas",
            F.when(
                F.col("investimento_total") > 0,
                F.round(F.col("receita_total") / F.col("investimento_total"), 2),
            ),
        )
    )
