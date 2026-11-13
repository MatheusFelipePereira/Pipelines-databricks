"""
Marketing & Sales — Incremental pipeline (Streaming Tables + Materialized View)

Lakeflow Declarative Pipelines (Databricks), PySpark Structured Streaming.
Source : workspace.default.marketing_vendas_10000_linhas (Delta, append-only)
Targets: marketing_bronze_incremental  -> Streaming Table
         marketing_silver_incremental  -> Streaming Table + Expectations
         marketing_gold_incremental    -> Materialized View

Each run ingests only the rows appended to the source since the last checkpoint
(e.g. the +2,000 rows inserted in sql/04_incremental_insert.sql).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.default.marketing_vendas_10000_linhas"


# ---------------------------------------------------------------------------
# BRONZE — incremental ingestion
# @dp.table + spark.readStream => Streaming Table with checkpoint control.
# ---------------------------------------------------------------------------
@dp.table(name="marketing_bronze_incremental")
def marketing_bronze_incremental():
    return spark.readStream.table(SOURCE_TABLE)


# ---------------------------------------------------------------------------
# SILVER — incremental cleaning + data quality rules
# dropDuplicates() was removed: global dedup does not behave the same way in
# a continuous stream (incremental dedup is a planned next step).
# ---------------------------------------------------------------------------
@dp.table(name="marketing_silver_incremental")
@dp.expect_or_drop("cliques_validos", "cliques >= 0")
@dp.expect_or_drop("receita_valida", "receita_brl >= 0")
def marketing_silver_incremental():
    return (
        spark.readStream.table("marketing_bronze_incremental")
        .withColumn("data", F.to_date("data"))
        .withColumn("impressoes", F.col("impressoes").cast("int"))
        .withColumn("cliques", F.col("cliques").cast("int"))
        .withColumn("leads", F.col("leads").cast("int"))
        .withColumn("pedidos", F.col("pedidos").cast("int"))
        .withColumn("investimento_brl", F.col("investimento_brl").cast("double"))
        .withColumn("receita_brl", F.col("receita_brl").cast("double"))
        .filter(F.col("data").isNotNull())
    )


# ---------------------------------------------------------------------------
# GOLD — KPIs per channel over the full Silver history
# Aggregations over all rows => Materialized View (batch read).
# ---------------------------------------------------------------------------
@dp.materialized_view(name="marketing_gold_incremental")
def marketing_gold_incremental():
    return (
        spark.read.table("marketing_silver_incremental")
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
