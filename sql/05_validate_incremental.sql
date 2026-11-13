-- ============================================================
-- 05 · Validate the incremental pipeline
-- Source must be Delta for spark.readStream.table() to work.
-- ============================================================

-- Confirm the source format is "delta"
DESCRIBE DETAIL workspace.default.marketing_vendas_10000_linhas;

-- Row counts across the incremental layers
-- Expected: Bronze 12,000 | Silver 12,000 (minus rows dropped by expectations) | Gold = # of channels
SELECT
    (SELECT COUNT(*) FROM workspace.default.marketing_bronze_incremental) AS bronze,
    (SELECT COUNT(*) FROM workspace.default.marketing_silver_incremental) AS silver,
    (SELECT COUNT(*) FROM workspace.default.marketing_gold_incremental)   AS gold;
