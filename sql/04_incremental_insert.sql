-- ============================================================
-- 04 · Simulate new data arriving (incremental load test)
-- 1) Upload the 2,000-row CSV as a NEW table (do not replace the original)
-- 2) Append it to the Delta source monitored by the incremental pipeline
-- ⚠️ Run the INSERT only once — running it again duplicates the 2,000 rows.
-- ============================================================

-- Check the staging table (expected: 2,000)
SELECT COUNT(*)
FROM workspace.default.marketing_vendas_novos_2000_registros;

-- Append by column name (independent of column order)
INSERT INTO workspace.default.marketing_vendas_10000_linhas
BY NAME
SELECT *
FROM workspace.default.marketing_vendas_novos_2000_registros;

-- Check the source (expected: 12,000)
SELECT COUNT(*)
FROM workspace.default.marketing_vendas_10000_linhas;
