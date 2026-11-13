-- ============================================================
-- 01 · Row counts per Medallion layer (batch pipeline)
-- Expected: Bronze = 10,000 | Silver <= 10,000 | Gold = # of channels
-- Silver may be smaller: dropDuplicates(), null dates and expectations.
-- ============================================================
SELECT 'Bronze' AS camada, COUNT(*) AS registros
FROM workspace.default.marketing_bronze

UNION ALL

SELECT 'Silver', COUNT(*)
FROM workspace.default.marketing_silver

UNION ALL

SELECT 'Gold', COUNT(*)
FROM workspace.default.marketing_gold;
