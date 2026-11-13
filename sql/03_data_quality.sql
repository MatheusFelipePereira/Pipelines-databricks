-- ============================================================
-- 03 · Data quality checks on the Silver layer
-- A pipeline that runs without errors is not the same as correct data.
-- ============================================================
SELECT
    COUNT(*)                         AS total_registros,
    COUNT_IF(data IS NULL)           AS datas_nulas,
    COUNT_IF(receita_brl IS NULL)    AS receitas_nulas,
    COUNT_IF(investimento_brl IS NULL) AS investimentos_nulos
FROM workspace.default.marketing_silver;
