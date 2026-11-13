-- ============================================================
-- 02 · Marketing KPIs per channel (Gold layer)
-- Which channels bring the most revenue and the best ROAS?
-- ============================================================
SELECT
    canal,
    ROUND(receita_total, 2)      AS receita,
    ROUND(investimento_total, 2) AS investimento,
    total_pedidos,
    roas
FROM workspace.default.marketing_gold
ORDER BY receita_total DESC;
