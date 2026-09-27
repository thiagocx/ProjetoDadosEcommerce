-- gold.precos_competitividade
-- Tabela de competitividade de precos para a Diretoria de Pricing.
-- Uma linha por produto que tem preco de concorrente (JOIN de silver.produtos com a agregacao
-- de silver.preco_competidores; LEFT JOIN com a receita de silver.vendas).
-- Periodo de dados: 13/12/2025 a 11/01/2026.

CREATE OR REFRESH MATERIALIZED VIEW gold.precos_competitividade AS
SELECT
    p.id_produto,
    p.nome_produto,
    p.categoria,
    p.marca,
    p.nosso_preco,
    p.preco_medio_concorrentes,
    p.preco_minimo_concorrentes,
    p.preco_maximo_concorrentes,
    p.total_concorrentes,
    p.diferenca_pct_vs_media,
    p.diferenca_pct_vs_minimo,
    c.classificacao_preco,
    p.possui_preco_suspeito,
    p.receita,
    p.itens_vendidos
FROM (
    SELECT
        n.id_produto,
        CASE
            WHEN n.id_produto IS NULL THEN 'Produto nao cadastrado'
            ELSE initcap(trim(lower(n.nome_produto)))
        END AS nome_produto,
        CASE
            WHEN n.id_produto IS NULL THEN 'Não cadastrado'
            ELSE coalesce(n.categoria, 'Não cadastrado')
        END AS categoria,
        CASE
            WHEN n.id_produto IS NULL THEN 'Não cadastrado'
            ELSE coalesce(n.marca, 'Não cadastrado')
        END AS marca,
        COALESCE(ns.preco_atual, 0) AS nosso_preco,
        ROUND(AVG(pc.preco_concorrente), 2) AS preco_medio_concorrentes,
        MIN(pc.preco_concorrente) AS preco_minimo_concorrentes,
        MAX(pc.preco_concorrente) AS preco_maximo_concorrentes,
        COUNT(DISTINCT pc.nome_concorrente) AS total_concorrentes,
        ROUND(
            CASE
                WHEN AVG(pc.preco_concorrente) = 0 THEN 0
                ELSE ((COALESCE(ns.preco_atual, 0) - ROUND(AVG(pc.preco_concorrente), 2)) / ROUND(AVG(pc.preco_concorrente), 2) * 100)
            END, 2
        ) AS diferenca_pct_vs_media,
        ROUND(
            CASE
                WHEN MIN(pc.preco_concorrente) = 0 THEN 0
                ELSE ((COALESCE(ns.preco_atual, 0) - MIN(pc.preco_concorrente)) / MIN(pc.preco_concorrente) * 100)
            END, 2
        ) AS diferenca_pct_vs_minimo,
        MAX(pc.preco_suspeito) AS possui_preco_suspeito,
        COALESCE(v.receita, 0) AS receita,
        COALESCE(v.itens_vendidos, 0) AS itens_vendidos
    FROM silver.produtos n
    LEFT JOIN silver.preco_competidores pc ON n.id_produto = pc.id_produto
    LEFT JOIN (
        SELECT id_produto, SUM(receita) AS receita, SUM(quantidade) AS itens_vendidos
        FROM silver.vendas
        WHERE data BETWEEN DATE '2025-12-13' AND DATE '2026-01-11'
        GROUP BY id_produto
    ) v ON n.id_produto = v.id_produto
    LEFT JOIN silver.produtos ns ON n.id_produto = ns.id_produto
    GROUP BY n.id_produto, n.nome_produto, n.categoria, n.marca, ns.preco_atual, v.receita, v.itens_vendidos
) p
LEFT JOIN (
    SELECT
        n.id_produto,
        COALESCE(ns.preco_atual, 0) AS nosso_preco,
        ROUND(AVG(pc.preco_concorrente), 2) AS preco_medio_concorrentes,
        MIN(pc.preco_concorrente) AS preco_minimo_concorrentes,
        MAX(pc.preco_concorrente) AS preco_maximo_concorrentes,
        COUNT(DISTINCT pc.nome_concorrente) AS total_concorrentes,
        CASE
            WHEN COUNT(DISTINCT pc.nome_concorrente) = 0 THEN 'NA_MEDIA'
            WHEN COALESCE(ns.preco_atual, 0) > MAX(pc.preco_concorrente) THEN 'MAIS_CARO_QUE_TODOS'
            WHEN COALESCE(ns.preco_atual, 0) < MIN(pc.preco_concorrente) THEN 'MAIS_BARATO_QUE_TODOS'
            WHEN ABS((COALESCE(ns.preco_atual, 0) - ROUND(AVG(pc.preco_concorrente), 2)) / ROUND(AVG(pc.preco_concorrente), 2)) < 0.05 THEN 'NA_MEDIA'
            WHEN COALESCE(ns.preco_atual, 0) > ROUND(AVG(pc.preco_concorrente), 2) THEN 'MAIS_CARO_QUE_MEDIA'
            ELSE 'MAIS_BARATO_QUE_MEDIA'
        END AS classificacao_preco
    FROM silver.produtos n
    LEFT JOIN silver.preco_competidores pc ON n.id_produto = pc.id_produto
    LEFT JOIN silver.produtos ns ON n.id_produto = ns.id_produto
    GROUP BY n.id_produto, ns.preco_atual
) c ON p.id_produto = c.id_produto