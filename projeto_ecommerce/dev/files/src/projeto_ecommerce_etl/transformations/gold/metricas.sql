-- gold.vendas_totais
-- Agregação de métricas totais de vendas para a camada gold.
-- Esta tabela fornece os KPIs principais do e-commerce.
CREATE OR REPLACE MATERIALIZED VIEW gold.vendas_totais AS
SELECT
    COUNT(*) AS total_vendas,
    SUM(receita) AS receita_total,
    SUM(CASE WHEN produto_cadastrado = FALSE THEN 1 ELSE 0 END) AS vendas_produto_nao_cadastrado,
    SUM(CASE WHEN venda_antes_do_cadastro = TRUE THEN 1 ELSE 0 END) AS vendas_antes_do_cadastro,
    AVG(receita) AS ticket_medio
FROM silver.vendas;

-- gold.clientes_por_regiao
-- Distribuição de clientes por região geográfica (Brasil - 27 UFs).
CREATE OR REPLACE MATERIALIZED VIEW gold.clientes_por_regiao AS
SELECT
    regiao,
    COUNT(*) AS total_clientes,
    COUNT(DISTINCT estado) AS ufs_presentes
FROM silver.clientes
WHERE regiao IS NOT NULL
GROUP BY regiao
ORDER BY total_clientes DESC;

-- gold.produtos_por_faixa_preco
-- Distribuição de produtos por faixa de preço (PREMIUM, MEDIO, BASICO).
CREATE OR REPLACE MATERIALIZED VIEW gold.produtos_por_faixa_preco AS
SELECT
    faixa_preco,
    COUNT(*) AS total_produtos,
    AVG(preco_atual) AS preco_medio_faixa,
    MIN(preco_atual) AS min_preco_faixa,
    MAX(preco_atual) AS max_preco_faixa
FROM silver.produtos
GROUP BY faixa_preco
ORDER BY
    CASE faixa_preco
        WHEN 'PREMIUM' THEN 1
        WHEN 'MEDIO' THEN 2
        WHEN 'BASICO' THEN 3
    END;

-- gold.preco_competitivo_resumo
-- Resumo dos preços de concorrentes com indicação de suspeitos.
CREATE OR REPLACE MATERIALIZED VIEW gold.preco_competitivo_resumo AS
SELECT
    COUNT(*) AS total_precos_competidores,
    SUM(CASE WHEN preco_suspeito = TRUE THEN 1 ELSE 0 END) AS precos_suspeitos,
    AVG(preco_concorrente) AS preco_medio_concorrente,
    MIN(preco_concorrente) AS min_preco_concorrente,
    MAX(preco_concorrente) AS max_preco_concorrente
FROM silver.preco_competidores;