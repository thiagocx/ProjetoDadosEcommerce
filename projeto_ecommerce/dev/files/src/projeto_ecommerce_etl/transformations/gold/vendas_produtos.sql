-- gold.vendas_produtos
-- Tabela de vendas por produto para a Diretoria Comercial.
-- Uma linha por produto vendido (LEFT JOIN de silver.vendas com silver.produtos).
-- Periodo de dados: 13/12/2025 a 11/01/2026.
--
-- Comentarios das colunas:
--   id_produto: identificador do produto (chave primaria)
--   nome_produto: nome do produto (contar por id_produto, pois produtos diferentes podem ter o mesmo nome)
--   categoria: categoria do produto (ou 'Não cadastrado' quando nao existir em silver.produtos)
--   marca: marca do produto (ou 'Não cadastrado' quando nao existir)
--   faixa_preco: faixa de preco do produto (PREMIUM, MEDIO ou BASICO; ou 'Não cadastrado')
--   produto_cadastrado: verdadeiro se produto existe em silver.produtos, falso caso contrario
--   total_vendas: numero total de vendas deste produto no periodo
--   itens_vendidos: soma da quantidade vendida deste produto
--   receita: receita total gerada por este produto
--   ticket_medio: ROUND(AVG(receita), 2) - cuidado: nao e a media da quantidade, mas da receita por venda
--   ranking_receita: ROW_NUMBER geral por receita desc (entre todos os produtos)
--   ranking_na_categoria: ROW_NUMBER particionado por categoria, ordenado por receita desc
--
-- Avisos para o Genie:
--   - nome_produto: produtos diferentes podem ter o mesmo nome; contar e filtrar por id_produto
--   - produto nao cadastrado: quando id_produto nao existir em silver.produtos, usar
--     'Produto nao cadastrado' para nome_produto e categoria e 'Não cadastrado' para marca
--   - faixa_preco: para produtos nao cadastrados, o valor sera 'Não cadastrado' (nao aplica as faixas PREMIUM/MEDIO/BASICO)
CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_produtos AS
WITH produto_info AS (
    SELECT
        p.id_produto,
        s.quantidade,
        s.receita,
        -- Nome do produto: se nao existir, usar 'Produto nao cadastrado'
        CASE
            WHEN p.id_produto IS NULL THEN 'Produto nao cadastrado'
            ELSE initcap(trim(lower(nome_produto)))
        END AS nome_produto,
        -- Categoria: se nao existir, 'Não cadastrado'
        CASE
            WHEN p.id_produto IS NULL THEN 'Não cadastrado'
            ELSE coalesce(categoria, 'Não cadastrado')
        END AS categoria,
        -- Marca: se nao existir, 'Não cadastrado'
        CASE
            WHEN p.id_produto IS NULL THEN 'Não cadastrado'
            ELSE coalesce(marca, 'Não cadastrado')
        END AS marca,
        -- Faixa de preco: se produto existir, usar a da silver.produtos; se nao, 'Não cadastrado'
        CASE
            WHEN p.id_produto IS NULL THEN 'Não cadastrado'
            ELSE coalesce(faixa_preco, 'Não cadastrado')
        END AS faixa_preco,
        -- Flag se produto esta cadastrado
        CASE
            WHEN p.id_produto IS NOT NULL THEN True
            ELSE False
        END AS produto_cadastrado
    FROM silver.vendas s
    LEFT JOIN silver.produtos p ON s.id_produto = p.id_produto
),
vendas_agregadas AS (
    SELECT
        id_produto,
        nome_produto,
        categoria,
        marca,
        faixa_preco,
        produto_cadastrado,
        COUNT(*) AS total_vendas,
        SUM(quantidade) AS itens_vendidos,
        SUM(receita) AS receita,
        ROUND(AVG(receita), 2) AS ticket_medio
    FROM produto_info
    GROUP BY
        id_produto,
        nome_produto,
        categoria,
        marca,
        faixa_preco,
        produto_cadastrado
),
-- Calculos de ranking
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY receita DESC) AS ranking_receita,
        ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY receita DESC) AS ranking_na_categoria
    FROM vendas_agregadas
)
SELECT
    id_produto,
    nome_produto,
    categoria,
    marca,
    faixa_preco,
    produto_cadastrado,
    total_vendas,
    itens_vendidos,
    receita,
    ticket_medio,
    ranking_receita,
    ranking_na_categoria
FROM ranked;