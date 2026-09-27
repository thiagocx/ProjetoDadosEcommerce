-- gold.clientes_segmentacao
-- Tabela de segmentação de clientes para a Diretoria de Customer Success.
-- Inclui todos os clientes (inclusive quem nunca comprou) a partir de silver.clientes.
-- Periodo de dados: 13/12/2025 a 11/01/2026.
--
-- Segmentacao de clientes:
--   VIP:      receita a partir de R$ 22.000
--   TOP_TIER: de R$ 17.000 ate R$ 21.999,99
--   REGULAR:  abaixo de R$ 17.000
--
-- Limites antigos (R$ 10.000 e R$ 5.000) nao serviam: com eles quase todo mundo
-- virava VIP, nao diferenciando os niveis de valor para o time de ativação.
--
-- Comentarios de coluna sao essenciais para o Genie (IA que escreve SQL em portugues).
CREATE OR REFRESH MATERIALIZED VIEW gold.clientes_segmentacao AS
WITH compras_cliente AS (
    -- Agrega todas as compras por cliente a partir de silver.vendas
    SELECT
        id_cliente,
        COUNT(DISTINCT id_venda) AS total_compras,
        COALESCE(SUM(receita), 0) AS receita,
        ROUND(AVG(receita), 2) AS ticket_medio,
        MIN(data_venda) AS primeira_compra,
        MAX(data_venda) AS ultima_compra
    FROM silver.vendas
    WHERE data BETWEEN DATE '2025-12-13' AND DATE '2026-01-11'
    GROUP BY id_cliente
),
-- Junta com silver.clientes para ter todos os clientes, inclusive os que nunca compraram
clientes_compras AS (
    SELECT
        c.id_cliente,
        c.nome_cliente,
        c.estado,
        c.nome_estado,
        c.regiao,
        COALESCE(cc.total_compras, 0) AS total_compras,
        COALESCE(cc.receita, 0) AS receita,
        cc.ticket_medio,
        cc.primeira_compra,
        cc.ultima_compra,
        -- Segmentacao baseada nos limites definidos com a diretoria
        CASE
            WHEN COALESCE(cc.receita, 0) >= 22000 THEN 'VIP'
            WHEN COALESCE(cc.receita, 0) >= 17000 THEN 'TOP_TIER'
            ELSE 'REGULAR'
        END AS segmento_cliente,
        -- Ranking por receita (decisao desc)
        ROW_NUMBER() OVER (ORDER BY COALESCE(cc.receita, 0) DESC) AS ranking_receita
    FROM silver.clientes c
    LEFT JOIN compras_cliente cc ON c.id_cliente = cc.id_cliente
)
SELECT
    id_cliente,
    -- Remove pronome de tratamento do nome (mesma logica do silver)
    CASE
        WHEN nome_cliente rlike '^(Sr|Sra|Srta|Dr|Dra)\\s*\\.' then regexp_replace(nome_cliente, '^(Sr|Sra|Srta|Dr|Dra)\\s*\\.', '')
        ELSE nome_cliente
    END AS nome_cliente,
    estado,
    nome_estado,
    regiao,
    total_compras,
   receita,
    ticket_medio,
    primeira_compra,
    ultima_compra,
    segmento_cliente,
    ranking_receita
FROM clientes_compras;