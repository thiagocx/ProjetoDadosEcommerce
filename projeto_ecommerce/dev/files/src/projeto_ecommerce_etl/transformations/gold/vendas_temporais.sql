-- gold.vendas_temporais
-- Tabela de vendas temporais para a Diretoria Comercial.
-- Uma linha por data × hora × canal_venda.
-- Periodo de dados: 13/12/2025 a 11/01/2026.
--
-- Comentarios das colunas (essenciais para o Genie):
--   data: data da venda (intervalo 13/12/2025 a 11/01/2026)
--   dia_semana_num: numero do dia da semana (1=domingo ... 7=sabado)
--   dia_semana: nome em português (Domingo, Segunda, Terca, Quarta, Quinta, Sexta, Sábado)
--   hora: hora da venda (0-23)
--   canal_venda: canal da venda ('ecommerce' ou 'loja_fisica')
--   total_vendas: numero total de vendas neste periodo x hora x canal
--   itens_vendidos: soma da quantidade de itens vendidos
--   receita: soma da receita total deste grupo
--   clientes_unicos: COUNT DISTINCT id_cliente neste grupo
--   Aviso: nao somar clientes_unicos entre linhas; para obter clientes unicos no periodo
--   usar a tabela gold.clientes_segmentacao. Esta coluna representa clientes unicos
--   APENAS naquele grupo data x hora x canal, nao no periodo total.
CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_temporais AS
SELECT
    data,
    dia_semana_num,
    dia_semana,
    hora,
    canal_venda,
    COUNT(*) AS total_vendas,
    SUM(quantidade) AS itens_vendidos,
    SUM(receita) AS receita,
    COUNT(DISTINCT id_cliente) AS clientes_unicos
FROM silver.vendas
WHERE data BETWEEN DATE '2025-12-13' AND DATE '2026-01-11'
GROUP BY
    data,
    dia_semana_num,
    dia_semana,
    hora,
    canal_venda;