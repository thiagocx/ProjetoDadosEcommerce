-- gold.vendas_detalhadas
-- Tabela de vendas detalhadas para a Diretoria Comercial e cruces com outras diretorias.
-- Uma linha por venda, para perguntas que cruzam diretorias ("receita por regiao e categoria",
-- "canal preferido dos VIPs") e para os filtros cruzados do dashboard.
-- Periodo de dados: 13/12/2025 a 11/01/2026.
-- CLUSTER BY (data) para otimizar queries por faixa de data.
--
-- Comentarios das colunas:
--   id_venda: identificador unico da venda
--   data_venda: data original da venda (pode diferir de 'data' por ajustes de fuso horario)
--   data: data padronizada da venda (intervalo 13/12/2025 a 11/01/2026)
--   dia_semana_num: numero do dia da semana (1=domingo ... 7=sabado)
--   dia_semana: nome em portugues (Domingo, Segunda, Terca, Quarta, Quinta, Sexta, Sábado)
--   hora: hora da venda (0-23)
--   canal_venda: canal da venda ('ecommerce' ou 'loja_fisica')
--   id_produto: identificador do produto da venda
--   nome_produto: nome do produto (sem pronome de tratamento; 'Produto nao cadastrado' se nao existir)
--   categoria: categoria do produto ('Não cadastrado' se nao existir)
--   marca: marca do produto ('Não cadastrado' se nao existir)
--   faixa_preco: faixa de preco (PREMIUM, MEDIO, BASICO ou 'Não cadastrado')
--   id_cliente: identificador do cliente da venda
--   nome_cliente: nome do cliente (sem pronome de tratamento)
--   estado: UF do cliente em maiusculas
--   nome_estado: nome completo do estado
--   regiao: regiao do Brasil (Norte, Nordeste, Centro-Oeste, Sudeste, Sul)
--   segmento_cliente: segmento (VIP, TOP_TIER, REGULAR) de gold.clientes_segmentacao
--   quantidade: quantidade de itens na venda
--   preco_unitario: preco unitario da venda (DECIMAL(10,2))
--   receita: receita total desta venda (quantidade * preco_unitario)
--   produto_cadastrado: verdadeiro se produto existe em silver.produtos, falso caso contrario
--   venda_antes_do_cadastro: verdadeiro se data_venda anterior a data de criacao do produto
--
-- Avisos para o Genie:
--   - Toda venda desta tabela deve ter segmento e regio preenchidos (via LEFT JOIN com
--     gold.clientes_segmentacao e silver.produtos). Caso algum registro esteja sem
--     essas informacoes, verificar a integridade das chaves em silver.cliente e silver.produto.
CREATE OR REFRESH MATERIALIZED VIEW gold.vendas_detalhadas
CLUSTER BY (data) AS
SELECT
    s.id_venda,
    s.data_venda,
    s.data,
    s.dia_semana_num,
    s.dia_semana,
    s.hora,
    s.canal_venda,
    s.id_produto,
    -- nome_produto: usa 'Produto nao cadastrado' se produto nao existir
    CASE
        WHEN p.id_produto IS NULL THEN 'Produto nao cadastrado'
        ELSE initcap(trim(lower(nome_produto)))
    END AS nome_produto,
    -- categoria: 'Não cadastrado' se produto nao existir
    CASE
        WHEN p.id_produto IS NULL THEN 'Não cadastrado'
        ELSE coalesce(categoria, 'Não cadastrado')
    END AS categoria,
    -- marca: 'Não cadastrado' se produto nao existir
    CASE
        WHEN p.id_produto IS NULL THEN 'Não cadastrado'
        ELSE coalesce(marca, 'Não cadastrado')
    END AS marca,
    -- faixa_preco: 'Não cadastrado' se produto nao existir
    CASE
        WHEN p.id_produto IS NULL THEN 'Não cadastrado'
        ELSE coalesce(faixa_preco, 'Não cadastrado')
    END AS faixa_preco,
    -- id_cliente e dados do cliente (via LEFT JOIN com clientes_segmentacao para segmento e regiao)
    s.id_cliente,
    -- nome_cliente sem pronome de tratamento
    CASE
        WHEN c.nome_cliente rlike '^(Sr|Sra|Srta|Dr|Dra)\\s*\\.' then regexp_replace(c.nome_cliente, '^(Sr|Sra|Srta|Dr|Dra)\\s*\\.', '')
        ELSE c.nome_cliente
    END AS nome_cliente,
    -- estado, nome_estado, regiao, segmento_cliente vindo da segmentacao de clientes
    c.estado,
    c.nome_estado,
    c.regiao,
    c.segmento_cliente,
    s.quantidade,
    s.preco_unitario,
    s.receita,
    -- produto_cadastrado
    CASE
        WHEN p.id_produto IS NOT NULL THEN True
        ELSE False
    END AS produto_cadastrado,
    -- venda_antes_do_cadastro
    CASE
        WHEN s.data_venda < p.data_criacao THEN True
        ELSE False
    END AS venda_antes_do_cadastro
FROM silver.vendas s
-- Left join para trazer dados do produto (se nao existir, usamos 'Produto nao cadastrado')
LEFT JOIN silver.produtos p ON s.id_produto = p.id_produto
-- Left join para trazer dados do cliente e sua segmentacao
-- Usamos gold.clientes_segmentacao para ter segmento e regiao garantidos
LEFT JOIN gold.clientes_segmentacao c ON s.id_cliente = c.id_cliente;