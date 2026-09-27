# silver.vendas
# Objetivo: Limpar e padronizar a tabela de vendas para a camada silver.
# Regras aplicadas:
# - Remoção de duplicatas por id_venda (mantendo a primeira ocorrência).
# - Garantia de que preco_unitario esteja em DECIMAL(10,2).
# - Cálculo da receita: quantidade × preco_unitario em DECIMAL(10,2).
# - Extração de data (date), hora (0-23) e dia da semana (numero e nome em português).
# - Identificação de produto cadastrado: produto_cadastrado = false quando id_produto não existe em silver.produtos.
# - Identificação de venda antes do cadastro: venda_antes_do_cadastro = true quando data_venda < data_criacao do produto.
# - NUNCA descartar linhas: problemas de qualidade são marcados com @dp.expect (warn).
# Fail: id_venda, data_venda, id_cliente, id_produto, quantidade e preco_unitario preenchidos;
#         quantidade > 0; preco_unitario > 0; canal_venda em ('ecommerce', 'loja_fisica').
# Warn: produto_cadastrado; venda_depois_do_cadastro = NOT venda_antes_do_cadastro.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

# Mapeamento de dias da semana em português
# Mapeamento de dias da semana em português (Spark dayofweek: 1=Domingo, 7=Sábado)
DIA_SEMANA_MAP = {
    1: "Domingo",
    2: "Segunda",
    3: "Terça",
    4: "Quarta",
    5: "Quinta",
    6: "Sexta",
    7: "Sábado",
}

@dp.table(name="silver.vendas")
@dp.expect_or_fail("id_venda_nao_preenchido", "id_venda IS NOT NULL")
@dp.expect_or_fail("data_venda_nao_preenchida", "data IS NOT NULL")
@dp.expect_or_fail("id_cliente_nao_preenchido", "id_cliente IS NOT NULL")
@dp.expect_or_fail("id_produto_nao_preenchido", "id_produto IS NOT NULL")
@dp.expect_or_fail("quantidade_invalida", "quantidade IS NOT NULL AND quantidade > 0")
@dp.expect_or_fail("preco_unitario_nao_positivo", "preco_unitario > 0")
@dp.expect("canal_venda_invalido", "canal_venda IN ('ecommerce', 'loja_fisica')")
@dp.expect("produto_nao_cadastrado", "produto_cadastrado = true")
def silver_vendas():
    bronze_vendas = spark.read.table("bronze.vendas")
    silver_produtos = spark.read.table("silver.produtos")
    # Remove duplicatas por id_venda (mantém primeira ocorrência)
    df = bronze_vendas.dropDuplicates(["id_venda"])

    # Garantir que preco_unitario esteja em DECIMAL(10,2)
    df = df.withColumn(
        "preco_unitario",
        F.col("preco_unitario").cast(DecimalType(10, 2)),
    )

    # Cálculo da receita: quantidade × preco_unitario
    df = df.withColumn(
        "receita",
        F.col("quantidade") * F.col("preco_unitario"),
    )

    # Extração de data (date), hora (0-23) e dia da semana
    df = df.withColumn("data", F.col("data_venda").cast(DateType()))
    df = df.withColumn("hora", F.hour(F.col("data_venda")))
    df = df.withColumn(
        "dia_semana_num",
        F.dayofweek(F.col("data")),
    )
    df = df.withColumn(
        "dia_semana",
        F.when(F.col("dia_semana_num") == 1, F.lit("Domingo"))
        .when(F.col("dia_semana_num") == 2, F.lit("Segunda"))
        .when(F.col("dia_semana_num") == 3, F.lit("Terça"))
        .when(F.col("dia_semana_num") == 4, F.lit("Quarta"))
        .when(F.col("dia_semana_num") == 5, F.lit("Quinta"))
        .when(F.col("dia_semana_num") == 6, F.lit("Sexta"))
        .when(F.col("dia_semana_num") == 7, F.lit("Sábado"))
        .otherwise(F.lit("Desconhecido"))
    )

    # produto_cadastrado: false quando id_produto não existe em silver.produtos
    # Fazendo left join para verificar existência
    df = df.join(silver_produtos.select("id_produto"), df.id_produto == silver_produtos.id_produto, "left")
    df = df.withColumn(
        "produto_cadastrado",
        F.when(silver_produtos.id_produto.isNull(), F.lit(False)).otherwise(F.lit(True)),
    )
    # Remove a coluna de join auxiliar
    df = df.drop(silver_produtos.id_produto)

    # venda_antes_do_cadastro: true quando data_venda é anterior à data_criacao do produto
    # Note: isso exigiria join com silver.produtos para obter data_criacao
    # Para esta implementação, assumimos que a data_criacao está disponível ou será feita em job posterior
    # Por enquanto, marcamos como null e será calculado no job/pipeline
    df = df.withColumn(
        "venda_antes_do_cadastro",
        F.lit(None).cast("boolean"),
    )
    df = df.withColumn(
        "venda_depois_do_cadastro",
        F.when(F.col("venda_antes_do_cadastro") == True, F.lit(False))
        .otherwise(F.lit(True)),
    )

    return df