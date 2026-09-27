# silver.produtos
# Objetivo: Limpar e padronizar a tabela de produtos para a camada silver.
# Regras aplicadas:
# - Remoção de duplicatas por id_produto (mantendo a primeira ocorrência).
# - Aplicação de trim em nome_produto para remover espaços desnecessários.
# - Garantia de que preco_atual esteja em DECIMAL(10,2).
# - Cálculo da faixa_preco baseada no preço: PREMIUM (>1000), MEDIO (>500) ou BASICO.
# - NUNCA descartar linhas: problemas de qualidade são marcados com @dp.expect (warn).
# Fail: id_produto preenchido e preco_atual > 0.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

@dp.table(name="silver.produtos")
@dp.expect("id_produto_nao_preenchido", "id_produto IS NOT NULL")
@dp.expect("preco_nao_positivo", "preco_atual > 0")
def silver_produtos():
    bronze_produtos = spark.read.table("bronze.produtos")
    # Remove duplicatas por id_produto (mantém primeira ocorrência)
    df = bronze_produtos.dropDuplicates(["id_produto"])

    # Trim em nome_produto
    df = df.withColumn("nome_produto", F.trim(F.col("nome_produto")))

    # Garantir que preco_atual esteja em DECIMAL(10,2)
    df = df.withColumn(
        "preco_atual",
        F.col("preco_atual").cast(DecimalType(10, 2)),
    )

    # Faixa de preço: PREMIUM (>1000), MEDIO (>500) ou BASICO
    df = df.withColumn(
        "faixa_preco",
        F.when(F.col("preco_atual") > 1000, "PREMIUM")
        .when(F.col("preco_atual") > 500, "MEDIO")
        .otherwise("BASICO"),
    )

    return df