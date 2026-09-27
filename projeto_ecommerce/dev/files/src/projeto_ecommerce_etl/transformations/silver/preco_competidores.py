# silver.preco_competidores
# Objetivo: Limpar e padronizar a tabela de preços de concorrentes para a camada silver.
# Regras aplicadas:
# - Remoção de duplicatas por (id_produto, nome_concorrente).
# - Conversão de data_coleta de texto para timestamp.
# - preco_concorrente em DECIMAL(10,2).
# - Cálculo de preco_suspeito: true quando preco_concorrente < 60% do preço_atual da nossa loja.
# - NUNCA descartar linhas: problemas de qualidade são marcados com @dp.expect (warn).
# Fail: id_produto preenchido, preço_concorrente > 0.
# Warn: preco_plausible = NOT preco_suspeito.

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

@dp.table(name="silver.preco_competidores")
@dp.expect("id_produto_nao_preenchido", "id_produto IS NOT NULL")
@dp.expect("preco_concorrente_nao_positivo", "preco_concorrente > 0")
def silver_preco_competidores():
    bronze_preco_competidores = spark.read.table("bronze.preco_competidores")
    # Remove duplicatas por (id_produto, nome_concorrente)
    df = bronze_preco_competidores.dropDuplicates(["id_produto", "nome_concorrente"])

    # Conversão de data_coleta de texto para timestamp
    df = df.withColumn(
        "data_coleta",
        F.to_timestamp(F.col("data_coleta")),
    )

    # Garantir que preco_concorrente esteja em DECIMAL(10,2)
    df = df.withColumn(
        "preco_concorrente",
        F.col("preco_concorrente").cast(DecimalType(10, 2)),
    )

    # Join com silver.produtos para obter o preco_atual
    silver_produtos = spark.read.table("silver.produtos")
    df = df.join(silver_produtos.select("id_produto", "preco_atual"), "id_produto", "left")

    # Cálculo de preco_suspeito: true quando preco_concorrente < 60% do preco_atual
    df = df.withColumn(
        "preco_suspeito",
        F.when(
            F.col("preco_concorrente") < 0.6 * F.col("preco_atual"),
            True,
        ).otherwise(False),
    )

    return df