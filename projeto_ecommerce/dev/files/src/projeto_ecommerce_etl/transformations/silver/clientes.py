# silver.clientes
# Objetivo: Limpar e padronizar a tabela de clientes para a camada silver.
# Regras aplicadas:
# - Remoção de duplicatas por id_cliente (mantendo a primeira ocorrência).
# - Guarda o nome original em nome_original.
# - Remoção de pronome de tratamento no início do nome (Sr., Sra., Srta., Dr., Dra) e conversão para formato título.
# - UF em maiúsculas.
# - Cálculo de nome_estado e regio a partir de um mapeamento fixo das 27 UFs do IBGE (declaração no próprio arquivo, não existe tabela de estados na bronze).
# - NUNCA descartar linhas: problemas de qualidade são marcados com @dp.expect (warn).
# Fail: id_cliente preenchido, regiao preenchida.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Mapeamento fixo das 27 UFs do IBGE para nome_estado e regio
UF_MAPPING = {
    "AC": {"nome_estado": "Acre", "regiao": "Norte"},
    "AL": {"nome_estado": "Alagoas", "regiao": "Nordeste"},
    "AM": {"nome_estado": "Amazonas", "regiao": "Norte"},
    "AP": {"nome_estado": "Amapá", "regiao": "Norte"},
    "BA": {"nome_estado": "Bahia", "regiao": "Nordeste"},
    "CE": {"nome_estado": "Ceará", "regiao": "Nordeste"},
    "DF": {"nome_estado": "Distrito Federal", "regiao": "Centro-Oeste"},
    "ES": {"nome_estado": "Espírito Santo", "regiao": "Sudeste"},
    "GO": {"nome_estado": "Goiás", "regiao": "Centro-Oeste"},
    "MA": {"nome_estado": "Maranhão", "regiao": "Nordeste"},
    "MT": {"nome_estado": "Mato Grosso", "regiao": "Centro-Oeste"},
    "MS": {"nome_estado": "Mato Grosso do Sul", "regiao": "Centro-Oeste"},
    "MG": {"nome_estado": "Minas Gerais", "regiao": "Sudeste"},
    "PA": {"nome_estado": "Pará", "regiao": "Norte"},
    "PB": {"nome_estado": "Paraíba", "regiao": "Nordeste"},
    "PR": {"nome_estado": "Paraná", "regiao": "Sul"},
    "PE": {"nome_estado": "Pernambuco", "regiao": "Nordeste"},
    "PI": {"nome_estado": "Piauí", "regiao": "Nordeste"},
    "RJ": {"nome_estado": "Rio de Janeiro", "regiao": "Sudeste"},
    "RN": {"nome_estado": "Rio Grande do Norte", "regiao": "Nordeste"},
    "RS": {"nome_estado": "Rio Grande do Sul", "regiao": "Sul"},
    "RO": {"nome_estado": "Rondônia", "regiao": "Norte"},
    "RR": {"nome_estado": "Roraima", "regiao": "Norte"},
    "SC": {"nome_estado": "Santa Catarina", "regiao": "Sul"},
    "SP": {"nome_estado": "São Paulo", "regiao": "Sudeste"},
    "SE": {"nome_estado": "Sergipe", "regiao": "Nordeste"},
    "TO": {"nome_estado": "Tocantins", "regiao": "Norte"},
}

@dp.table(name="silver.clientes")
@dp.expect("id_cliente_nao_preenchido", "id_cliente IS NOT NULL")
@dp.expect("regiao_nao_preenchida", "regiao IS NOT NULL")
def silver_clientes():
    bronze_clientes = spark.read.table("bronze.clientes")
    # Remove duplicatas por id_cliente (mantém primeira ocorrência)
    df = bronze_clientes.dropDuplicates(["id_cliente"])

    # Guarda o nome original antes de quaisquer transformações
    df = df.withColumn("nome_original", F.col("nome_cliente"))

    # Remove pronome de tratamento no início e converte para formato título
    # Patterns: "Sr.", "Sra.", "Srta.", "Dr.", "Dra" no início do nome
    df = df.withColumn(
        "nome_cliente",
        F.when(
            F.col("nome_cliente").rlike("^(Sr|Sra|Srta|Dr|Dra)\\s*\\."),
            F.regexp_replace(F.col("nome_cliente"), "^(Sr|Sra|Srta|Dr|Dra)\\s*", ""),
        ).otherwise(F.col("nome_cliente")),
    )
    df = df.withColumn("nome_cliente", F.initcap(F.col("nome_cliente")))

    # UF em maiúsculas
    df = df.withColumn("estado", F.upper(F.col("estado")))

    # nome_estado e regio a partir do mapeamento fixo das 27 UFs do IBGE
    # Usando um join com um mapa estático
    from pyspark.sql.types import StringType
    import pyspark.sql.types as T

    uf_data = [
        (uf, info["nome_estado"], info["regiao"])
        for uf, info in UF_MAPPING.items()
    ]
    uf_schema = T.StructType([
        T.StructField("uf", T.StringType(), False),
        T.StructField("nome_estado", T.StringType(), False),
        T.StructField("regiao", T.StringType(), False),
    ])
    uf_df = df.sparkSession.createDataFrame(uf_data, uf_schema)

    df = df.join(uf_df, df.estado == uf_df.uf, "left").drop(uf_df.uf)

    return df