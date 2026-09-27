# 🛒 E-Commerce Data & AI Platform

Uma solução completa de **Engenharia e Análise de Dados** com integração de **IA Generativa** voltada para o mercado de E-commerce. O projeto abrange desde a ingestão de dados brutos até a entrega de inteligência de negócio através de Dashboards e um Assistente Virtual Contextualizado.

## 📌 Visão Geral do Projeto

No cenário do e-commerce, a rapidez e a precisão no acesso aos dados determinam a capacidade de resposta do negócio. Este projeto foi desenvolvido para resolver o desafio de transformar dados desestruturados/dispersos em insights acionáveis de forma automatizada.

A plataforma realiza o ciclo completo do dado (**Data Lifecycle**):

1. **Coleta e Ingestão:** Extração contínua de dados de vendas, clientes e estoque.

2. **Armazenamento:** Estruturação dos dados em um repositório centralizado.

3. **Tratamento e Transformação (ETL):** Limpeza, padronização e modelagem dimensional dos dados.

4. **Visualização:** Dashboards executivos interativos para monitoramento de KPIs.

5. **IA Negocial:** Chatbot integrado com acesso à base tratada, permitindo consultas dinâmicas em linguagem natural.

## 🏗️ Arquitetura da Solução

```
[Fontes de Dados] ──(Extração)──> [Databricks / Storage]
                                          │
                                     (ETL / Trata)
                                          │
                                          ▼
                                 [Data Warehouse / Databricks]
                                     │          │
                       ┌─────────────┘          └─────────────┐
                       ▼                                      ▼
          [Dashboard Analítico,Databricks]            [Chatbot de IA / RAG]
          (KPIs, Tendências, Vendas)               (Consultas em Linguagem Natural)

```

## 🛠️ Tecnologias Utilizadas

* **Linguagens:** Python, SQL

* **Ingestão e Tratamento (ETL):** Pandas, PySpark / dbt,Boto3

* **Armazenamento / Data Warehouse:** PostgreSQL / Databricks

* **Visualização de Dados:** Databricks

* **Inteligência Artificial:** Opencode,Databricks Genie Code

* **Orquestração & Outros:** Git/GitHub

## 🚀 Etapas de Desenvolvimento

### 1. Extração & Armazenamento

* Configuração de conectores para extração automatizada das fontes de e-commerce (transações, histórico de navegação, cadastro de produtos).

* Criação do pipeline de carga na zona de *Staging*.

### 2. Tratamento & Modelagem de Dados

* Remoção de inconsistências, duplicatas e tratamento de valores nulos.

* Aplicação de modelagem dimensional (Star Schema) focada na criação de *Data Marts* de Vendas, Clientes e Produtos.

* Cálculo de métricas fundamentais como **LTV (Lifetime Value)**, **CAC**, **Ticket Médio**, **Taxa de Conversão** e **Churn Rate**.

### 3. Dashboard Interativo

* Construção de painéis visuais para tomada de decisão estratégica:

  * **Visão Geral de Vendas:** Faturamento, venda por categoria e canal.

  * **Análise de Produtos:** Curva ABC de estoque e itens mais vendidos.

### 4. Assistente Virtual de IA (Chat Negocial)

* Desenvolvimento de um bot conversacional alimentado pelos dados já tratados e modelados.

* Permite que gestores e analistas façam perguntas como:

  > *"Qual foi o produto mais vendido na última semana e qual a sua margem de lucro?"*
  >
  > *"Qual segmento de clientes teve o maior churn este mês?"*

* Respostas rápidas, contextuais e fundamentadas diretamente nos dados da operação.

## 📊 Principais KPIs e Insights Gerados

* **Eficiência Operacional:** Redução do tempo de geração de relatórios de dias para segundos.

* **Democratização dos Dados:** Acesso simplificado a dados complexos via interface de chat sem necessidade de conhecimento prévio em SQL.

* **Prevenção de Perdas:** Identificação precoce de gargalos no estoque e quedas na taxa de conversão.

  ## 

Desenvolvido por **Thiago Marcos**.
