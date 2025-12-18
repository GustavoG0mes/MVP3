# MVP –  Projeto de Engenharia de dados (Pipeline de Dados em Nuvem com Databricks)

## Descrição

Este trabalho tem como objetivo o desenvolvimento de um **MVP de pipeline de dados em nuvem**, contemplando todas as etapas do ciclo analítico: **busca, coleta, modelagem, carga e análise de dados**.

A solução foi implementada utilizando a **Plataforma Databricks**, explorando conceitos de **Data Lakehouse**, **Delta Lake**, **pipelines de ETL**, **modelagem analítica** e **consultas SQL**, com foco em dados reais e públicos.

![Overview do Projeto](Imagens/overview-projeto.png)

---

## Objetivo

O objetivo deste MVP é construir um pipeline de dados **escalável e governado** para analisar informações públicas da **Agência Nacional de Saúde Suplementar (ANS)**, permitindo responder perguntas relacionadas a **custos assistenciais, beneficiários e operadoras de planos de saúde no Brasil**.

As perguntas/problemas que desejo responder através das análises são:

1.Qual é o índice atual de sinistralidade no setor de seguros de saúde? Ele está abaixo ou acima da média histórica?

2.Qual é a segurança mais eficiente do ponto de vista de custo do beneficiário ?

3.Qual é a participação de mercado em número de beneficiários no segmento médico-hospitalar?

4.Quantas empresas de plano de saúde existem no Brasil?

5.Quantos beneficiários existem no Brasil? Qual é a taxa de cobertura?

6.Existem mais planos individuais ou coletivos?

---

## Plataforma

O pipeline foi desenvolvido na **Plataforma Databricks**, utilizando recursos compatíveis com a **Databricks Community Edition**.

A arquitetura adotada permite a execução de cargas, transformações e análises utilizando **PySpark**, **SQL** e **Delta Lake**, respeitando boas práticas de engenharia de dados e governança.

---

## 1. Busca pelos Dados

Foram utilizados **dados públicos disponibilizados pela Agência Nacional de Saúde Suplementar (ANS)**, que disponibiliza informações detalhadas sobre:

- Operadoras de planos de saúde  
- Beneficiários  
- Custos assistenciais  
- Demonstrativos financeiros  

Os dados foram obtidos diretamente a partir do portal oficial da ANS, respeitando os termos de uso e licenciamento público das bases, o que permite sua utilização para fins acadêmicos e analíticos.
Os dados têm boa qualidade, no geral, e são bem organizados, sendo a grande maioria acompanhados de um arquivo de metadados ou catálogo. Alguns catálogos informam, inclusive, que algumas colunas são chaves estrangeiras de tabelas em outros conjuntos de dados, o que é muito útil.
Os dados são disponibilizados em arquivos compactados .zip, por isso, o código para coleta de dados envolve a remoção dos arquivos, leitura em memória, e persistência em um volume no Databricks (ou armazenamento no S3), que serviu como landing/camada raw.
Esse procedimento de persistência dos dados em uma camada raw, fora do ambiente do Delta Lake, é uma forma de evitar problemas comuns ao trabalhar com grandes volumes de dados e clusters Spark, como derramamento .
Após salvar os arquivos no armazenamento, o código faz a leitura usando pyspark, e a inserção na camada bronze, em formato Delta.
O processo de ingestão dos dados e criação da camada de bronze foi feito através de classes de ingestão , como:

bronze_beneficiarios.py


(Colocar o código aqui)

---

## 3. Modelagem

### Arquitetura de Dados

A arquitetura adotada segue o modelo de **Data Lakehouse**, utilizando o **Delta Lake** como tecnologia principal.

Os dados foram organizados em três camadas:

- **Bronze**: dados brutos, conforme disponibilizados pela fonte original  
- **Silver**: dados tratados, limpos e normalizados  
- **Gold**: dados agregados e modelados para consumo analítico  

Essa separação facilita a governança, o versionamento e a rastreabilidade dos dados.

![Delta Lake](Imagens/delta-lake.png)

---

### Catálogo de Dados e Linhagem

O pipeline do trabalho utiliza o framework Delta e as tabelas são processadas em camadas bronze, silver e gold.

Ao salvar as tabelas em formato deltae utilizar o Unity Catalog, é possível usufruir de funcionalidades integradas ao metastore do Databricks, como controle de acesso, métricas de uso, visualização de esquema e linhagem dos dados.

Para ilustrar, observe o diagrama de linhagem da tabela gold.ans.custo_beneficiario, que utiliza 4 tabelas como origem.

![Custo Beneficiário Lineage](Imagens/custo-beneficiario-lineage.jpg)

Podemos observar que no esquema das tabelas da camada bronze há maior dimensionalidade, além de dados com tipos inapropriados, nomes de colunas de diferentes formatos, entre outras características de dados em menor qualidade.

Um exemplo é na coluna "CNPJ" da tabela bronze.ans.operadoras, que ao ler os dados usando Spark, foi inferido um tipo bigint, enquanto o correto seria string. Essa transformação é feita na camada prata e na tabela silver.ans.operadorasjá podemos ver a mudança feita.

O código utilizado para transformação desses dados na camada silver pode ser encontrado em /src/silver/silver_operadoras.py .

silver_operadoras.py

(Colocar o código)

Destaque para o código que transforma os dados da coluna "CNPJ" em string.
(Código)

Além dessa transformação, na camada silver, é feito um tratamento da "RAZAO_SOCIAL", removendo os termos comuns (LTDA, SA, EIRELI, etc). Quando "NOME_FANTASIA" é NULL, o código substitui o campo nulo pelo valor de "RAZAO_SOCIAL".

Exemplo: Custo por Beneficiário
Dessa forma, conforme as tabelas avançadasm no fluxo, é definido um esquema, até chegar na tabela ouro que será consumida pelo ambiente de análise , para geração de insights e produtos de dados, como dashboards.

A tabela gold.ans.custo_beneficiario, que calcula um indicador setorial relacionado à eficiência da operadora, é um bom exemplo, pois utiliza todas as 3 (três) fontes primárias para ser construída.

Na camada gold, na maioria dos casos, utilizei a linguagem SQL para criar as tabelas:

custo_beneficiario.sql

```sql
CREATE OR REPLACE TABLE gold.ans.custo_beneficiario
USING DELTA AS (
  WITH despesas AS (
    SELECT ANO, REG_ANS, AVG(VL_SALDO_INICIAL) AS TOTAL_DESPESAS
    FROM silver.ans.demonstracoes_contabeis
    WHERE ANO = 2023 AND CD_CONTA_CONTABIL = '41'
    GROUP BY ANO, REG_ANS
  ),

  beneficiarios AS (
    SELECT CD_OPERADORA, SUM(TOTAL_BENEFICIARIOS) AS NUM_BENEFICIARIOS
    FROM gold.ans.num_beneficiarios
    GROUP BY CD_OPERADORA
  ),

  operadoras AS (
    SELECT * FROM silver.ans.operadoras
    WHERE LOWER(MODALIDADE) NOT LIKE '%odonto%'
  )

  SELECT d.REG_ANS, o.NOME_FANTASIA, ROUND(d.TOTAL_DESPESAS / b.NUM_BENEFICIARIOS, 2) AS CUSTO_BENEFICIARIO
  FROM despesas AS d
  LEFT JOIN beneficiarios AS b ON d.REG_ANS = b.CD_OPERADORA
  LEFT JOIN operadoras AS o ON d.REG_ANS = o.REGISTRO_ANS
  WHERE b.NUM_BENEFICIARIOS > 0 AND d.TOTAL_DESPESAS > 0
);
```

Ao final do fluxo de transformações, a tabela na camada `gold` possui apenas 3 (três) domínios, seguindo o catálogo abaixo:

| Variável | Tipo | Descrição |
| -------- | ------------ | --------- |
| REG_ANS | string | Código ANS de identificação da operadora |
| NOME_FANTASIA | string | Nome fantasia da operadora |
| CUSTO_BENEFICIARIO | double | Custo por beneficiário em reais (R$) por trimestre |

Essa tabela final está pronta para ser consumida por dashboards ou por *stakeholders* dentro da organização, sendo possível rankear as empresas da mais eficiente para a menos eficiente, assim como fazer *joins* com outras tabelas, como `gold.ans.market_share`, e comparar a eficiência entre as líderes do mercado.

---

## 4. Carga

A etapa de carga foi realizada por meio de **pipelines de ETL implementadas no Databricks**, utilizando **PySpark e SQL**.
As tabelas finais foram persistidas no formato **Delta**, garantindo desempenho, versionamento e possibilidade de auditoria.

Se considerarmos a etapa de ingestão na camada `raw`, o processo segue a lógica de ELT (extração, carga e transformção), de modo que os dados são coletados e carregados como arquivos no formato original (.csv), em uma *landing zone*, e só depois são transformados em tabelas `delta`.

A escolha por adotar esse processo foi devido ao grande volume dos conjuntos de dados de `demonstrações_contábeis` e `beneficiários`, além da eficiência gerada por essa abordagem, resultando em operações mais rápidas e sobrecarga reduzida nos clusters.
Todos os arquivos utilizados para construção da pipeline de ETL (ou ELT), separados em camadas bronze, silver e gold, podem ser consultados neste repositório na pasta `/src`.
Aqui estão os links para os arquivos:

#### Bronze

- [bronze_beneficiarios.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/bronze/bronze_beneficiarios.py)
- [bronze_demonstracoes_contabeis.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/bronze/bronze_demonstracoes_contabeis.py)
- [bronze_operadoras.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/bronze/bronze_operadoras.py)

#### Silver

- [silver_beneficiarios.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/silver/silver_beneficiarios.py)
- [silver_demonstracoes_contabeis.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/silver/silver_demonstracoes_contabeis.py)
- [silver_operadoras.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/silver/silver_operadoras.py)

#### Gold

- [sinistralidade.py](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/sinistralidade.py)
- [custo_beneficiario.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/custo_beneficiario.sql)
- [num_beneficiarios.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/num_beneficiarios.sql)
- [num_operadoras.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/num_beneficiarios.sql)
- [market_share.sql](https://github.com/ianaraujo/puc-engenharia-dados/blob/master/src/gold/market_share.sql)

---

## 5. Exportação para AWS

Além dos códigos utilizados para pipeline dos dados da ANS, no diretório `/src` também pode ser encontrado o arquivo `export.py`.
Como eu tinha o intuíto de aprender o máximo de ferramentas nessa etapa da pós, criei um *shared volume* - que liga o Databricks a um storage externo no ambiente da AWS, no meu caso o S3 - e um código em Python que carrega todas as tabelas da camada `gold` nesse volume em formato `parquet`.
O código utilizado para realizar essa etapa final foi:

```python
bucket = 'databricks-gold-ans'
database = 'gold.ans'

tables = spark.sql(f'show tables in {database}')


def export_parquet(bucket: str, database: str, table: DataFrame) -> None:
    df = spark.read.table(f'{database}.{table.tableName}') 

    save_path = f'/Volumes/gold/ans/{bucket}/{table.tableName}'
    
    df.repartition(1) \
        .write.mode('overwrite') \
        .format('parquet') \
        .option("header","true") \
        .option("inferSchema", "true") \
        .save(save_path)

    parquet_file = [file.name for file in dbutils.fs.ls(save_path) if file.name.startswith('part-')]

    dbutils.fs.mv(save_path + "/" + parquet_file[0], f"{save_path}.parquet")

    for file in dbutils.fs.ls(save_path):
        dbutils.fs.rm(file.path)
    
    dbutils.fs.rm(save_path)
    
    print(f"Saved '{table.tableName}.parquet' to {bucket}!")


for table in tables.collect():
    export_parquet(bucket, database, table)
    
    print("All data exported!")
```
---
## 6. Databricks Workflows

Todos os processos descritos acima, incluindo a coleta dos dados, transformações, carga e exportação dos dados finais para AWS, foram automatizados utilizando a funcionalidade do Databricks Workflows. 

O Databricks Workflows é um serviço integrado na plataforma para fazer a **orquestração de pipelines**, de forma similar a outras soluções no mercado, como Airflow, Dagster, Prefect, etc.

Assim como os demais, é possível agendar execuções, *triggers*, monitorar falhas e logs, e definir de forma visual a ordem de execução das tarefas.

A grande vantagem do Databricks Workflows, frente aos outros serviços, é a integração com a plataforma. As tarefas de um workflow podem utilizar clusters dedicados - chamados de *job clusters*, que são ligado e depois terminados apenas para execução da pipeline - e podem ser definidas a partir dos próprios Notebooks do Databricks.

Dessa forma, é possível utilizar diversas liguagens, como Python, SQL, Scala e R, na mesma pipeline.

Ao final da configuração de um workflow, é também possível gerar um arquivo `.json`, que foi salvo em [fluxos de trabalho/pipeline.json](https://github.com/GustavoG0mes/MVP3/blob/main/fluxos%20de%20trabalho/pipeline.json)
, e permite versionar os *jobs*, além de definir programaticamente uma visualização da sua pipeline:

![Workflow Run](Imagens/workflow-run.png)
![Workflow Tasks](Imagens/workflow-tasks.png)

---

## 5. Análise

### a. Qualidade dos Dados

Foi realizada uma análise de qualidade para os principais atributos dos conjuntos de dados, verificando:

- Valores nulos  
- Distribuição dos dados numéricos  
- Coerência entre campos relacionados  
- Presença de *outliers*  

Os dados analisados apresentaram bom nível de qualidade, compatível com bases previamente curadas. Ainda assim, foram realizadas validações estatísticas e exploratórias para garantir que eventuais inconsistências não impactassem as análises finais.

---

### b. Solução do Problema

Com os dados organizados na camada Gold, foram realizadas análises analíticas utilizando **SQL**, permitindo responder às perguntas definidas na etapa de objetivos.

Entre os principais resultados obtidos, destacam-se:

- Evolução dos custos assistenciais ao longo do tempo  
- Cálculo do custo médio por beneficiário  
- Comparações entre diferentes tipos de operadoras  
- Identificação de padrões relevantes para apoio à tomada de decisão  

Cada resultado foi discutido de forma crítica, conectando os valores obtidos aos objetivos iniciais do MVP.

Ao final, foi possível concluir que o pipeline construído atende ao objetivo proposto, fornecendo uma base sólida, governada e escalável para análises futuras.

---

## Conclusão

Este MVP demonstrou a viabilidade da construção de um pipeline de dados completo em nuvem, utilizando tecnologias modernas como **Databricks e Delta Lake**, desde a ingestão até a análise final.

A solução desenvolvida atende aos requisitos propostos, respeitando boas práticas de engenharia de dados, modelagem analítica e governança, além de permitir extensões futuras para análises mais avançadas.
