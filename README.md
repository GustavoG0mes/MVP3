# MVP – Pipeline de Dados em Nuvem com Databricks

## Descrição

Este trabalho tem como objetivo o desenvolvimento de um **MVP de pipeline de dados em nuvem**, contemplando todas as etapas do ciclo analítico: **busca, coleta, modelagem, carga e análise de dados**.

A solução foi implementada utilizando a **Plataforma Databricks**, explorando conceitos de **Data Lakehouse**, **Delta Lake**, **pipelines de ETL**, **modelagem analítica** e **consultas SQL**, com foco em dados reais e públicos.

---

## Objetivo

O objetivo deste MVP é construir um pipeline de dados **escalável e governado** para analisar informações públicas da **Agência Nacional de Saúde Suplementar (ANS)**, permitindo responder perguntas relacionadas a **custos assistenciais, beneficiários e operadoras de planos de saúde no Brasil**.

As principais perguntas que orientaram o desenvolvimento do trabalho foram:

- Como evoluem os custos assistenciais das operadoras ao longo do tempo?
- Qual o custo médio por beneficiário?
- Existem diferenças relevantes de custos entre tipos de operadoras?
- É possível estruturar um pipeline confiável e reprodutível para análises futuras?

Ressalta-se que **nem todas as perguntas necessariamente precisam ser respondidas integralmente**. As perguntas foram mantidas como parte do escopo original, e sua discussão é retomada na etapa de análise e autoavaliação.

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

---

## 2. Coleta

A coleta dos dados foi realizada por meio de **download direto dos arquivos disponibilizados pela ANS**, os quais foram posteriormente armazenados na nuvem e ingeridos no ambiente Databricks.

Nesta etapa, não foi necessário o uso de técnicas de *web scraping*, uma vez que os dados são fornecidos de forma estruturada e aberta.

Os arquivos brutos foram carregados na **camada Bronze** do Data Lake, preservando sua forma original para garantir rastreabilidade e reprocessamento futuro.

---

## 3. Modelagem

### Arquitetura de Dados

A arquitetura adotada segue o modelo de **Data Lakehouse**, utilizando o **Delta Lake** como tecnologia principal.

Os dados foram organizados em três camadas:

- **Bronze**: dados brutos, conforme disponibilizados pela fonte original  
- **Silver**: dados tratados, limpos e normalizados  
- **Gold**: dados agregados e modelados para consumo analítico  

Essa separação facilita a governança, o versionamento e a rastreabilidade dos dados.

![Arquitetura Delta Lake](images/arquitetura-delta-lake.png)

---

### Catálogo de Dados e Linhagem

Foi construído um **Catálogo de Dados**, contendo descrições dos principais atributos, seus domínios e tipos, bem como expectativas de valores mínimos e máximos para dados numéricos e categorias possíveis para dados categóricos.

A linhagem dos dados foi documentada e pode ser visualizada por meio das funcionalidades do **Unity Catalog**, permitindo identificar claramente:

- A origem dos dados  
- As transformações aplicadas  
- As dependências entre tabelas Bronze, Silver e Gold  

![Linhagem dos Dados](images/linhagem-custo-beneficiario.png)

---

## 4. Carga

A etapa de carga foi realizada por meio de **pipelines de ETL implementadas no Databricks**, utilizando **PySpark e SQL**.

As principais transformações realizadas incluem:

- Padronização de colunas  
- Conversão de tipos de dados  
- Junção de diferentes conjuntos de dados da ANS  
- Criação de métricas derivadas, como custo médio por beneficiário  

As tabelas finais foram persistidas no formato **Delta**, garantindo desempenho, versionamento e possibilidade de auditoria.

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
