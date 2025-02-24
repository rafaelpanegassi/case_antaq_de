CASE ANTAQ - DATA ENGINEERING
========

## How to run this project

This section explains how to run the project.

All the steps here are intended for a `bash` terminal.

The project setup uses [`pyenv`](https://github.com/pyenv/pyenv), [`poetry`](https://python-poetry.org/), [`astro-cli`](https://www.astronomer.io/docs/astro/cli/overview/) and [`docker`](https://www.docker.com/).

1 - Clone the repo locally:
```bash
git https://github.com/rafaelpanegassi/case_antaq_de.git
```

2 - Access the project directory:
```bash
cd case_antaq_de
```

To run this properly, it's necessary to create the `.env` variable file in the root of the project. An example can be found in [.env-example](.env-example). The default configuration to connect with MongoDB is:
```
ENDPOINT_URL=http://minio:9000
MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=minio_password
REGION_NAME=us-east-1
RAW_BUCKET=raw
BRONZE_BUCKET=bronze
SILVER_BUCKET=silver
GOLD_BUCKET=gold
BASE_URL=https://web3.antaq.gov.br/ea/txt/
```

### Local Setup

After completing steps 1 and 2, and with the `.env` variable file configured:

3 - Set the Python version with `pyenv`:
```bash
pyenv local 3.12.2
```

4 - Create the virtual environment:
```bash
poetry env use 3.12.2
```

5 - Activate the virtual environment:
```bash
poetry shell
```

6 - Install dependencies:
```bash
poetry install
```

Go to airflow directory:
```bash
cd airflow
```

Build the images:
```bash
astro dev start
```

Access the local host airflow:
```
http://localhost:8080/
```
Access the local host minio:
```
http://localhost:9001/
```

Access the local host mysql:
```
http://localhost:3306/
```

### Setup Dags Activate in airflow

**NOTE:** Activate dags in this sequence: crawler, bronze, silver and gold.


### To Run Manually


1 - Install java to run `Pyspark`:
```bash
apt-get update && \
apt-get install -y openjdk-17-jdk && \
apt-get clean
```

2 - Setup java path:
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

3 - To run Crawler:
```bash
poetry run python jobs/crawlers/crawler_antaq.py
```

4 - To run bronze scripts:
```bash
poetry run python jobs/bronze/script_name.py
```

5 - To run silver scripts:
```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.3.0 jobs/silver/script_name.py
```

6 - To run sigoldlver scripts:
```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.3.0 jobs/gold/script_name.py
```

### Questionário

**1) Auto avaliação**  
Avalie suas habilidades nos requisitos abaixo, classificando seu nível de domínio de acordo com:  
- **0, 1, 2**: não tem conhecimento e experiência  
- **3, 4, 5**: conhece a técnica e tem pouca experiência  
- **6**: domina a técnica e já desenvolveu vários projetos utilizando-a  

**Tópicos de Conhecimento:**  
- Ferramentas de visualização de dados (Power BI, Qlik Sense e outros): 6  
- Manipulação e tratamento de dados com Python: 6  
- Manipulação e tratamento de dados com Pyspark: 6  
- Desenvolvimento de data workflows em Ambiente Azure com Databricks: 5  
- Desenvolvimento de data workflows com Airflow: 6  
- Manipulação de bases de dados NoSQL: 4  
- Web crawling e web scraping para mineração de dados: 5  
- Construção de APIs (REST, SOAP e Microservices): 5  

**2) Auto avaliação** 
- **A**: Olhando para todos os dados disponíveis na fonte citada acima, em qual
estrutura de dados você orienta guardá-los? Data Lake, SQL ou NoSQL?
Discorra sobre sua orientação.
- **R**: Recomendo utilizar um Data Lake com Delta Lake, pois essa solução oferece uma alta flexibilidade para armazenar diferentes tipos de dados (estruturados, semi-estruturados e não estruturados) e, ao mesmo tempo, garante transações ACID, assegurando a integridade e consistência das operações. Além disso, o Delta Lake permite versionar os dados, facilitando a auditoria e a reversão de mudanças, e conta com otimizações que melhoram o desempenho das consultas, sendo ideal para ambientes com processamento tanto em lote quanto em tempo real.

- **B**: Nosso cliente estipulou que necessita de informações apenas sobre as
atracações e cargas contidas nessas atracações dos últimos 3 anos (2021-
2023). Logo, o time de especialistas de dados, em conjunto com você,
analisaram e decidiram que os dados devem constar no data lake do
observatório e em duas tabelas do SQL Server, uma para atracação e outra
para carga.
Assim, desenvolva script(s) em Python e Spark que extraia os dados do
anuário, transforme-os e grave os dados tanto no data lake, quanto nas duas
tabelas do SQL Server, sendo atracacao_fato e carga_fato, com as respectivas
colunas abaixo. Os scripts de criação das tabelas devem constar no código
final.
Lembrando que os dados têm periodicidade mensal, então script’s
automatizados e robustos ganham pontos extras. (2 pontos + 1 ponto para
solução automatizada e elegante).
- **R**: Está no código.

- **C**: Essas duas tabelas ficaram guardadas no nosso Banco SQL SERVER. Nossos
economistas gostaram tanto dos dados novos que querem escrever uma
publicação sobre eles. Mais especificamente sobre o tempo de espera dos
navios para atracar. Mas eles não sabem consultar o nosso banco e apenas
usam o Excel. Nesse caso, pediram a você para criar uma consulta (query)
otimizada em sql em que eles vão rodar no excel e por isso precisa ter o menor
número de linhas possível para não travar o programa. Eles
querem uma tabela com dados do Ceará, Nordeste e Brasil contendo número
de atracações, para cada localidade, bem como tempo de espera para atracar
e tempo atracado por meses nos anos de 2021 e 2023.

**3) Criação de ambiente de desenvolvimento com Linux e Docker.** 
- **A**: Finalmente, este processo deverá ser automatizado usando a ferramenta de orquestração de workflow Apache Airflow + Docker. Escreva uma DAG para a base ANTAQ levando em conta as características e etapas de ETL para esta base de dados considerando os repositórios de
data lake e banco de dados. Esta também podperá conter operadores para enviar avisos por
e-mail, realizar checagens quando necessário (e.g.: caso os dados não sejam encontrados,
quando o processo for finalizado, etc). Todos os passos do processo ETL devem ser listados
como tasks e orquestrados de forma otimizada.

- **R**: Está no código.