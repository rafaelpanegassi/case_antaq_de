# Case ANTAQ - Data Engineering

This repository contains an end-to-end data engineering pipeline that scrapes maritime transport data from [Agência Nacional de Transportes Aquaviários (ANTAQ)](https://web3.antaq.gov.br/ea/sense/download.html#pt). The pipeline leverages MinIO as a data lake, processing raw data through a layered architecture (Raw → Bronze → Silver → Gold) before ingesting it into MySQL. This project showcases web scraping, data transformation, and workflow orchestration using tools like Apache Airflow and Docker.

---

## How to Run This Project

This section guides you through setting up and running the project using a `bash` terminal.

### Prerequisites
- [`pyenv`](https://github.com/pyenv/pyenv) - Python version management
- [`poetry`](https://python-poetry.org/) - Dependency management
- [`astro-cli`](https://www.astronomer.io/docs/astro/cli/overview/) - Airflow orchestration
- [`docker`](https://www.docker.com/) - Containerization

### Setup Instructions

1. **Clone the Repository**

    ```bash
    git clone https://github.com/rafaelpanegassi/case_antaq_de.git
    ```

    ```bash
    cd case_antaq_de
    ```

2. **Configure Environment Variables**

    - Create a .env file in the project root based on .env-example.
    - Default MinIO configuration:
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

3. **Set Python Version**

    - Set the Python version with `pyenv`:
    ```bash
    pyenv local 3.12.4
    ```

4. **Set Up Virtual Environment**

    - Create the virtual environment:
    ```bash
    poetry env use 3.12.4
    poetry shell
    poetry install
    ```
5. **Run All Services**

    - At root directory run bash:
    ```bash
    bash local_workspace/run_all_services.sh
    ```

6. **Access Local Services**
    - Airflow: `http://localhost:8080`
    - MiniO:   `http://localhost:9001`
    - MySQL:   `http://localhost:3306`

**NOTE:** Default credentials are `minio_admin/minio_password `for MinIO and `admin/admin` for Airflow/MySQL based on your setup.

### Manual Execution

To run the pipeline steps individually:

1. **Crawl Data**

    - Scrapes data from ANTAQ and stores it in the Raw layer.

    ```bash
    bash local_workspace/run_crawler.sh
    ```
2. **Process Bronze Layer**

    - Only copy our raw data to bronze.

    ```bash
    bash local_workspace/run_bronze_pipelines.sh
    ```

3. **Process Silver Layer**

    - Cleans and structures raw data.

    ```bash
    bash local_workspace/run_silver_pipelines.sh
    ```

4. **Process Gold Layer**

    - Further refine data to make it ready for use.

    ```bash
    bash local_workspace/run_gold_pipelines.sh
    ```

5. **Ingest into MySQL**

    - Loads Gold data into MySQL tables.

    ```bash
    bash local_workspace/run_gold_to_db.sh
    ```

### Project Structure
- `local_workspace/` - Scripts for running services and pipelines with bash.
- `.env-example` - Template for environment variables.
- `jobs` - All scripts for each layer.
- `dags` - Located in the Airflow directory.

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