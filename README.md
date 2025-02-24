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