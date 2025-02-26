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
- `local_workspace/` - Bash scripts for running services and pipelines.
- `.env-example` - Template for environment variables.
- `jobs` - All scripts for each layer.
- `dags` - Located in the Airflow directory.

### Future Improvements

Here are some potential enhancements to elevate this project:

- **Data Quality Checks:** Add automated validation steps in the pipeline (e.g., schema checks, duplicate detection) to ensure data integrity across layers.
- **Visualization Dashboard:** Build a simple dashboard (e.g., using Streamlit or Power BI) to provide interactive insights from the Gold layer data.
- **Cloud Deployment:** Migrate the pipeline to a cloud provider (e.g., AWS, Azure) for scalability, using S3 instead of MinIO and managed services like AWS Glue or Azure Data Factory.Migrate the pipeline to a cloud provider (e.g., AWS, Azure) for scalability, using S3 instead of MinIO and managed services like AWS Glue or Azure Data Factory.
- **Unit Tests:** Implement tests for the Python scripts (e.g., using pytest) to validate data transformations and improve maintainability.
