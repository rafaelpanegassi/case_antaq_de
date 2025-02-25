#!/usr/bin/env bash
set -e  # Exit immediately if a command fails

# Navigate to the airflow directory and start Astro in background
echo "Starting services..."
cd airflow
astro dev start &

# Wait for services to be ready (Airflow, MinIO, and MySQL) with retries
echo "Waiting for services to be ready..."

# Wait for Airflow (assuming Airflow webserver is available on http://localhost:8080)
until curl -s http://localhost:8080 > /dev/null; do
    echo "Waiting for Airflow (port 8080)..."
    sleep 60
done
echo "Airflow is up and running!"

# Wait for MinIO (assuming MinIO is available on http://localhost:9000)
until curl -s http://localhost:9000 > /dev/null; do
    echo "Waiting for MinIO (port 9000)..."
    sleep 60
done
echo "MinIO is up and running!"

# Wait for MySQL (assuming MySQL health check is available, e.g., via a simple TCP check on port 3306)
# Note: MySQL doesn't typically have a web endpoint, so we'll use a similar approach to check the port
until curl -s --head --connect-timeout 5 http://localhost:3306 > /dev/null 2>&1 || nc -z localhost 3306 2>/dev/null; do
    echo "Waiting for MySQL (port 3306)..."
    sleep 60
done
echo "MySQL is up and running!"

# Return to the parent directory
cd ..

echo "Updating package list and installing OpenJDK 17..."
sudo apt-get update && \
    sudo apt-get install -y openjdk-17-jdk && \
    sudo apt-get clean

# Set environment variables for this session
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
export PATH="$JAVA_HOME/bin:$PATH"

echo "Java installed successfully. Java version:"
java -version

echo "Step 3: Running Crawler..."
poetry run python jobs/crawlers/crawler_antaq.py

echo "Step 4: Running Bronze scripts..."
for script in jobs/bronze/*.py; do
    echo "Running Bronze script: $script"
    poetry run python "$script"
done

echo "Step 5: Running Silver scripts..."
for script in jobs/silver/*.py; do
    echo "Running Silver script: $script"
    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.3.0 "$script"
done

echo "Step 6: Running Gold scripts..."
for script in jobs/gold/*.py; do
    echo "Running Gold script: $script"
    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.3.0 "$script"
done

echo "Step 7: Running Gold-to-DB ingestion script..."
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.3.0,mysql:mysql-connector-java:8.0.33,io.delta:delta-core_2.12:2.4.0 jobs/gold_to_db/ingest_gold_to_db.py

echo "All scripts executed successfully."