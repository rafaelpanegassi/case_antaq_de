#!/usr/bin/env bash
set -e  # Exit immediately if a command fails

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
