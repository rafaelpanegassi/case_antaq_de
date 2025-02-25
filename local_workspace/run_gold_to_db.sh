#!/usr/bin/env bash
set -e  # Exit immediately if a command fails

# Check if Java is already installed
echo "Checking Java installation..."
if ! command -v java >/dev/null 2>&1; then
    echo "Java not found, attempting to install OpenJDK 17..."
    # Only run sudo commands if Java isn't installed
    if sudo apt-get update && sudo apt-get install -y openjdk-17-jdk && sudo apt-get clean; then
        echo "Java installed successfully."
    else
        echo "Failed to install Java. Please install OpenJDK 17 manually and rerun the script."
        exit 1
    fi
else
    echo "Java is already installed."
fi

# Set environment variables for this session
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
export PATH="$JAVA_HOME/bin:$PATH"

echo "Java version:"
java -version

echo "Running Gold-to-DB ingestion script..."
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.3.0,mysql:mysql-connector-java:8.0.33,io.delta:delta-core_2.12:2.4.0 jobs/gold_to_db/ingest_gold_to_db.py

echo "Ingestion to database script executed successfully."