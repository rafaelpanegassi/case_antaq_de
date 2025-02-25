#!/usr/bin/env bash
set -e  # Exit immediately if a command fails

# Navigate to the airflow directory and start Astro in background
echo "Starting services..."
cd airflow
astro dev start &

# Wait for services to be ready (Airflow, MinIO, and MySQL) with retries
echo "Waiting for services to be ready..."

# Wait for Airflow (port 8080)
until curl -s http://localhost:8080 > /dev/null; do
    echo "Waiting for Airflow (port 8080)..."
    sleep 60
done
echo "Airflow is up and running!"

# Wait for MinIO (port 9000)
until curl -s http://localhost:9000 > /dev/null; do
    echo "Waiting for MinIO (port 9000)..."
    sleep 60
done
echo "MinIO is up and running!"

# Wait for MySQL (port 3306) using netcat for a more reliable check
until bash -c "echo > /dev/tcp/localhost/3306" 2>/dev/null; do
    echo "Waiting for MySQL (port 3306)..."
    sleep 60
done
echo "MySQL is up and running!"

# Return to the parent directory
cd ..
