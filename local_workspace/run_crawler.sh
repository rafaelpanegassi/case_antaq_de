#!/usr/bin/env bash
set -e  # Exit immediately if a command fails

echo "Running Crawler script..."
poetry run python jobs/crawlers/crawler_antaq.py

echo "Crawler script executed successfully...."
