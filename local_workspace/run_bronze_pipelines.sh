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

echo "Running Bronze scripts..."
for script in jobs/bronze/*.py; do
    echo "Running Bronze script: $script"
    poetry run python "$script"
done

echo "Bronze pipeline executed successfully...."