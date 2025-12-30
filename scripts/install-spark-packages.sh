#!/bin/bash

echo "=========================================="
echo "Installing Python Packages in Spark Containers"
echo "=========================================="

# List of required packages
PACKAGES=(
    "gdelt"
    "pandas-datareader"
    "beautifulsoup4"
    "requests"
    "python-dotenv"
    "lxml"
)

# Install packages in Spark Master
echo "Installing packages in spark..."
for package in "${PACKAGES[@]}"; do
    echo "Installing $package..."
    docker exec spark-master pip install "$package"
done

# Install packages in Spark Worker (if you have workers)
echo "Installing packages in spark-worker-1..."
for package in "${PACKAGES[@]}"; do
    echo "Installing $package..."
    docker exec spark-worker-1 pip install "$package"
done

echo "Installing packages in spark-worker-2..."
for package in "${PACKAGES[@]}"; do
    echo "Installing $package..."
    docker exec spark-worker-2 pip install "$package"
done

echo "=========================================="
echo "Package installation completed!"
echo "=========================================="
echo "Installed packages:"
for package in "${PACKAGES[@]}"; do
    echo "  - $package"
done
echo ""
echo "You can now run 'make bronze' to execute the Bronze layer job."
