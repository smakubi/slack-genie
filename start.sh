#!/bin/bash

# This script sets up and runs the Databricks Genie Slack Bot

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install Python 3.10 or newer."
    exit 1
fi

# Set up virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Setting up virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Run the application
echo "Starting Databricks Genie Slack Bot..."
python app.py