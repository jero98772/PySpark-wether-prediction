#!/bin/bash

# Exit on errors
set -e

# Variables
URL="https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m&format=csv"
FILENAME="weather.csv"
S3_BUCKET="ldzfinal"
S3_PATH="raw/api/$FILENAME"

# Download the CSV
echo "Downloading weather data..."
curl -s -o "$FILENAME" "$URL"

# Upload to S3
echo "Uploading to S3..."
aws s3 cp "$FILENAME" "s3://$S3_BUCKET/$S3_PATH"

echo "Done! Uploaded $FILENAME to s3://$S3_BUCKET/$S3_PATH"
