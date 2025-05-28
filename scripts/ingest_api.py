# scripts/ingest_api.py
import requests
import boto3
import os
import pandas as pd
from datetime import datetime

# -------- CONFIG -------- #
LAT = 6.25
LON = -75.56
START_DATE = "2022-01-01"
END_DATE = "2022-12-31"
TIMEZONE = "America/Bogota"
PARAMS = "temperature_2m_max,precipitation_sum"
S3_BUCKET = os.environ.get("S3_BUCKET", "your-bucket-name")
RAW_PREFIX = "raw/open-meteo"
FILENAME = f"openmeteo_{START_DATE}_{END_DATE}.csv"

# -------- BUILD API URL -------- #
url = (
    f"https://archive-api.open-meteo.com/v1/archive?"
    f"latitude={LAT}&longitude={LON}&start_date={START_DATE}&end_date={END_DATE}"
    f"&daily={PARAMS}&timezone={TIMEZONE}"
)

print("Fetching data from:", url)
response = requests.get(url)
data = response.json()

# -------- PARSE TO DATAFRAME -------- #
df = pd.DataFrame({
    'date': data['daily']['time'],
    'temperature_2m_max': data['daily']['temperature_2m_max'],
    'precipitation_sum': data['daily']['precipitation_sum']
})

print("Sample data:")
print(df.head())

# -------- SAVE LOCALLY -------- #
df.to_csv(FILENAME, index=False)

# -------- UPLOAD TO S3 -------- #
s3 = boto3.client('s3')
s3.upload_file(FILENAME, S3_BUCKET, f"{RAW_PREFIX}/{FILENAME}")
print(f"Uploaded to s3://{S3_BUCKET}/{RAW_PREFIX}/{FILENAME}")
