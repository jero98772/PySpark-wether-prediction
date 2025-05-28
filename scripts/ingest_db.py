import psycopg2
import pandas as pd
import boto3
from io import StringIO
import os
from datetime import datetime

# --------------- Configuration ---------------

# Database credentials (replace with your actual credentials or set via environment variables)
DB_HOST = os.getenv("DB_HOST", "your-db-host.amazonaws.com")
DB_NAME = os.getenv("DB_NAME", "weather_db")
DB_USER = os.getenv("DB_USER", "your_user")
DB_PASS = os.getenv("DB_PASS", "your_password")
DB_PORT = os.getenv("DB_PORT", "5432")

# AWS S3 config
S3_BUCKET = os.getenv("S3_BUCKET", "your-bucket-name")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Tables to export from the database
TABLES_TO_EXPORT = ["locations", "weather_readings"]

# --------------- Helper Functions ---------------

def upload_to_s3(df: pd.DataFrame, key: str):
    """
    Upload a DataFrame to S3 in CSV format.
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())

    print(f"✅ Uploaded to s3://{S3_BUCKET}/{key}")

def export_table_to_s3(table_name: str):
    """
    Connect to the PostgreSQL database, read the table, and upload to S3.
    """
    try:
        # Connect to the DB
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        print(f"🔌 Connected to PostgreSQL: {DB_NAME}")

        # Read table into pandas
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
        conn.close()

        # Create S3 path
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        s3_key = f"raw/postgres/{table_name}_{date_str}.csv"

        # Upload to S3
        upload_to_s3(df, s3_key)

    except Exception as e:
        print(f"❌ Error exporting table {table_name}: {e}")

# --------------- Main Execution ---------------

def main():
    for table in TABLES_TO_EXPORT:
        export_table_to_s3(table)

if __name__ == "__main__":
    main()
