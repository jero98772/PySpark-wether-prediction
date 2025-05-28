from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# --------------- Initialize Spark Session ---------------

spark = SparkSession.builder \
    .appName("ETL-WeatherPipeline") \
    .getOrCreate()

# --------------- S3 Paths (Adjust as needed) ---------------

BUCKET = "your-bucket-name"

# Input raw zone
RAW_API_PATH = f"s3a://{BUCKET}/raw/open-meteo/"
RAW_DB_PATH = f"s3a://{BUCKET}/raw/postgres/"

# Output trusted zone
TRUSTED_OUTPUT = f"s3a://{BUCKET}/trusted/weather_merged/"

# --------------- Read Raw Data ---------------

print("📥 Reading API data...")
api_df = spark.read.option("header", True).csv(RAW_API_PATH)

print("📥 Reading PostgreSQL data...")
db_df = spark.read.option("header", True).csv(RAW_DB_PATH)

# --------------- Clean & Join ---------------

# Example schema assumptions (you can adjust this to your real data):
# api_df has: date, location_id, temperature_2m_max, precipitation_sum
# db_df has: location_id, city, department

# Parse date and cast fields
api_df = api_df.withColumn("date", to_date("date")) \
               .withColumn("temperature_2m_max", col("temperature_2m_max").cast("double")) \
               .withColumn("precipitation_sum", col("precipitation_sum").cast("double"))

# Join on location_id (can be region, station_id, etc.)
trusted_df = api_df.join(db_df, on="location_id", how="inner")

# --------------- Write to Trusted Zone ---------------

print("💾 Writing trusted dataset to S3...")
trusted_df.write.mode("overwrite").parquet(TRUSTED_OUTPUT)

print("✅ ETL process completed.")
spark.stop()
