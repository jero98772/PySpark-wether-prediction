import requests
import sys
import tempfile
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Weather Data Analysis from API") \
        .getOrCreate()

    # URL of the weather CSV
    api_url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m&format=csv"

    # Download the CSV from the API and store it in a temp file
    response = requests.get(api_url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data from API. HTTP Status Code: {response.status_code}")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp_file:
        tmp_file.write(response.content)
        temp_path = tmp_file.name

    # Load CSV using PySpark
    df = spark.read.csv(temp_path, header=True, inferSchema=True)

    # OPTIONAL: Rename column to make it easier to handle
    df = df.withColumnRenamed("temperature_2m (°C)", "temperature")

    # Simple analysis: average temperature per hour
    analysis = df.groupBy("time").avg("temperature")

    # Show the result
    analysis.show()

    # Write result to Parquet (target path comes from command line)
    analysis.write.mode("overwrite").parquet(sys.argv[1])

    spark.stop()
