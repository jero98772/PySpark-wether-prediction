from pyspark.sql import SparkSession
import sys

if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Weather Data Analysis") \
        .getOrCreate()

    # Read CSV file passed as first argument
    raw_data = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

    # Example analysis: Average temperature by hour
    analysis = raw_data.groupBy("time").avg("temperature")

    # Show sample output
    analysis.show()

    # Write result to Parquet (second argument)
    analysis.write.mode("overwrite").parquet(sys.argv[2])

    # Stop Spark
    spark.stop()
