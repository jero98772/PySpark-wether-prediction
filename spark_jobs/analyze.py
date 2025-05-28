from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max as spark_max, min as spark_min
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# --------------- Initialize Spark ---------------

spark = SparkSession.builder \
    .appName("Analytics-WeatherPipeline") \
    .getOrCreate()

# --------------- S3 Paths (adjust as needed) ---------------

BUCKET = "your-bucket-name"

TRUSTED_INPUT = f"s3a://{BUCKET}/trusted/weather_merged/"
REFINED_DESCRIPTIVE_OUTPUT = f"s3a://{BUCKET}/refined/descriptive/"
REFINED_CLUSTERING_OUTPUT = f"s3a://{BUCKET}/refined/clustering/"

# --------------- Load Trusted Dataset ---------------

df = spark.read.parquet(TRUSTED_INPUT)
df.createOrReplaceTempView("weather")

# --------------- Descriptive Analytics (SQL) ---------------

print("📊 Running descriptive statistics...")

descriptive_df = spark.sql("""
    SELECT
        city,
        department,
        AVG(temperature_2m_max) AS avg_temp,
        MAX(temperature_2m_max) AS max_temp,
        MIN(temperature_2m_max) AS min_temp,
        AVG(precipitation_sum) AS avg_rain
    FROM weather
    GROUP BY city, department
""")

descriptive_df.write.mode("overwrite").parquet(REFINED_DESCRIPTIVE_OUTPUT)
print("✅ Descriptive stats saved.")

# --------------- Optional: Clustering (KMeans) ---------------

print("🧠 Running KMeans clustering...")

features_df = df.select("temperature_2m_max", "precipitation_sum").dropna()
assembler = VectorAssembler(inputCols=["temperature_2m_max", "precipitation_sum"], outputCol="features")
vector_df = assembler.transform(features_df)

kmeans = KMeans(k=3, seed=1)
model = kmeans.fit(vector_df)
clusters_df = model.transform(vector_df)

clusters_df.write.mode("overwrite").parquet(REFINED_CLUSTERING_OUTPUT)
print("✅ Clustering results saved.")

spark.stop()
