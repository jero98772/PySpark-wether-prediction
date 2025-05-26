from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
import requests
from datetime import datetime

# Initialize Spark Session with AWS configurations
spark = SparkSession.builder \
    .appName("WeatherAnalytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

class LocalWeatherPipeline:
    def __init__(self):
        self.spark = spark
        self.base_path = "/tmp/weather_data/"
        self.current_path = f"{self.base_path}current/"
        self.historical_path = f"{self.base_path}historical/"
        self.processed_path = f"{self.base_path}processed/"
        self.models_path = f"{self.base_path}models/"

    def ingest_data(self):
        """Simplified data ingestion from API"""
        print("🌤️ Ingesting weather data...")
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 6.25, "longitude": -75.56,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "timezone": "America/Bogota"
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            daily = data['daily']
            
            records = [{
                "date": daily['time'][i],
                "temp_max": daily['temperature_2m_max'][i],
                "temp_min": daily['temperature_2m_min'][i],
                "precipitation": daily['precipitation_sum'][i],
                "ingest_time": datetime.now().isoformat()
            } for i in range(len(daily['time']))]
            
            schema = StructType([
                StructField("date", StringType()),
                StructField("temp_max", DoubleType()),
                StructField("temp_min", DoubleType()),
                StructField("precipitation", DoubleType()),
                StructField("ingest_time", StringType())
            ])
            
            df = self.spark.createDataFrame(records, schema)
            df.write.mode("overwrite").parquet(self.current_path)
            print(f"✅ Ingested {df.count()} records")
            return df
            
        except Exception as e:
            print(f"❌ Ingestion failed: {e}")
            return None

    def process_data(self):
        """Local ETL processing"""
        print("🔄 Processing data...")
        try:
            df = spark.read.parquet(self.current_path)
            
            processed_df = df.withColumn(
                "date", to_date(col("date"))
            ).withColumn(
                "temp_avg", (col("temp_max") + col("temp_min")) / 2
            ).withColumn(
                "month", month(col("date"))
            ).fillna(0)
            
            processed_df.write.mode("overwrite").parquet(self.processed_path)
            print(f"✅ Processed {processed_df.count()} records")
            return processed_df
            
        except Exception as e:
            print(f"❌ Processing failed: {e}")
            return None

    def analyze_data(self):
        """Local data analysis"""
        print("📊 Analyzing data...")
        try:
            df = spark.read.parquet(self.processed_path)
            
            # Generate insights
            monthly_stats = df.groupBy("month").agg(
                avg("temp_avg").alias("avg_temp"),
                sum("precipitation").alias("total_rain")
            )
            
            monthly_stats.show()
            
            # Save insights
            monthly_stats.write.mode("overwrite").parquet(
                f"{self.base_path}insights/monthly_stats"
            )
            return monthly_stats
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            return None

    def train_model(self):
        """Local ML training"""
        print("🤖 Training prediction model...")
        try:
            df = spark.read.parquet(self.processed_path)
            
            # Feature engineering
            assembler = VectorAssembler(
                inputCols=["temp_max", "temp_min", "month"],
                outputCol="features"
            )
            
            rf = RandomForestRegressor(
                labelCol="temp_avg",
                numTrees=50,
                maxDepth=5
            )
            
            pipeline = Pipeline(stages=[assembler, rf])
            model = pipeline.fit(df)
            
            # Save model
            model.write().overwrite().save(f"{self.models_path}temperature_model")
            print("✅ Model trained and saved")
            return model
            
        except Exception as e:
            print(f"❌ Training failed: {e}")
            return None

def main():
    pipeline = LocalWeatherPipeline()
    
    try:
        # Run pipeline
        raw_df = pipeline.ingest_data()
        processed_df = pipeline.process_data()
        analysis = pipeline.analyze_data()
        model = pipeline.train_model()
        
        print("🎉 Pipeline completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
    
    finally:
        spark.stop()
        print("🔴 Spark session closed")

if __name__ == "__main__":
    main()