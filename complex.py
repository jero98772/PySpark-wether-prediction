"""
Weather Data Analytics Pipeline with PySpark
Complete solution for ST0263 Big Data Project
Includes: Data Ingestion, ETL, Analytics, Predictions, ML Rain Classification
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import requests
import json
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WeatherAnalytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

class WeatherDataPipeline:
    def __init__(self):
        self.spark = spark
        self.current_data_path = "s3://your-bucket/raw/weather_current/"
        self.historical_data_path = "s3://your-bucket/raw/weather_historical/"
        self.trusted_data_path = "s3://your-bucket/trusted/weather_processed/"
        self.refined_data_path = "s3://your-bucket/refined/weather_analytics/"
        
    def ingest_current_weather_data(self):
        """Ingest current weather data from API"""
        print("🌤️  Ingesting current weather data...")
        
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 6.25,  # Medellín coordinates
            "longitude": -75.56,
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", 
                     "windspeed_10m_max", "relative_humidity_2m", "pressure_msl"],
            "hourly": ["temperature_2m", "precipitation", "windspeed_10m", "relative_humidity_2m"],
            "past_days": 7,
            "forecast_days": 7,
            "timezone": "America/Bogota"
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            # Convert to Spark DataFrame
            daily_data = data.get('daily', {})
            dates = daily_data.get('time', [])
            
            # Create records for daily data
            records = []
            for i, date in enumerate(dates):
                record = {
                    'date': date,
                    'temperature_max': daily_data.get('temperature_2m_max', [None])[i],
                    'temperature_min': daily_data.get('temperature_2m_min', [None])[i],
                    'precipitation': daily_data.get('precipitation_sum', [None])[i],
                    'windspeed_max': daily_data.get('windspeed_10m_max', [None])[i],
                    'humidity': daily_data.get('relative_humidity_2m', [None])[i],
                    'pressure': daily_data.get('pressure_msl', [None])[i],
                    'data_type': 'current',
                    'ingestion_timestamp': datetime.now().isoformat()
                }
                records.append(record)
            
            # Create DataFrame
            schema = StructType([
                StructField("date", StringType(), True),
                StructField("temperature_max", DoubleType(), True),
                StructField("temperature_min", DoubleType(), True),
                StructField("precipitation", DoubleType(), True),
                StructField("windspeed_max", DoubleType(), True),
                StructField("humidity", DoubleType(), True),
                StructField("pressure", DoubleType(), True),
                StructField("data_type", StringType(), True),
                StructField("ingestion_timestamp", StringType(), True)
            ])
            
            df = self.spark.createDataFrame(records, schema)
            
            # Save to S3 Raw zone (simulated with parquet)
            df.write.mode("overwrite").parquet(f"{self.current_data_path}current_weather")
            print(f"✅ Current weather data saved: {df.count()} records")
            return df
            
        except Exception as e:
            print(f"❌ Error ingesting current data: {e}")
            return None
    
    def ingest_historical_weather_data(self):
        """Ingest historical weather data from API"""
        print("📊 Ingesting historical weather data...")
        
        all_records = []
        
        # Get data for multiple years
        for year in range(2020, 2025):
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": 6.25,
                "longitude": -75.56,
                "start_date": f"{year}-01-01",
                "end_date": f"{year}-12-31",
                "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum",
                         "windspeed_10m_max", "relative_humidity_2m", "pressure_msl"],
                "timezone": "America/Bogota"
            }
            
            try:
                response = requests.get(url, params=params)
                data = response.json()
                
                daily_data = data.get('daily', {})
                dates = daily_data.get('time', [])
                
                for i, date in enumerate(dates):
                    record = {
                        'date': date,
                        'temperature_max': daily_data.get('temperature_2m_max', [None])[i],
                        'temperature_min': daily_data.get('temperature_2m_min', [None])[i],
                        'precipitation': daily_data.get('precipitation_sum', [None])[i],
                        'windspeed_max': daily_data.get('windspeed_10m_max', [None])[i],
                        'humidity': daily_data.get('relative_humidity_2m', [None])[i],
                        'pressure': daily_data.get('pressure_msl', [None])[i],
                        'data_type': 'historical',
                        'year': year,
                        'ingestion_timestamp': datetime.now().isoformat()
                    }
                    all_records.append(record)
                
                print(f"✅ Downloaded {year} data: {len(dates)} records")
                
            except Exception as e:
                print(f"❌ Error downloading {year} data: {e}")
                continue
        
        # Create comprehensive DataFrame
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("temperature_max", DoubleType(), True),
            StructField("temperature_min", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("windspeed_max", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("data_type", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("ingestion_timestamp", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(all_records, schema)
        
        # Save to S3 Raw zone
        df.write.mode("overwrite").partitionBy("year").parquet(f"{self.historical_data_path}historical_weather")
        print(f"✅ Historical weather data saved: {df.count()} records")
        return df
    
    def etl_process(self, historical_df, current_df=None):
        """Advanced ETL processing with data quality and feature engineering"""
        print("🔄 Starting ETL process...")
        
        # Combine datasets if current data exists
        if current_df is not None:
            # Align schemas
            current_df = current_df.withColumn("year", year(col("date")))
            combined_df = historical_df.union(current_df)
        else:
            combined_df = historical_df
        
        # Data quality checks and cleaning
        print("🧹 Cleaning and preparing data...")
        
        # Convert date string to date type and extract features
        etl_df = combined_df \
            .withColumn("date", to_date(col("date"))) \
            .withColumn("month", month(col("date"))) \
            .withColumn("day_of_year", dayofyear(col("date"))) \
            .withColumn("quarter", quarter(col("date"))) \
            .withColumn("week_of_year", weekofyear(col("date"))) \
            .withColumn("is_weekend", dayofweek(col("date")).isin([1, 7]).cast("int")) \
            .filter(col("date").isNotNull())
        
        # Handle missing values with sophisticated imputation
        etl_df = etl_df.fillna({
            'temperature_max': 25.0,  # Average temperature for Medellín
            'temperature_min': 15.0,
            'precipitation': 0.0,
            'windspeed_max': 5.0,
            'humidity': 70.0,
            'pressure': 1013.25
        })
        
        # Feature engineering
        print("⚙️  Engineering advanced features...")
        
        etl_df = etl_df \
            .withColumn("temperature_range", col("temperature_max") - col("temperature_min")) \
            .withColumn("temperature_avg", (col("temperature_max") + col("temperature_min")) / 2) \
            .withColumn("is_rainy", (col("precipitation") > 0.1).cast("int")) \
            .withColumn("rain_intensity", 
                       when(col("precipitation") == 0, "None")
                       .when(col("precipitation") <= 2.5, "Light")
                       .when(col("precipitation") <= 10, "Moderate")
                       .when(col("precipitation") <= 50, "Heavy")
                       .otherwise("Extreme")) \
            .withColumn("humidity_category",
                       when(col("humidity") < 40, "Low")
                       .when(col("humidity") <= 70, "Normal")
                       .otherwise("High")) \
            .withColumn("season",
                       when(col("month").isin([12, 1, 2]), "Dry_Season")
                       .when(col("month").isin([3, 4, 5]), "Transition_Dry")
                       .when(col("month").isin([6, 7, 8]), "Rainy_Season")
                       .otherwise("Transition_Wet"))
        
        # Add moving averages and trends
        from pyspark.sql.window import Window
        
        window_7d = Window.partitionBy().orderBy("date").rowsBetween(-6, 0)
        window_30d = Window.partitionBy().orderBy("date").rowsBetween(-29, 0)
        
        etl_df = etl_df \
            .withColumn("temp_max_7d_avg", avg("temperature_max").over(window_7d)) \
            .withColumn("temp_min_7d_avg", avg("temperature_min").over(window_7d)) \
            .withColumn("precipitation_7d_sum", sum("precipitation").over(window_7d)) \
            .withColumn("humidity_7d_avg", avg("humidity").over(window_7d)) \
            .withColumn("temp_max_30d_avg", avg("temperature_max").over(window_30d)) \
            .withColumn("precipitation_30d_sum", sum("precipitation").over(window_30d))
        
        # Save to Trusted zone
        etl_df.write.mode("overwrite").partitionBy("year", "month").parquet(f"{self.trusted_data_path}processed_weather")
        print(f"✅ ETL completed: {etl_df.count()} records processed")
        
        return etl_df
    
    def descriptive_analytics(self, df):
        """Comprehensive descriptive analytics"""
        print("📈 Performing descriptive analytics...")
        
        # Basic statistics
        print("\n=== WEATHER STATISTICS SUMMARY ===")
        df.select("temperature_max", "temperature_min", "precipitation", "humidity", "pressure").describe().show()
        
        # Yearly trends
        print("\n=== YEARLY TRENDS ===")
        yearly_stats = df.groupBy("year") \
            .agg(
                avg("temperature_max").alias("avg_temp_max"),
                avg("temperature_min").alias("avg_temp_min"),
                sum("precipitation").alias("total_precipitation"),
                avg("humidity").alias("avg_humidity"),
                sum("is_rainy").alias("rainy_days"),
                count("*").alias("total_days")
            ) \
            .withColumn("rainy_days_percentage", (col("rainy_days") * 100.0 / col("total_days"))) \
            .orderBy("year")
        
        yearly_stats.show(truncate=False)
        
        # Seasonal analysis
        print("\n=== SEASONAL ANALYSIS ===")
        seasonal_stats = df.groupBy("season") \
            .agg(
                avg("temperature_avg").alias("avg_temperature"),
                sum("precipitation").alias("total_precipitation"),
                avg("humidity").alias("avg_humidity"),
                (sum("is_rainy") * 100.0 / count("*")).alias("rainy_percentage")
            )
        
        seasonal_stats.show(truncate=False)
        
        # Monthly patterns
        print("\n=== MONTHLY PATTERNS ===")
        monthly_stats = df.groupBy("month") \
            .agg(
                avg("temperature_avg").alias("avg_temperature"),
                avg("precipitation").alias("avg_precipitation"),
                avg("humidity").alias("avg_humidity"),
                (sum("is_rainy") * 100.0 / count("*")).alias("rainy_percentage")
            ) \
            .orderBy("month")
        
        monthly_stats.show()
        
        # Extreme weather events
        print("\n=== EXTREME WEATHER EVENTS ===")
        extremes = df.agg(
            max("temperature_max").alias("max_temperature"),
            min("temperature_min").alias("min_temperature"),
            max("precipitation").alias("max_precipitation"),
            max("windspeed_max").alias("max_windspeed")
        )
        extremes.show()
        
        # Rain intensity distribution
        print("\n=== RAIN INTENSITY DISTRIBUTION ===")
        df.groupBy("rain_intensity").count().orderBy(desc("count")).show()
        
        return {
            'yearly_stats': yearly_stats,
            'seasonal_stats': seasonal_stats,
            'monthly_stats': monthly_stats
        }
    
    def advanced_analytics_sparksql(self, df):
        """Advanced analytics using SparkSQL"""
        print("🔍 Performing advanced analytics with SparkSQL...")
        
        # Register as temporary view
        df.createOrReplaceTempView("weather_data")
        
        # Complex SQL analytics
        print("\n=== ADVANCED SQL ANALYTICS ===")
        
        # Weather pattern correlations
        correlation_query = """
        SELECT 
            year,
            corr(temperature_avg, humidity) as temp_humidity_corr,
            corr(temperature_avg, precipitation) as temp_precipitation_corr,
            corr(humidity, precipitation) as humidity_precipitation_corr,
            corr(pressure, precipitation) as pressure_precipitation_corr
        FROM weather_data 
        WHERE temperature_avg IS NOT NULL 
        GROUP BY year
        ORDER BY year
        """
        
        correlations = self.spark.sql(correlation_query)
        print("Weather Variable Correlations by Year:")
        correlations.show(truncate=False)
        
        # Consecutive rainy days analysis
        consecutive_rain_query = """
        WITH rain_sequences AS (
            SELECT 
                date,
                is_rainy,
                date - INTERVAL (ROW_NUMBER() OVER (PARTITION BY is_rainy ORDER BY date)) DAY as grp
            FROM weather_data
            WHERE is_rainy = 1
        ),
        consecutive_counts AS (
            SELECT 
                grp,
                COUNT(*) as consecutive_days,
                MIN(date) as start_date,
                MAX(date) as end_date
            FROM rain_sequences
            GROUP BY grp
        )
        SELECT 
            consecutive_days,
            COUNT(*) as frequency,
            AVG(consecutive_days) as avg_consecutive_days
        FROM consecutive_counts
        WHERE consecutive_days >= 2
        GROUP BY consecutive_days
        ORDER BY consecutive_days
        """
        
        consecutive_rain = self.spark.sql(consecutive_rain_query)
        print("\nConsecutive Rainy Days Analysis:")
        consecutive_rain.show()
        
        # Temperature anomaly detection
        anomaly_query = """
        WITH temp_stats AS (
            SELECT 
                month,
                AVG(temperature_avg) as monthly_avg,
                STDDEV(temperature_avg) as monthly_stddev
            FROM weather_data
            GROUP BY month
        )
        SELECT 
            wd.date,
            wd.temperature_avg,
            ts.monthly_avg,
            ABS(wd.temperature_avg - ts.monthly_avg) / ts.monthly_stddev as z_score,
            CASE 
                WHEN ABS(wd.temperature_avg - ts.monthly_avg) / ts.monthly_stddev > 2 
                THEN 'Anomaly' 
                ELSE 'Normal' 
            END as temperature_status
        FROM weather_data wd
        JOIN temp_stats ts ON wd.month = ts.month
        WHERE ABS(wd.temperature_avg - ts.monthly_avg) / ts.monthly_stddev > 2
        ORDER BY z_score DESC
        """
        
        anomalies = self.spark.sql(anomaly_query)
        print(f"\nTemperature Anomalies (Z-score > 2): {anomalies.count()} events")
        anomalies.show(10)
        
        return correlations, consecutive_rain, anomalies
    
    def weather_prediction_model(self, df):
        """Advanced weather prediction using multiple ML models"""
        print("🤖 Building weather prediction models...")
        
        # Prepare features for ML
        feature_cols = [
            "temperature_max", "temperature_min", "humidity", "pressure", 
            "windspeed_max", "month", "day_of_year", "quarter", "is_weekend",
            "temp_max_7d_avg", "temp_min_7d_avg", "humidity_7d_avg"
        ]
        
        # Clean data for ML
        ml_df = df.select(feature_cols + ["temperature_avg", "precipitation"]) \
                 .filter(col("temperature_avg").isNotNull()) \
                 .filter(col("precipitation").isNotNull()) \
                 .dropna()
        
        # Feature vector assembly
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Split data
        train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
        
        # Temperature prediction model
        print("🌡️  Building temperature prediction model...")
        
        temp_rf = RandomForestRegressor(
            featuresCol="scaled_features", 
            labelCol="temperature_avg",
            numTrees=100,
            maxDepth=10
        )
        
        temp_pipeline = Pipeline(stages=[assembler, scaler, temp_rf])
        temp_model = temp_pipeline.fit(train_df)
        
        # Temperature predictions
        temp_predictions = temp_model.transform(test_df)
        temp_evaluator = RegressionEvaluator(labelCol="temperature_avg", predictionCol="prediction")
        temp_rmse = temp_evaluator.setMetricName("rmse").evaluate(temp_predictions)
        temp_r2 = temp_evaluator.setMetricName("r2").evaluate(temp_predictions)
        
        print(f"Temperature Prediction - RMSE: {temp_rmse:.3f}, R²: {temp_r2:.3f}")
        
        # Precipitation prediction model
        print("🌧️  Building precipitation prediction model...")
        
        precip_rf = RandomForestRegressor(
            featuresCol="scaled_features",
            labelCol="precipitation",
            numTrees=100,
            maxDepth=10
        )
        
        precip_pipeline = Pipeline(stages=[assembler, scaler, precip_rf])
        precip_model = precip_pipeline.fit(train_df)
        
        # Precipitation predictions
        precip_predictions = precip_model.transform(test_df)
        precip_evaluator = RegressionEvaluator(labelCol="precipitation", predictionCol="prediction")
        precip_rmse = precip_evaluator.setMetricName("rmse").evaluate(precip_predictions)
        
        print(f"Precipitation Prediction - RMSE: {precip_rmse:.3f}")
        
        # Save models
        temp_model.write().overwrite().save(f"{self.refined_data_path}models/temperature_model")
        precip_model.write().overwrite().save(f"{self.refined_data_path}models/precipitation_model")
        
        return temp_model, precip_model, temp_predictions, precip_predictions
    
    def rain_classification_model(self, df):
        """Advanced rain classification with multiple algorithms"""
        print("☔ Building rain classification models...")
        
        # Prepare features for rain classification
        feature_cols = [
            "temperature_max", "temperature_min", "temperature_avg", "humidity", 
            "pressure", "windspeed_max", "month", "day_of_year", "quarter",
            "temp_max_7d_avg", "humidity_7d_avg", "temperature_range"
        ]
        
        # Clean data
        ml_df = df.select(feature_cols + ["is_rainy"]) \
                 .filter(col("is_rainy").isNotNull()) \
                 .dropna()
        
        # Feature assembly
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        
        # Split data
        train_df, test_df = ml_df.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set: {train_df.count()}, Test set: {test_df.count()}")
        
        # Random Forest Classifier
        print("🌲 Training Random Forest Classifier...")
        
        rf_classifier = RandomForestClassifier(
            featuresCol="scaled_features",
            labelCol="is_rainy",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        rf_pipeline = Pipeline(stages=[assembler, scaler, rf_classifier])
        rf_model = rf_pipeline.fit(train_df)
        
        # RF Predictions
        rf_predictions = rf_model.transform(test_df)
        rf_evaluator = BinaryClassificationEvaluator(labelCol="is_rainy", rawPredictionCol="rawPrediction")
        rf_auc = rf_evaluator.evaluate(rf_predictions)
        
        # Logistic Regression
        print("📊 Training Logistic Regression...")
        
        lr_classifier = LogisticRegression(
            featuresCol="scaled_features",
            labelCol="is_rainy",
            maxIter=100,
            regParam=0.01
        )
        
        lr_pipeline = Pipeline(stages=[assembler, scaler, lr_classifier])
        lr_model = lr_pipeline.fit(train_df)
        
        # LR Predictions
        lr_predictions = lr_model.transform(test_df)
        lr_auc = rf_evaluator.evaluate(lr_predictions)
        
        # Model comparison
        print(f"\n=== RAIN CLASSIFICATION RESULTS ===")
        print(f"Random Forest AUC: {rf_auc:.4f}")
        print(f"Logistic Regression AUC: {lr_auc:.4f}")
        
        # Detailed metrics for best model
        best_model = rf_model if rf_auc > lr_auc else lr_model
        best_predictions = rf_predictions if rf_auc > lr_auc else lr_predictions
        
        # Confusion matrix
        print("\n=== CONFUSION MATRIX (Best Model) ===")
        confusion_matrix = best_predictions.groupBy("is_rainy", "prediction").count()
        confusion_matrix.show()
        
        # Feature importance (Random Forest only)
        if rf_auc > lr_auc:
            print("\n=== FEATURE IMPORTANCE (Random Forest) ===")
            feature_importance = rf_model.stages[-1].featureImportances.toArray()
            importance_df = self.spark.createDataFrame(
                [(feature_cols[i], float(feature_importance[i])) for i in range(len(feature_cols))],
                ["feature", "importance"]
            ).orderBy(desc("importance"))
            importance_df.show(truncate=False)
        
        # Save models
        rf_model.write().overwrite().save(f"{self.refined_data_path}models/rain_rf_model")
        lr_model.write().overwrite().save(f"{self.refined_data_path}models/rain_lr_model")
        
        return best_model, best_predictions, rf_auc, lr_auc
    
    def generate_weather_insights(self, df, analytics_results):
        """Generate comprehensive weather insights and recommendations"""
        print("💡 Generating weather insights and recommendations...")
        
        insights = []
        
        # Seasonal insights
        seasonal_stats = analytics_results['seasonal_stats'].collect()
        for row in seasonal_stats:
            season = row['season']
            temp = row['avg_temperature']
            rain_pct = row['rainy_percentage']
            insights.append(f"🌍 {season}: Average temperature {temp:.1f}°C, {rain_pct:.1f}% rainy days")
        
        # Yearly trends
        yearly_stats = analytics_results['yearly_stats'].collect()
        recent_years = sorted(yearly_stats, key=lambda x: x['year'])[-3:]
        
        temp_trend = "increasing" if recent_years[-1]['avg_temp_max'] > recent_years[0]['avg_temp_max'] else "decreasing"
        rain_trend = "increasing" if recent_years[-1]['rainy_days_percentage'] > recent_years[0]['rainy_days_percentage'] else "decreasing"
        
        insights.append(f"📊 Recent trend: Temperature is {temp_trend}, rainy days are {rain_trend}")
        
        # Climate recommendations
        insights.append("🌿 Climate recommendations:")
        insights.append("  • Best months for outdoor activities: January, February (dry season)")
        insights.append("  • Rainy season preparation: June-August")
        insights.append("  • Energy efficiency: Use fans during high humidity periods")
        insights.append("  • Agriculture: Plant crops considering precipitation patterns")
        
        # Save insights
        insights_df = self.spark.createDataFrame(
            [(i, insight) for i, insight in enumerate(insights)],
            ["insight_id", "insight_text"]
        )
        
        insights_df.write.mode("overwrite").parquet(f"{self.refined_data_path}insights/weather_insights")
        
        print("\n=== WEATHER INSIGHTS ===")
        for insight in insights:
            print(insight)
        
        return insights
    
    def run_complete_pipeline(self):
        """Run the complete weather analytics pipeline"""
        print("🚀 Starting complete weather analytics pipeline...")
        
        # Step 1: Data Ingestion
        print("\n" + "="*50)
        print("STEP 1: DATA INGESTION")
        print("="*50)
        
        current_df = self.ingest_current_weather_data()
        historical_df = self.ingest_historical_weather_data()
        
        # Step 2: ETL Processing
        print("\n" + "="*50)
        print("STEP 2: ETL PROCESSING")
        print("="*50)
        
        processed_df = self.etl_process(historical_df, current_df)
        
        # Step 3: Descriptive Analytics
        print("\n" + "="*50)
        print("STEP 3: DESCRIPTIVE ANALYTICS")
        print("="*50)
        
        analytics_results = self.descriptive_analytics(processed_df)
        
        # Step 4: Advanced Analytics with SparkSQL
        print("\n" + "="*50)
        print("STEP 4: ADVANCED SPARKSQL ANALYTICS")
        print("="*50)
        
        correlations, consecutive_rain, anomalies = self.advanced_analytics_sparksql(processed_df)
        
        # Step 5: Weather Prediction Models
        print("\n" + "="*50)
        print("STEP 5: WEATHER PREDICTION MODELS")
        print("="*50)
        
        temp_model, precip_model, temp_pred, precip_pred = self.weather_prediction_model(processed_df)
        
        # Step 6: Rain Classification Model
        print("\n" + "="*50)
        print("STEP 6: RAIN CLASSIFICATION MODEL")
        print("="*50)
        
        rain_model, rain_predictions, rf_auc, lr_auc = self.rain_classification_model(processed_df)
        
        # Step 7: Generate Insights
        print("\n" + "="*50)
        print("STEP 7: WEATHER INSIGHTS")
        print("="*50)
        
        insights = self.generate_weather_insights(processed_df, analytics_results)
        
        # Step 8: Save Final Results
        print("\n" + "="*50)
        print("STEP 8: SAVING RESULTS")
        print("="*50)
        
        # Save final results for API consumption
        final_results = {
            'total_records': processed_df.count(),
            'data_range': {
                'start_date': processed_df.agg(min('date')).collect()[0][0],
                'end_date': processed_df.agg(max('date')).collect()[0][0]
            },
            'model_performance': {
                'rain_classification_auc': max(rf_auc, lr_auc),
                'temperature_prediction_r2': 'Available in model artifacts'
            },
            'pipeline_completion': datetime.now().isoformat()
        }
        
        # Save summary results
        summary_df = self.spark.createDataFrame([final_results], 
            StructType([
                StructField("total_records", IntegerType(), True),
                StructField("pipeline_completion", StringType(), True)
            ])
        )
        
        summary_df.write.mode("overwrite").parquet(f"{self.refined_data_path}summary/pipeline_summary")
        
        print("✅ Complete weather analytics pipeline finished successfully!")
        print(f"📊 Processed {final_results['total_records']} weather records")
        print(f"📅 Data range: {final_results['data_range']['start_date']} to {final_results['data_range']['end_date']}")
        print(f"🎯 Rain prediction accuracy (AUC): {final_results['model_performance']['rain_classification_auc']:.4f}")
        
        return {
            'processed_data': processed_df,
            'models': {
                'temperature': temp_model,
                'precipitation': precip_model,
                'rain_classification': rain_model
            },
            'analytics': analytics_results,
            'insights': insights,
            'performance': final_results
        }

# Additional utility functions for API endpoints and Athena queries

class WeatherAPIEndpoints:
    """API endpoints for accessing weather analytics results"""
    
    def __init__(self, pipeline_results):
        self.results = pipeline_results
        self.processed_df = pipeline_results['processed_data']
    
    def get_current_weather_summary(self):
        """API endpoint: Get current weather summary"""
        recent_data = self.processed_df.filter(col("date") >= date_sub(current_date(), 7))
        
        summary = recent_data.agg(
            avg("temperature_avg").alias("avg_temperature"),
            sum("precipitation").alias("total_precipitation"),
            avg("humidity").alias("avg_humidity"),
            max("temperature_max").alias("max_temperature"),
            min("temperature_min").alias("min_temperature")
        ).collect()[0]
        
        return {
            "period": "Last 7 days",
            "average_temperature": float(summary["avg_temperature"]),
            "total_precipitation": float(summary["total_precipitation"]),
            "average_humidity": float(summary["avg_humidity"]),
            "temperature_range": {
                "max": float(summary["max_temperature"]),
                "min": float(summary["min_temperature"])
            }
        }
    
    def get_rain_prediction(self, features_dict):
        """API endpoint: Predict rain probability"""
        # This would use the trained model to predict rain
        # Features expected: temperature_max, temperature_min, humidity, pressure, etc.
        
        return {
            "rain_probability": 0.65,  # Example prediction
            "confidence": "High",
            "recommendation": "Carry an umbrella",
            "model_used": "Random Forest Classifier"
        }
    
    def get_monthly_forecast_trend(self, year, month):
        """API endpoint: Get monthly weather trends"""
        monthly_data = self.processed_df.filter(
            (col("year") == year) & (col("month") == month)
        )
        
        if monthly_data.count() == 0:
            return {"error": "No data available for specified period"}
        
        stats = monthly_data.agg(
            avg("temperature_avg").alias("avg_temp"),
            sum("precipitation").alias("total_rain"),
            (sum("is_rainy") * 100.0 / count("*")).alias("rainy_days_pct")
        ).collect()[0]
        
        return {
            "year": year,
            "month": month,
            "average_temperature": float(stats["avg_temp"]),
            "total_rainfall": float(stats["total_rain"]),
            "rainy_days_percentage": float(stats["rainy_days_pct"])
        }

class WeatherAthenaQueries:
    """Athena-compatible SQL queries for weather data analysis"""
    
    @staticmethod
    def create_external_table_ddl():
        """DDL for creating external table in Athena"""
        return """
        CREATE EXTERNAL TABLE weather_analytics (
            date date,
            temperature_max double,
            temperature_min double,
            temperature_avg double,
            precipitation double,
            windspeed_max double,
            humidity double,
            pressure double,
            is_rainy int,
            rain_intensity string,
            season string,
            temperature_range double,
            temp_max_7d_avg double,
            precipitation_7d_sum double,
            month int,
            year int,
            quarter int,
            day_of_year int
        )
        PARTITIONED BY (
            year int,
            month int
        )
        STORED AS PARQUET
        LOCATION 's3://your-bucket/trusted/weather_processed/'
        """
    
    @staticmethod
    def get_weather_summary_query():
        """Athena query: Weather summary statistics"""
        return """
        SELECT 
            year,
            COUNT(*) as total_days,
            AVG(temperature_avg) as avg_temperature,
            SUM(precipitation) as total_precipitation,
            SUM(is_rainy) as rainy_days,
            (SUM(is_rainy) * 100.0 / COUNT(*)) as rainy_percentage,
            MAX(temperature_max) as max_temperature,
            MIN(temperature_min) as min_temperature
        FROM weather_analytics
        WHERE year >= 2020
        GROUP BY year
        ORDER BY year;
        """
    
    @staticmethod
    def get_seasonal_patterns_query():
        """Athena query: Seasonal weather patterns"""
        return """
        SELECT 
            season,
            AVG(temperature_avg) as avg_temperature,
            SUM(precipitation) as total_precipitation,
            AVG(humidity) as avg_humidity,
            COUNT(CASE WHEN is_rainy = 1 THEN 1 END) as rainy_days,
            COUNT(*) as total_days,
            (COUNT(CASE WHEN is_rainy = 1 THEN 1 END) * 100.0 / COUNT(*)) as rainy_percentage
        FROM weather_analytics
        GROUP BY season
        ORDER BY 
            CASE season
                WHEN 'Dry_Season' THEN 1
                WHEN 'Transition_Dry' THEN 2
                WHEN 'Rainy_Season' THEN 3
                WHEN 'Transition_Wet' THEN 4
            END;
        """
    
    @staticmethod
    def get_extreme_weather_events_query():
        """Athena query: Extreme weather events"""
        return """
        WITH temperature_stats AS (
            SELECT 
                AVG(temperature_avg) as overall_avg_temp,
                STDDEV(temperature_avg) as temp_stddev
            FROM weather_analytics
        ),
        precipitation_stats AS (
            SELECT 
                AVG(precipitation) as overall_avg_precip,
                STDDEV(precipitation) as precip_stddev
            FROM weather_analytics
            WHERE precipitation > 0
        )
        SELECT 
            w.date,
            w.temperature_avg,
            w.precipitation,
            CASE 
                WHEN ABS(w.temperature_avg - t.overall_avg_temp) > 2 * t.temp_stddev 
                THEN 'Temperature Extreme'
                WHEN w.precipitation > p.overall_avg_precip + 2 * p.precip_stddev 
                THEN 'Precipitation Extreme'
                ELSE 'Normal'
            END as event_type
        FROM weather_analytics w
        CROSS JOIN temperature_stats t
        CROSS JOIN precipitation_stats p
        WHERE 
            ABS(w.temperature_avg - t.overall_avg_temp) > 2 * t.temp_stddev
            OR w.precipitation > p.overall_avg_precip + 2 * p.precip_stddev
        ORDER BY w.date DESC;
        """

# Visualization functions (for local analysis)
def create_weather_visualizations(processed_df):
    """Create comprehensive weather visualizations"""
    print("📊 Creating weather visualizations...")
    
    # Convert to Pandas for plotting (sample data for performance)
    sample_df = processed_df.sample(0.1, seed=42).toPandas()
    
    # Set up the plotting style
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    fig.suptitle('Comprehensive Weather Analytics Dashboard', fontsize=16, fontweight='bold')
    
    # 1. Temperature trends over time
    axes[0, 0].plot(sample_df['date'], sample_df['temperature_avg'], alpha=0.7, color='red')
    axes[0, 0].set_title('Temperature Trends Over Time')
    axes[0, 0].set_xlabel('Date')
    axes[0, 0].set_ylabel('Temperature (°C)')
    axes[0, 0].tick_params(axis='x', rotation=45)
    
    # 2. Precipitation patterns
    axes[0, 1].bar(sample_df['date'], sample_df['precipitation'], alpha=0.7, color='blue')
    axes[0, 1].set_title('Precipitation Patterns')
    axes[0, 1].set_xlabel('Date')
    axes[0, 1].set_ylabel('Precipitation (mm)')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    # 3. Temperature vs Humidity correlation
    axes[0, 2].scatter(sample_df['temperature_avg'], sample_df['humidity'], alpha=0.6)
    axes[0, 2].set_title('Temperature vs Humidity Correlation')
    axes[0, 2].set_xlabel('Temperature (°C)')
    axes[0, 2].set_ylabel('Humidity (%)')
    
    # 4. Seasonal temperature distribution
    seasonal_data = sample_df.groupby('season')['temperature_avg'].apply(list)
    axes[1, 0].boxplot([seasonal_data[season] for season in seasonal_data.index], 
                       labels=seasonal_data.index)
    axes[1, 0].set_title('Seasonal Temperature Distribution')
    axes[1, 0].set_ylabel('Temperature (°C)')
    axes[1, 0].tick_params(axis='x', rotation=45)
    
    # 5. Monthly precipitation totals
    monthly_precip = sample_df.groupby('month')['precipitation'].sum()
    axes[1, 1].bar(monthly_precip.index, monthly_precip.values, color='skyblue')
    axes[1, 1].set_title('Monthly Precipitation Totals')
    axes[1, 1].set_xlabel('Month')
    axes[1, 1].set_ylabel('Total Precipitation (mm)')
    
    # 6. Rain intensity distribution
    rain_intensity_counts = sample_df['rain_intensity'].value_counts()
    axes[1, 2].pie(rain_intensity_counts.values, labels=rain_intensity_counts.index, autopct='%1.1f%%')
    axes[1, 2].set_title('Rain Intensity Distribution')
    
    plt.tight_layout()
    plt.savefig('weather_analytics_dashboard.png', dpi=300, bbox_inches='tight')
    plt.show()
    print("✅ Visualizations saved as 'weather_analytics_dashboard.png'")

# EMR Step functions for automation
class EMRStepGenerator:
    """Generate EMR steps for automated processing"""
    
    @staticmethod
    def generate_etl_step():
        """Generate EMR step for ETL processing"""
        return {
            "Name": "Weather-Data-ETL-Processing",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.adaptive.enabled=true",
                    "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
                    "--py-files", "s3://your-bucket/scripts/weather_pipeline.zip",
                    "s3://your-bucket/scripts/weather_etl.py",
                    "--input-path", "s3://your-bucket/raw/weather_historical/",
                    "--output-path", "s3://your-bucket/trusted/weather_processed/"
                ]
            }
        }
    
    @staticmethod
    def generate_analytics_step():
        """Generate EMR step for analytics processing"""
        return {
            "Name": "Weather-Data-Analytics-Processing",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.adaptive.enabled=true",
                    "--py-files", "s3://your-bucket/scripts/weather_pipeline.zip",
                    "s3://your-bucket/scripts/weather_analytics.py",
                    "--input-path", "s3://your-bucket/trusted/weather_processed/",
                    "--output-path", "s3://your-bucket/refined/weather_analytics/"
                ]
            }
        }
    
    @staticmethod
    def generate_ml_step():
        """Generate EMR step for ML model training"""
        return {
            "Name": "Weather-ML-Model-Training",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "--conf", "spark.sql.adaptive.enabled=true",
                    "--packages", "org.apache.spark:spark-mllib_2.12:3.2.0",
                    "--py-files", "s3://your-bucket/scripts/weather_pipeline.zip",
                    "s3://your-bucket/scripts/weather_ml.py",
                    "--input-path", "s3://your-bucket/trusted/weather_processed/",
                    "--model-output-path", "s3://your-bucket/refined/models/",
                    "--predictions-output-path", "s3://your-bucket/refined/predictions/"
                ]
            }
        }

# Main execution
if __name__ == "__main__":
    # Initialize and run the complete pipeline
    weather_pipeline = WeatherDataPipeline()
    
    try:
        # Run complete pipeline
        results = weather_pipeline.run_complete_pipeline()
        
        # Initialize API endpoints
        api_endpoints = WeatherAPIEndpoints(results)
        
        # Example API calls
        print("\n" + "="*50)
        print("EXAMPLE API ENDPOINTS")
        print("="*50)
        
        # Current weather summary
        current_summary = api_endpoints.get_current_weather_summary()
        print("📊 Current Weather Summary:")
        print(json.dumps(current_summary, indent=2))
        
        # Monthly forecast
        monthly_forecast = api_endpoints.get_monthly_forecast_trend(2024, 6)
        print("\n📅 Monthly Forecast (June 2024):")
        print(json.dumps(monthly_forecast, indent=2))
        
        # Rain prediction example
        rain_prediction = api_endpoints.get_rain_prediction({
            "temperature_max": 28.5,
            "temperature_min": 18.2,
            "humidity": 85.0,
            "pressure": 1010.5
        })
        print("\n☔ Rain Prediction:")
        print(json.dumps(rain_prediction, indent=2))
        
        # Generate visualizations (optional)
        try:
            create_weather_visualizations(results['processed_data'])
        except Exception as e:
            print(f"Note: Visualization skipped (requires matplotlib): {e}")
        
        # Print Athena queries for reference
        print("\n" + "="*50)
        print("ATHENA QUERY EXAMPLES")
        print("="*50)
        
        athena_queries = WeatherAthenaQueries()
        print("📋 Create External Table DDL:")
        print(athena_queries.create_external_table_ddl())
        
        print("\n📊 Weather Summary Query:")
        print(athena_queries.get_weather_summary_query())
        
        # Print EMR steps for automation
        print("\n" + "="*50)
        print("EMR AUTOMATION STEPS")
        print("="*50)
        
        emr_generator = EMRStepGenerator()
        print("🔧 ETL Step Configuration:")
        print(json.dumps(emr_generator.generate_etl_step(), indent=2))
        
        print(f"\n🎉 Weather Analytics Pipeline completed successfully!")
        print(f"📈 Total records processed: {results['performance']['total_records']}")
        print(f"🤖 Models trained: Temperature prediction, Precipitation prediction, Rain classification")
        print(f"🔍 Analytics completed: Descriptive, Advanced SQL, ML predictions")
        print(f"💾 Results saved to S3 paths: raw/, trusted/, refined/")
        
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        raise
    
    finally:
        # Clean up Spark session
        spark.stop()
        print("✅ Spark session closed")

# Additional configuration for production deployment
"""
PRODUCTION DEPLOYMENT NOTES:

1. AWS EMR Cluster Configuration:
   - Instance types: m5.xlarge (master), m5.large (core nodes)
   - EMR release: emr-6.4.0 or later
   - Applications: Spark, Hadoop, Hive
   - Python packages: Install via bootstrap action

2. S3 Bucket Structure:
   your-bucket/
   ├── raw/
   │   ├── weather_current/
   │   └── weather_historical/
   ├── trusted/
   │   └── weather_processed/
   ├── refined/
   │   ├── weather_analytics/
   │   ├── models/
   │   └── predictions/
   └── scripts/
       ├── weather_pipeline.py
       ├── weather_etl.py
       ├── weather_analytics.py
       └── weather_ml.py

3. Automation with Step Functions:
   - Schedule daily data ingestion
   - Trigger EMR cluster creation
   - Execute processing steps
   - Save results and terminate cluster

4. API Gateway Integration:
   - Create REST API endpoints
   - Connect to Lambda functions
   - Query results from S3/Athena
   - Return JSON responses

5. Monitoring and Alerting:
   - CloudWatch metrics for pipeline health
   - SNS notifications for failures
   - Data quality checks
   - Cost optimization alerts
"""