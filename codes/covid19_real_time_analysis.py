import os
import sqlite3
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


DB_FOLDER = r"C:\Users\roway\Desktop\BD_project"
DB_PATH = os.path.join(DB_FOLDER, "covid_analysis.db")
CHECKPOINT_DIR = "C:/spark_checkpoints/covid_final"

os.makedirs(DB_FOLDER, exist_ok=True)

 # DATABASE INITIALIZATION
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor() 
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS covid_metrics (
        date TEXT,
        country TEXT,
        cumulative_total_cases REAL,
        daily_new_cases REAL,
        active_cases REAL,
        cumulative_total_deaths REAL,
        daily_new_deaths REAL,
        recovered_cases REAL,
        spread_trend_7d_avg REAL,
        hotspot_score REAL,
        predicted_new_cases_next_day REAL,
        processed_date TEXT,
        PRIMARY KEY (date, country) 
    )
    """)
    conn.commit()
    conn.close()
    print(f"Database initialized at: {DB_PATH}")

init_db()

spark = SparkSession.builder \
    .appName("Covid19_Full_Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

 # SCHEMA DEFINITION
schema = StructType([
    StructField("date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("cumulative_total_cases", DoubleType(), True),
    StructField("daily_new_cases", DoubleType(), True),
    StructField("active_cases", DoubleType(), True),
    StructField("cumulative_total_deaths", DoubleType(), True),
    StructField("daily_new_deaths", DoubleType(), True)
])

 # CORE PROCESSING LOGIC
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    incoming_pdf = batch_df.select(
        "date", "country", 
        "cumulative_total_cases", "daily_new_cases", 
        "active_cases", "cumulative_total_deaths", "daily_new_deaths",
        "processed_date"
    ).toPandas()

    incoming_pdf['date'] = pd.to_datetime(incoming_pdf['date'])


    try:
        conn = sqlite3.connect(DB_PATH)
        existing_pdf = pd.read_sql("SELECT * FROM covid_metrics", conn)
        existing_pdf['date'] = pd.to_datetime(existing_pdf['date'])
    except:
        existing_pdf = pd.DataFrame()
    finally:
        conn.close()


    full_df = pd.concat([existing_pdf, incoming_pdf])
    full_df = full_df.sort_values(by=['country', 'date'])
    full_df = full_df.drop_duplicates(subset=['date', 'country'], keep='last')
    
    # 1. Recovered Cases
    # Recovered = Total - Active - Deaths
    full_df['recovered_cases'] = (
        full_df['cumulative_total_cases'] - 
        full_df['active_cases'] - 
        full_df['cumulative_total_deaths']
    ).clip(lower=0)

    # 2. Track Spread (7-Day Moving Average)
    full_df['spread_trend_7d_avg'] = full_df.groupby('country')['daily_new_cases'].transform(
        lambda x: x.rolling(window=7, min_periods=1).mean()
    )

    # 3. Identify Hotspots (Hotspot Score)
    # Ratio: Today's Cases / 7-Day Average. >1 means accelerating spread.
    full_df['hotspot_score'] = np.where(
        full_df['spread_trend_7d_avg'] > 0, 
        full_df['daily_new_cases'] / full_df['spread_trend_7d_avg'], 
        0
    )

    # 4. Predict Future Trends (Next Day Forecast)
    # Simple rolling average of last 3 days shifted back to predict tomorrow
    full_df['predicted_new_cases_next_day'] = full_df.groupby('country')['daily_new_cases'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean().shift(-1)
    )

    # Save Full Enriched Dataset 
    conn = sqlite3.connect(DB_PATH)
    # Convert Timestamp back to string for SQLite compatibility
    full_df['date'] = full_df['date'].dt.strftime('%Y-%m-%d')
    
    # 'replace' overwrites the table with the clean, deduplicated, enriched data
    full_df.to_sql("covid_metrics", conn, if_exists="replace", index=False)
    conn.close()
    
    print(f"Batch {batch_id}: Processed and Analyzed {len(incoming_pdf)} new rows. Total DB size: {len(full_df)}.")

 
# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "covid_cleaned") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500) \
    .load()

# Parse JSON
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .na.fill(0.0) \
    .withColumn("processed_date", current_timestamp().cast("string"))

# Write Stream (Triggering the process_batch function)
query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="10 seconds") \
    .start()

spark.streams.awaitAnyTermination()