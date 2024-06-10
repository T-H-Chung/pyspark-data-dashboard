# import findspark
# findspark.init("/home/ubuntu/spark-3.5.0-bin-hadoop3")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, TimestampType
from pyspark.sql.functions import from_json, col, window, first, last, min, max, regexp_replace, avg, desc
from websocket import create_connection
import json
import os
import time
import threading
import pandas as pd
import traceback

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell'
BTC_URL = "jdbc:postgresql://localhost:5432/btc"
BTC_PROPERTIES = {"user": "coin_user", "password": "coin_password", "driver": "org.postgresql.Driver"}
ETH_URL = "jdbc:postgresql://localhost:5432/eth"
ETH_PROPERTIES = {"user": "coin_user", "password": "coin_password", "driver": "org.postgresql.Driver"}

def send_to_websocket_server(row):
    ws_url = "ws://localhost:8000/ws"
    data = row.asDict()
    try:
        ws = create_connection(ws_url)
        ws.send(json.dumps(data))
        ws.close()
    except Exception as e:
        print(f"Error sending data: {e}")
        
spark = SparkSession \
    .builder \
    .appName("WebSocketStructuredStreaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.jars", "./postgresql-42.7.0.jar") \
    .getOrCreate()

# Define the schema corresponding to the WebSocket message structure
schema = StructType([
    StructField("type", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("open_24h", StringType(), True),
    StructField("volume_24h", StringType(), True),
    StructField("low_24h", StringType(), True),
    StructField("high_24h", StringType(), True),
    StructField("volume_30d", StringType(), True),
    StructField("best_bid", StringType(), True),
    StructField("best_bid_size", StringType(), True),
    StructField("best_ask", StringType(), True),
    StructField("best_ask_size", StringType(), True),
    StructField("side", StringType(), True),
    StructField("time", StringType(), True),
    StructField("trade_id", StringType(), True),
    StructField("last_size", StringType(), True)
])

# Read the stream in JSON format
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "coinbase_feed") \
    .load() \
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", "")) \
    .withColumn("value", regexp_replace(col("value"), "^\"|\"$", "")) \
    
# Convert the streaming text data to JSON
json_df = lines.select(from_json(col("value"), schema).alias("data")).select("data.*")


df_converted = json_df.select(
    col("type"),
    col("sequence").cast(LongType()),
    col("product_id"),
    col("price").cast(FloatType()),
    col("open_24h").cast(FloatType()),
    col("volume_24h").cast(FloatType()),
    col("low_24h").cast(FloatType()),
    col("high_24h").cast(FloatType()),
    col("volume_30d").cast(FloatType()),
    col("best_bid").cast(FloatType()),
    col("best_bid_size").cast(FloatType()),
    col("best_ask").cast(FloatType()),
    col("best_ask_size").cast(FloatType()),
    col("side"),
    col("time").cast(TimestampType()),
    col("trade_id").cast(LongType()),
    col("last_size").cast(FloatType())
)
        
def read_from_postgres(db_url, properties, table_name):
    query = f"(SELECT * FROM {table_name} ORDER BY end_time DESC LIMIT 3) AS latest_three_rows"
    return spark.read.jdbc(url=db_url, table=query, properties=properties)

def jdbc_read_function():
    time.sleep(180) # Sleep for 3 minute letting stream data collected
    try:
        while True:
            df_btc = read_from_postgres(BTC_URL, BTC_PROPERTIES, "btc_ohlc")
            df_eth = read_from_postgres(ETH_URL, ETH_PROPERTIES, "eth_ohlc")
            if df_btc.count() >= 3 and df_eth.count() >= 3:
                # Calculate the ratio of the mean close prices
                btc_avg = df_btc.select(avg("close").alias("avg_close")).collect()[0]["avg_close"]
                eth_avg = df_eth.select(avg("close").alias("avg_close")).collect()[0]["avg_close"]
                ratio = btc_avg / eth_avg
                last_window_time = df_btc.orderBy(desc("end_time")).select("end_time").first()["end_time"]
                
                # Build spark DataFrame
                new_df = pd.DataFrame({'product_id': ["BTC-ETH"], 'time': [last_window_time], 'ratio': [ratio]})
                new_spark_df = spark.createDataFrame(new_df)

                # Write the new Spark DataFrame to a PostgreSQL table
                new_spark_df.write.format("jdbc") \
                    .options(**BTC_PROPERTIES) \
                    .option("url", BTC_URL) \
                    .option("dbtable", "btc_eth") \
                    .mode("append") \
                    .save()

            time.sleep(60) # Sleep for 1 minute

    except Exception as e:
        print(f"Error during database query or write: {e}")
        traceback.print_exc()
        
def write_db_btc(df, epoch_id):
    try:
        df.write.jdbc(url=BTC_URL, table="btc_ohlc", mode="append", properties=BTC_PROPERTIES)
    except Exception as e:
        print(f"Error writing to btc: {e}")

def write_db_eth(df, epoch_id):
    try:
        df.write.jdbc(url=ETH_URL, table="eth_ohlc", mode="append", properties=ETH_PROPERTIES)
    except Exception as e:
        print(f"Error writing to eth: {e}")

# Apply a 5-minute watermark
dfWithWatermark = df_converted.withWatermark("time", "5 minutes")

# Windowed aggregation to calculate OHLC
ohlcv = dfWithWatermark \
    .groupBy(window(col("time"), "1 minute"), col("product_id")) \
    .agg(
        first("price").alias("open"),
        min("price").alias("low"),
        max("price").alias("high"),
        last("price").alias("close")
    ) \
    .select(
        col("product_id"),
        col("window.start").cast("string").alias("start_time"),
        col("window.end").cast("string").alias("end_time"),
        col("open"),
        col("low"),
        col("high"),
        col("close")
    )

# Filter the DataFrame
ohlcv_btc = ohlcv.filter(col("product_id") == 'BTC-USD')
ohlcv_eth = ohlcv.filter(col("product_id") == 'ETH-USD')
# Start running the query that prints the results to the console
ohlcv_query = ohlcv_btc.writeStream.outputMode("update").foreach(send_to_websocket_server).start()
# Write the stream to  persistent storage with trigger
query_btc = ohlcv_btc.writeStream.outputMode("update").foreachBatch(write_db_btc).trigger(processingTime='1 minute').start()
query_eth = ohlcv_eth.writeStream.outputMode("update").foreachBatch(write_db_eth).trigger(processingTime='1 minute').start()

# Await termination of the queries
jdbc_thread = threading.Thread(target=jdbc_read_function)
jdbc_thread.start()

spark.streams.awaitAnyTermination()
# query.awaitTermination()
# ohlcv_query.awaitTermination()
jdbc_thread.join()

