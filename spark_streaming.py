from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from transformers import pipeline
from elasticsearch import Elasticsearch
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SentimentAnalysisStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Session created successfully!")

# Define schema for incoming JSON data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "text-stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON data
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

print("Connected to Kafka stream!")

# Initialize sentiment analysis model (using a lightweight model)
print("Loading sentiment analysis model...")
sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
    device=-1  # CPU
)
print("Model loaded successfully!")

# Define UDF for sentiment analysis
def analyze_sentiment(text):
    if text is None or text == "":
        return json.dumps({"label": "NEUTRAL", "score": 0.0})
    
    try:
        result = sentiment_analyzer(text[:512])[0]  # Limit text length
        # Map POSITIVE/NEGATIVE to positive/negative/neutral
        label = result['label'].lower()
        score = result['score']
        
        if label == 'positive' and score > 0.6:
            final_label = 'positive'
        elif label == 'negative' and score > 0.6:
            final_label = 'negative'
        else:
            final_label = 'neutral'
            
        return json.dumps({"label": final_label, "score": float(score)})
    except Exception as e:
        print(f"Error analyzing sentiment: {e}")
        return json.dumps({"label": "error", "score": 0.0})

sentiment_udf = udf(analyze_sentiment, StringType())

# Apply sentiment analysis
result_df = parsed_df.withColumn("sentiment_raw", sentiment_udf(col("text")))

# Parse sentiment JSON
sentiment_schema = StructType([
    StructField("label", StringType(), True),
    StructField("score", StringType(), True)
])

final_df = result_df.withColumn("sentiment_data", from_json(col("sentiment_raw"), sentiment_schema)) \
    .select(
        col("id"),
        col("text"),
        col("source"),
        col("timestamp"),
        col("user_id"),
        col("sentiment_data.label").alias("sentiment"),
        col("sentiment_data.score").alias("confidence")
    )

print("Sentiment analysis pipeline configured!")

# Initialize Elasticsearch
es = Elasticsearch(
    ["http://localhost:9200"],
    request_timeout=30
)

# Create index if it doesn't exist
if not es.indices.exists(index="sentiment-analysis"):
    es.indices.create(
        index="sentiment-analysis",
        body={
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "text": {"type": "text"},
                    "source": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "sentiment": {"type": "keyword"},
                    "confidence": {"type": "float"}
                }
            }
        }
    )
    print("Elasticsearch index 'sentiment-analysis' created!")

# Function to write to Elasticsearch
def write_to_elasticsearch(batch_df, batch_id):
    print(f"Processing batch {batch_id}...")
    
    rows = batch_df.collect()
    
    for row in rows:
        doc = {
            "id": row.id,
            "text": row.text,
            "source": row.source,
            "timestamp": row.timestamp,
            "user_id": row.user_id,
            "sentiment": row.sentiment,
            "confidence": float(row.confidence) if row.confidence else 0.0
        }
        
        try:
            es.index(index="sentiment-analysis", document=doc)
            print(f"Indexed document: {row.id} - Sentiment: {row.sentiment}")
        except Exception as e:
            print(f"Error indexing document: {e}")

# Write stream to Elasticsearch
query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_elasticsearch) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

print("Streaming started! Waiting for data...")
print("Press Ctrl+C to stop...")

# Wait for termination
query.awaitTermination()