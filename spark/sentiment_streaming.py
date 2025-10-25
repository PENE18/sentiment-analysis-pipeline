from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from transformers import pipeline
from elasticsearch import Elasticsearch
import time

print("="*70)
print("🚀 SPARK SENTIMENT ANALYSIS STREAMING")
print("="*70)

# Wait for services to be ready
print("⏳ Waiting for Kafka and Elasticsearch to be ready...")
time.sleep(15)

# Initialize Spark Session
print("📊 Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("SentimentAnalysisStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session created successfully!")

# Define schema for incoming JSON data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("source", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True)
])

# Read from Kafka
print("📡 Connecting to Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "text-stream") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
print("✅ Connected to Kafka stream!")

# Initialize sentiment analysis model
print("⏳ Loading sentiment analysis model (this may take a few minutes)...")
try:
    sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english",
        device=-1  # CPU
    )
    print("✅ Model loaded successfully!")
except Exception as e:
    print(f"❌ Error loading model: {e}")
    exit(1)

# Initialize Elasticsearch
print("🔌 Connecting to Elasticsearch...")
max_retries = 5
for attempt in range(max_retries):
    try:
        es = Elasticsearch(
            ["http://elasticsearch:9200"],
            request_timeout=30
        )
        
        if es.ping():
            print("✅ Connected to Elasticsearch!")
            break
    except Exception as e:
        if attempt < max_retries - 1:
            print(f"⏳ Elasticsearch not ready, retrying... ({attempt + 1}/{max_retries})")
            time.sleep(5)
        else:
            print(f"❌ Cannot connect to Elasticsearch: {e}")
            exit(1)

# Create index if it doesn't exist
try:
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
        print("✅ Elasticsearch index 'sentiment-analysis' created!")
    else:
        print("✅ Using existing Elasticsearch index 'sentiment-analysis'")
except Exception as e:
    print(f"⚠️  Warning: Could not create index: {e}")

print("✅ Sentiment analysis pipeline configured!")

# Function to process batch and analyze sentiment
def process_batch(batch_df, batch_id):
    print(f"\n{'='*70}")
    print(f"📦 Processing batch {batch_id}")
    print(f"{'='*70}")
    
    if batch_df.isEmpty():
        print("   ⏭️  Batch is empty, waiting for data...")
        return
    
    # Collect rows to process
    rows = batch_df.collect()
    print(f"   📊 Found {len(rows)} messages to analyze")
    
    success_count = 0
    error_count = 0
    sentiment_counts = {"positive": 0, "negative": 0, "neutral": 0}
    
    for row in rows:
        text = row.text
        
        # Analyze sentiment
        if text and len(text.strip()) > 0:
            try:
                result = sentiment_analyzer(text[:512])[0]
                
                label = result['label'].lower()
                score = result['score']
                
                if label == 'positive' and score > 0.6:
                    final_label = 'positive'
                elif label == 'negative' and score > 0.6:
                    final_label = 'negative'
                else:
                    final_label = 'neutral'
                
                sentiment = final_label
                confidence = float(score)
                sentiment_counts[sentiment] += 1
                
            except Exception as e:
                print(f"   ❌ Error analyzing sentiment: {e}")
                sentiment = "error"
                confidence = 0.0
                error_count += 1
        else:
            sentiment = "neutral"
            confidence = 0.0
        
        # Create document for Elasticsearch
        doc = {
            "id": row.id,
            "text": text[:500] if text else "",
            "source": row.source,
            "timestamp": row.timestamp,
            "user_id": row.user_id,
            "sentiment": sentiment,
            "confidence": confidence
        }
        
        # Index to Elasticsearch
        try:
            es.index(index="sentiment-analysis", document=doc)
            success_count += 1
            
            # Show first 3 results
            if success_count <= 3:
                emoji = "😊" if sentiment == "positive" else "😞" if sentiment == "negative" else "😐"
                print(f"   {emoji} {row.id[:25]}... → {sentiment.upper()} ({confidence:.2f})")
                print(f"      Text: {text[:60]}...")
                
        except Exception as e:
            print(f"   ❌ Error indexing document {row.id}: {e}")
            error_count += 1
    
    # Summary
    print(f"\n   {'─'*66}")
    print(f"   ✅ Batch Summary:")
    print(f"   📈 Indexed: {success_count} documents")
    print(f"   😊 Positive: {sentiment_counts['positive']}")
    print(f"   😐 Neutral: {sentiment_counts['neutral']}")
    print(f"   😞 Negative: {sentiment_counts['negative']}")
    if error_count > 0:
        print(f"   ❌ Errors: {error_count}")
    print(f"   {'─'*66}\n")

# Write stream
try:
    query = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark_checkpoint") \
        .start()
    
    print("\n" + "="*70)
    print("🚀 STREAMING STARTED SUCCESSFULLY!")
    print("="*70)
    print("💡 Trigger a DAG in Airflow to send data to Kafka")
    print("🌐 Airflow UI: http://localhost:8080 (admin/admin)")
    print("📊 Kibana: http://localhost:5601")
    print("⏹️  Press Ctrl+C to stop (or stop the container)")
    print("="*70 + "\n")
    
    # Wait for termination
    query.awaitTermination()
    
except KeyboardInterrupt:
    print("\n⏹️  Stopping Spark Streaming...")
    if 'query' in locals():
        query.stop()
    spark.stop()
    print("✅ Spark session closed successfully!")
    
except Exception as e:
    print(f"\n❌ Streaming error: {e}")
    if 'query' in locals():
        query.stop()
    spark.stop()