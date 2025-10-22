from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json
import time
import os

def create_kafka_topic():
    """Create Kafka topic if it doesn't exist"""
    from kafka.admin import KafkaAdminClient, NewTopic
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='kafka:29092',
            client_id='topic_creator'
        )
        
        topic_list = [NewTopic(name="text-stream", num_partitions=1, replication_factor=1)]
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("✅ Topic 'text-stream' created successfully")
        except Exception as e:
            print(f"ℹ️ Topic may already exist: {e}")
        
        admin_client.close()
    except Exception as e:
        print(f"❌ Error creating topic: {e}")

def ingest_from_reddit(**context):
    """Fetch data from Reddit and send to Kafka"""
    
    print("🚀 Starting Reddit ingestion...")
    
    # Wait for Kafka to be ready
    print("⏳ Waiting for Kafka...")
    time.sleep(10)
    create_kafka_topic()
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=30000
        )
        print("✅ Kafka producer created")
    except Exception as e:
        print(f"❌ Failed to create Kafka producer: {e}")
        raise
    
    # Check if credentials are set
    client_id = os.getenv('REDDIT_CLIENT_ID', '')
    client_secret = os.getenv('REDDIT_CLIENT_SECRET', '')
    
    if not client_id or not client_secret or client_id == 'YOUR_CLIENT_ID':
        print("⚠️ Reddit credentials not set! Using sample data instead.")
        print("ℹ️ To use real Reddit data, set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in docker-compose.yml")
        return ingest_sample_data(producer)
    
    try:
        import praw
        
        print(f"🔐 Connecting to Reddit API...")
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=os.getenv('REDDIT_USER_AGENT', 'sentiment-analysis-bot/1.0')
        )
        
        # Test connection
        reddit.user.me()
        print("✅ Reddit API connected successfully!")
        
    except Exception as e:
        print(f"❌ Reddit API connection failed: {e}")
        print("⚠️ Falling back to sample data")
        return ingest_sample_data(producer)
    
    # Subreddits to monitor
    subreddits = ['technology', 'worldnews', 'AskReddit', 'movies', 'gaming']
    
    print(f"📡 Fetching posts from subreddits: {', '.join(subreddits)}")
    
    message_count = 0
    for subreddit_name in subreddits:
        try:
            subreddit = reddit.subreddit(subreddit_name)
            
            # Get hot posts (limit to 10 per subreddit)
            for submission in subreddit.hot(limit=10):
                # Combine title and selftext for better analysis
                text = submission.title
                if submission.selftext and len(submission.selftext) > 0:
                    text += " " + submission.selftext
                
                # Skip if text is too short
                if len(text.strip()) < 10:
                    continue
                
                message = {
                    'id': f'reddit_{submission.id}',
                    'text': text[:1000],  # Limit text length
                    'source': 'reddit',
                    'subreddit': subreddit_name,
                    'timestamp': datetime.fromtimestamp(submission.created_utc).isoformat(),
                    'user_id': str(submission.author) if submission.author else 'deleted',
                    'score': submission.score,
                    'num_comments': submission.num_comments,
                    'url': submission.url
                }
                
                producer.send('text-stream', value=message)
                message_count += 1
                print(f"✉️ Sent message {message_count}: [{subreddit_name}] {text[:60]}...")
                
        except Exception as e:
            print(f"⚠️ Error fetching from r/{subreddit_name}: {e}")
            continue
    
    producer.flush()
    producer.close()
    print(f"✅ Reddit ingestion completed! Sent {message_count} messages.")

def ingest_from_twitter(**context):
    """Fetch data from Twitter/X and send to Kafka"""
    
    print("🚀 Starting Twitter ingestion...")
    
    # Wait for Kafka to be ready
    print("⏳ Waiting for Kafka...")
    time.sleep(10)
    create_kafka_topic()
    
    # Create Kafka producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_block_ms=30000
        )
        print("✅ Kafka producer created")
    except Exception as e:
        print(f"❌ Failed to create Kafka producer: {e}")
        raise
    
    # Check if credentials are set
    bearer_token = os.getenv('TWITTER_BEARER_TOKEN', '')
    
    if not bearer_token or bearer_token == 'YOUR_BEARER_TOKEN':
        print("⚠️ Twitter credentials not set! Using sample data instead.")
        print("ℹ️ To use real Twitter data, set TWITTER_BEARER_TOKEN in docker-compose.yml")
        return ingest_sample_data(producer)
    
    try:
        import tweepy
        
        print("🔐 Connecting to Twitter API...")
        client = tweepy.Client(bearer_token=bearer_token)
        
        # Test connection with a simple search
        test = client.search_recent_tweets("hello", max_results=10)
        print("✅ Twitter API connected successfully!")
        
    except Exception as e:
        print(f"❌ Twitter API connection failed: {e}")
        print("⚠️ Falling back to sample data")
        return ingest_sample_data(producer)
    
    # Search queries (topics to monitor)
    queries = [
        'artificial intelligence',
        'climate change',
        'cryptocurrency',
        'technology news',
        'movie reviews'
    ]
    
    print(f"📡 Fetching tweets for queries: {', '.join(queries)}")
    
    message_count = 0
    for query in queries:
        try:
            # Search recent tweets (max 10 per query, English only)
            tweets = client.search_recent_tweets(
                query=f"{query} -is:retweet lang:en",
                max_results=10,
                tweet_fields=['created_at', 'author_id', 'public_metrics']
            )
            
            if tweets.data:
                for tweet in tweets.data:
                    # Skip very short tweets
                    if len(tweet.text.strip()) < 10:
                        continue
                    
                    message = {
                        'id': f'twitter_{tweet.id}',
                        'text': tweet.text,
                        'source': 'twitter',
                        'query': query,
                        'timestamp': tweet.created_at.isoformat() if tweet.created_at else datetime.now().isoformat(),
                        'user_id': str(tweet.author_id) if tweet.author_id else 'unknown',
                        'metrics': tweet.public_metrics if hasattr(tweet, 'public_metrics') else {}
                    }
                    
                    producer.send('text-stream', value=message)
                    message_count += 1
                    print(f"✉️ Sent message {message_count}: [{query}] {tweet.text[:60]}...")
                    
        except Exception as e:
            print(f"⚠️ Error fetching tweets for '{query}': {e}")
            continue
    
    producer.flush()
    producer.close()
    print(f"✅ Twitter ingestion completed! Sent {message_count} messages.")

def ingest_sample_data(producer):
    """Fallback: Use sample data when API credentials are not available"""
    
    print("📝 Using sample data (API credentials not configured)")
    
    import random
    
    SAMPLE_TEXTS = [
        "I absolutely love this new AI technology! It's revolutionary and will change everything.",
        "Terrible experience with customer service today. Very disappointed and frustrated.",
        "The new movie was decent, nothing special but entertaining enough.",
        "Just finished an amazing book! Highly recommend it to everyone!",
        "This product broke after one week. Complete waste of money.",
        "Pretty good value for the price. Does what it's supposed to do.",
        "Worst decision ever! Save your money and avoid this at all costs!",
        "Outstanding performance! Exceeded all my expectations and more!",
        "Meh, it's average. Nothing to get excited about really.",
        "Incredible experience! Will definitely be coming back for more!",
        "The climate crisis is getting worse every day. We need action now!",
        "New cryptocurrency regulations announced today. Market reacting positively.",
        "Breaking: Major tech company announces groundbreaking innovation.",
        "Sports fans are disappointed with today's game results.",
        "New restaurant in town is absolutely phenomenal! Best food ever!",
        "Traffic is terrible today. Spent 2 hours in gridlock. So annoying!",
        "Just adopted a puppy and I'm so happy! Best decision of my life!",
        "The weather has been perfect this week. Sunny and warm every day.",
        "Disappointed with the election results. Not what I hoped for.",
        "Amazing concert last night! The band was incredible and energetic!"
    ]
    
    SOURCES = ['reddit', 'twitter', 'reviews', 'feedback']
    SUBREDDITS = ['technology', 'worldnews', 'AskReddit', 'movies', 'gaming']
    
    message_count = 0
    for i in range(30):  # Send 30 sample messages
        text = random.choice(SAMPLE_TEXTS)
        source = random.choice(SOURCES)
        
        message = {
            'id': f'sample_{int(time.time())}_{i}',
            'text': text,
            'source': source,
            'timestamp': datetime.now().isoformat(),
            'user_id': f'user_{random.randint(1000, 9999)}'
        }
        
        if source == 'reddit':
            message['subreddit'] = random.choice(SUBREDDITS)
            message['score'] = random.randint(0, 1000)
            message['num_comments'] = random.randint(0, 100)
        
        producer.send('text-stream', value=message)
        message_count += 1
        print(f"✉️ Sent sample message {message_count}/30: {text[:60]}...")
        time.sleep(1)  # Send one message per second
    
    producer.flush()
    producer.close()
    print(f"✅ Sample data ingestion completed! Sent {message_count} messages.")

# Default arguments
default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Create DAG for Reddit
dag_reddit = DAG(
    'reddit_sentiment_ingestion',
    default_args=default_args,
    description='Ingest Reddit posts to Kafka for sentiment analysis',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    catchup=False,
    tags=['sentiment', 'kafka', 'reddit'],
)

reddit_task = PythonOperator(
    task_id='ingest_from_reddit',
    python_callable=ingest_from_reddit,
    dag=dag_reddit,
)

# Create DAG for Twitter
dag_twitter = DAG(
    'twitter_sentiment_ingestion',
    default_args=default_args,
    description='Ingest Twitter posts to Kafka for sentiment analysis',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    catchup=False,
    tags=['sentiment', 'kafka', 'twitter'],
)

twitter_task = PythonOperator(
    task_id='ingest_from_twitter',
    python_callable=ingest_from_twitter,
    dag=dag_twitter,
)