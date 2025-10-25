# 📊 Real-Time Sentiment Analysis Pipeline

A complete **DataOps + AI** production-ready pipeline for real-time sentiment analysis of streaming text data using modern data engineering tools and AI models.

![Pipeline Architecture](https://img.shields.io/badge/Status-Production_Ready-success)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![Python](https://img.shields.io/badge/Python-3.10-blue)
![License](https://img.shields.io/badge/License-MIT-green)

## 🎯 Overview

This project demonstrates a complete streaming analytics pipeline that:
- **Ingests** text data from Reddit/Twitter APIs or sample data
- **Streams** data through Apache Kafka
- **Processes** in real-time using Apache Spark Streaming
- **Analyzes** sentiment using Hugging Face Transformers (DistilBERT)
- **Stores** results in Elasticsearch
- **Visualizes** insights with Kibana dashboards
- **Orchestrates** workflows with Apache Airflow

## 🏗️ Architecture

```
┌─────────────┐
│   Airflow   │ ← Orchestration (every 15 min)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Kafka    │ ← Message Streaming
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Spark    │ ← Real-time Processing
│  Streaming  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  DistilBERT │ ← AI Sentiment Analysis
│    Model    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│Elasticsearch│ ← Data Storage
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Kibana    │ ← Visualization
└─────────────┘
```

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 2.8.4 | Workflow scheduling and monitoring |
| **Streaming** | Apache Kafka 7.5.0 | Real-time message broker |
| **Processing** | Apache Spark 3.5.0 | Distributed stream processing |
| **AI/ML** | Hugging Face Transformers | Sentiment classification (DistilBERT) |
| **Storage** | Elasticsearch 8.11.0 | Document store and search engine |
| **Visualization** | Kibana 8.11.0 | Interactive dashboards |
| **Containerization** | Docker & Docker Compose | Portable deployment |
| **Language** | Python 3.10 | Pipeline implementation |

## ✨ Features

- ✅ **Fully Dockerized** - No complex local setup required
- ✅ **Real-time Processing** - Analyze data as it arrives
- ✅ **AI-Powered** - State-of-the-art sentiment classification
- ✅ **Production Ready** - Includes error handling, logging, and monitoring
- ✅ **Scalable** - Easy to add more workers and partitions
- ✅ **API Integration** - Supports Reddit and Twitter APIs
- ✅ **Sample Data** - Works out-of-the-box without API keys
- ✅ **Interactive Dashboards** - Real-time visualization with Kibana

## 🚀 Quick Start

### Prerequisites

- Docker Desktop (with at least 6GB RAM allocated)
- Docker Compose
- Git

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/PENE18/sentiment-analysis-pipeline.git
cd sentiment-analysis-pipeline

# 2. Build and start all services
docker build -t my-airflow .
docker compose build spark
docker-compose up -d

# 3. Wait 3-5 minutes for services to initialize
docker-compose ps

# 4. Access the UIs
# Airflow:      http://localhost:8080 (admin/admin)
# Kibana:       http://localhost:5601
# Elasticsearch: http://localhost:9200
```

### Running the Pipeline

```bash
# 1. Open Airflow UI
open http://localhost:8080

# 2. Enable and trigger a DAG
# - Toggle ON: reddit_sentiment_ingestion or twitter_sentiment_ingestion
# - Click the play button to trigger

# 3. Watch Spark process the data
docker-compose logs -f spark

# 4. View results in Kibana
open http://localhost:5601
```

## 📁 Project Structure

```
sentiment-analysis-pipeline/
├── docker-compose.yml           # All services configuration
├── README.md                    # This file
├── .env.example                 # Environment variables template
├── airflow/
│   ├── Dockerfile              # Airflow container image
│   ├── requirements.txt        # Python dependencies
│   └── dags/
│       └── data_ingestion_dag.py  # Data ingestion workflow
├── spark/
│   ├── Dockerfile              # Spark container image
│   └── spark_streaming.py      # Sentiment analysis processor
└── data/                       # Shared data directory
```

## 🔧 Configuration

###  Reddit API Setup

To use real Reddit data instead of sample data:

1. Create a Reddit app at https://www.reddit.com/prefs/apps
2. Create a `.env` file in the project root:

```bash
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=sentiment-analysis-bot/1.0
```

3. Restart services:
```bash
docker-compose down
docker-compose up -d
```
## 📊 Sample Output

### Spark Processing Logs
```
======================================================================
📦 Processing batch 0
======================================================================
   📊 Found 30 messages to analyze
   😊 reddit_abc123... → POSITIVE (0.98)
      Text: I love this new AI technology! It's revolutionary...
   😞 reddit_def456... → NEGATIVE (0.95)
      Text: Terrible experience. Would not recommend...
   😐 reddit_ghi789... → NEUTRAL (0.65)
      Text: It's okay, nothing special but does the job...
   
   ──────────────────────────────────────────────────────────────────
   ✅ Batch Summary:
   📈 Indexed: 30 documents
   😊 Positive: 12
   😐 Neutral: 10
   😞 Negative: 8
   ──────────────────────────────────────────────────────────────────
```

### Kibana Dashboard Metrics
- **Sentiment Distribution**: Pie chart showing positive/negative/neutral breakdown
- **Trends Over Time**: Line chart showing sentiment changes
- **Source Analysis**: Bar chart by platform (Reddit/Twitter/Sample)
- **Top Keywords**: Word cloud of most common terms

## 🔍 Monitoring

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f spark
docker-compose logs -f airflow
docker-compose logs -f kafka

# Last 100 lines
docker-compose logs --tail=100 spark
```

### Check Service Health

```bash
# Container status
docker-compose ps

# Elasticsearch health
curl http://localhost:9200/_cluster/health?pretty

# Kafka topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Count indexed documents
curl http://localhost:9200/sentiment-analysis/_count?pretty
```

## 🛑 Management Commands

```bash
# Stop all services
docker-compose down

# Stop and remove all data
docker-compose down -v

# Restart specific service
docker-compose restart spark

# Rebuild after code changes
docker-compose up -d --build

# View resource usage
docker stats
```

## 📈 Performance

- **Throughput**: ~100 messages/second
- **Latency**: <2 seconds end-to-end
- **Model Load Time**: ~2 minutes (first run only)
- **Memory Usage**: ~4GB total
- **CPU Usage**: 2-4 cores recommended

## 🐛 Troubleshooting

### Services won't start
```bash
# Check Docker resources (need 6GB+ RAM)
docker info

# View error logs
docker-compose logs

# Complete reset
docker-compose down -v
docker-compose up -d --build
```

### Spark not processing data
```bash
# Check if Kafka has messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic text-stream \
  --from-beginning \
  --max-messages 5

# Restart Spark
docker-compose restart spark
```

### Airflow DAG not showing
```bash
# Check for errors
docker-compose logs airflow | grep ERROR
```

## 🚀 Advanced Usage

### Scale Kafka Partitions
```bash
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic text-stream \
  --partitions 3
```

### Add More Spark Workers
Edit `docker-compose.yml` and add:
```yaml
spark-worker:
  image: bitnami/spark:3.5.0
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-streaming:7077
```

### Custom Sentiment Model
Edit `spark/spark_streaming.py`:
```python
sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="cardiffnlp/twitter-roberta-base-sentiment-latest"
)
```

## 📚 Learning Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Streaming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Hugging Face Transformers](https://huggingface.co/docs/transformers)
- [Elasticsearch Guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Your Name**
- GitHub: [@yourusername](https://github.com/PENE18)
- LinkedIn: [Your LinkedIn](https://www.linkedin.com/in/mamadoupene495/)

## 🙏 Acknowledgments

- [Hugging Face](https://huggingface.co/) for pre-trained models
- [Apache Software Foundation](https://apache.org/) for Kafka, Spark, and Airflow
- [Elastic](https://www.elastic.co/) for Elasticsearch and Kibana
- The open-source community

## ⭐ Star History

If you find this project useful, please consider giving it a star! ⭐

---

**Made with ❤️ and ☕ by [Your Name]**
