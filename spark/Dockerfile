# ================================
#  Spark + Python + Kafka Connectors + Hugging Face
# ================================
FROM apache/spark:3.5.0

# Switch to root for installations
USER root

# ------------------------
# 1. Install basic dependencies
# ------------------------
RUN apt-get update && apt-get install -y \
    python3 python3-pip openjdk-11-jdk curl vim wget && \
    apt-get clean

# ------------------------
# 2. Set environment variables
# ------------------------
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# ------------------------
# 3. Install Python libraries for Spark Streaming + NLP
# ------------------------
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
        pyspark==3.5.0 \
        kafka-python==2.0.2 \
        transformers==4.45.2 \
        torch==2.4.1 \
        pandas==2.0.3 \
        elasticsearch==8.15.0

# ------------------------
# 4. Add Kafka connector jars (for spark-sql-kafka-0-10)
# ------------------------
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
      https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# ------------------------
# 5. Copy your Spark application code
# ------------------------
WORKDIR /opt/spark-apps
COPY . /opt/spark-apps

# ------------------------
# 6. Default command
# ------------------------
CMD ["bash"]
