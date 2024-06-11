# Streaming Data Architecture - Real-Time Dashboard for BTC



## Setup
#### All the connection URLs between servers are default to be localhost. Change them to your URLs if needed.
#### 

### To run Kafka server
Install Java,
```
sudo apt update
sudo apt install default-jdk -y
```
Install Kafka,
```
wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
```
Then, install Python packages,
```
pip install -r requirements.txt
```
Run Kafka server,
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
In another terminal,
```
bin/kafka-server-start.sh config/server.properties
```
Create the topic if it's the first time running,
```
bin/kafka-topics.sh --create --topic coinbase_feed --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```
Start streaming with another terminal,
```
python websocket_kafka.py
```

### To Run Spark server
Install Spark,
```
sudo apt update
apt-get install openjdk-8-jdk-headless -qq > /dev/null
wget -q https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz
tar xf spark-3.2.1-bin-hadoop3.2.tgz
```
Then, install Python packages,
```
pip install -r requirements.txt
```
Run Spark server,
```
python spark.py
```

### To Run FastAPI server
Install Python packages,
```
pip install -r requirements.txt
```
Run FastAPI server,
```
python fastAPI.py
```

### To Run the dashboard
Install libpq-dev,
```
sudo apt update
sudo apt-get install libpq-dev
```
Install Python packages,
```
pip install -r requirements.txt
```
Run the dashboard,
```
python dash_app.py
```

