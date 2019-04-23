# ML-Classification-Kafka-Streaming
This aims to classify the news articles ranging from a range of categories like politics, sports.. by streaming articles from The Guardian using Apache Kafka.

The data preprocessing is done using the Spark MLLib libraries by creating a Pipeline model with Tokenizer, Stopword remover, Labelizer, TF-IDF vectorizer, and a Classifier.

For reference you can refer to [Pipeline documentation](https://spark.apache.org/docs/latest/ml-pipeline.html#pipeline). 

### To run the code use the following steps:
1. Start zookeeper:
     > bin/zookeeper-server-start.sh config/zookeeper.properties
2. Start Kafka Server:
     > bin/kafka-server-start.sh config/server.properties
3. To create ML models and pipeline files you'll need to run pipeline.py file :
     > python3 pipeline.py
4. Run Producer File:
     > python3 stream_producer.py API-key fromDate toDate
5. Run Consumer file from inside the apache-spark directory (path of models and pipeline to be loaded needs to be provided in the consumer file)
    > bin/spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 <path to>/consumer.py localhost:9092 guardian2
