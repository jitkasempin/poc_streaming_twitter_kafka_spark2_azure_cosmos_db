# poc_streaming_twitter_kafka_spark2_azure_cosmos_db
I try to build the data pipeline that read the twitter stream and store tweet data into Azure Cosmos DB
# Currently in progress
What already done:
      * 1. Connect to twitter and read the tweet messages that contain the word "iot"
      * 2. Send that tweet messages to Kafka
      * 3. Use Spark to read the data from Kafka using Spark Structured Streaming API
      * 4. Print the messages to the console output

# How to run spark to read data from Kafka 
Use this command -> spark-submit --packages org.apache.kafka.common.serialization:kafka-clients-1.1.0.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 spark_read_kafka.py

