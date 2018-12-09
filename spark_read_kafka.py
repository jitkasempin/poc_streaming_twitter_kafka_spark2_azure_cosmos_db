import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    topic = "iot_topic"
    
    spark = SparkSession\
        .builder\
        .appName("Twitter")\
        .getOrCreate()

    #spark.sparkContext.setLogLevel("ERROR")

    tweet_data = spark.readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers",  "localhost:9092")\
                        .option("subscribe", topic)\
                        .load()

    #tweet_df = tweet_data.selectExpr("CAST(value AS STRING) as tweetmessage")

    #def add_length_str(text):

    #    str_len = len(text)
    #    return str_len


    #add_length_str_udf = udf(
    #                        add_length_str,
    #                        IntegerType()
    #                        )

    #tweet_df = tweet_df.withColumn(
    #                                "word_length", 
    #                                add_length_str_udf(tweet_df.tweetmessage)
    #                                )
    #query = tweet_df.writeStream\
    #                            .outputMode("append")\
    #                            .format("console")\
    #                            .option("truncate", "false")\
    #                            .trigger(processingTime="5 seconds")\
    #                            .start()\
    #                            .awaitTermination()

    query = tweet_data \
                                 .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                                 .writeStream \
                                 .format("kafka") \
                                 .option("kafka.bootstrap.servers", "localhost:9092") \
                                 .option("topic", "topic_output") \
                                 .start() \
                                 .awaitTermination()
    



    
