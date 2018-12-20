import sys
import re
# import pyspark.sql.functions as f
# from pyspark.sql.functions import to_json, struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    topic = "iot_topic"
    columns_to_drop = ["tweetmessage"]

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

    tweet_df = tweet_data.selectExpr("CAST(value AS STRING) as tweetmessage")

    def add_length_str(text):
        str_len = len(text)
        return str_len

    def cleaning_message_str(txt):
        clean_txt = re.sub(r'"', '', txt)
        return clean_txt
        
        
    add_length_str_udf = udf(
                            add_length_str,
                            IntegerType()
                            )

    clean_txt_txt_udf = udf(
                            cleaning_message_str,
                            StringType()
                            )
    
    #tweet_df = tweet_df.select("tweetmessage", f.regexp_replace(f.col("tweetmessage"), "[\$#,]", "").alias("replaced"))
    
    tweet_df = tweet_df.withColumn(
                                    "word_length", 
                                    add_length_str_udf(tweet_df.tweetmessage)
                                    )

    tweet_df = tweet_df.withColumn(
                                    "replaced",
                                    clean_txt_txt_udf(tweet_df.tweetmessage)
                                    )

    final_df = tweet_df.drop(*columns_to_drop)

                                                                            
    # query = final_df.select(to_json(struct([final_df[x] for x in final_df.columns])).alias("value"))\
    #                            .writeStream\
    #                            .outputMode("append")\
    #                            .format("console")\
    #                            .option("truncate", "false")\
    #                            .trigger(processingTime="5 seconds")\
    #                            .start()\
    #                            .awaitTermination()

    # Below code is for writing to Kafka topic
    
    #query = tweet_data \
    #                             .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    query = final_df.select(to_json(struct([final_df[x] for x in final_df.columns])).alias("value"))\
                                 .writeStream \
                                 .format("json") \
                                 .option("checkpointLocation", "chwritefiledir") \
                                 .option("path","hdfs://localhost:9000/user") \
                                 .start() \
                                 .awaitTermination()


                                 #.format("kafka") \
                                 #.option("kafka.bootstrap.servers", "localhost:9092") \
                                 #.option("checkpointLocation", "pabcdefdir") \
                                 #.option("topic", "topic_output") \
                                 #.start() \
                                 #.awaitTermination()
