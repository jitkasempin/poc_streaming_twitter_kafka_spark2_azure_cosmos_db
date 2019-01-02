package com.jitkasem

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object KafkaSparkCosmosDB {

  private val RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]"
  private val APPLICATION_NAME = "SparkStructuredStreaming"

  def main(args: Array[String]): Unit = {

    val kafkaUrl = "ip-172-31-17-31.ec2.internal:9092"

    val topic = "iot_topic"

    val conf = new SparkConf()
                  .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                  .setAppName(APPLICATION_NAME)

    val spark = SparkSession.builder.config(conf).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-17-31.ec2.internal:9092")
      .option("subscribe", topic)
      .load()

    val dss = df
                .selectExpr("CAST(value AS STRING)")
                .writeStream
                .format("csv")
                .option("checkpointLocation", "chkpointjittempdir")
                .option("path","hdfs://localhost:9000/user")
                .start()
                .awaitTermination()

  }


}
