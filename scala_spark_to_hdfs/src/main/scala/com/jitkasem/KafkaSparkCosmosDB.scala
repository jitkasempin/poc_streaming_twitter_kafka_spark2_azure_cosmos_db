package com.jitkasem

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object KafkaSparkCosmosDB {

  private val RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]"
  private val APPLICATION_NAME = "SparkStructuredStreaming"

  def main(args: Array[String]): Unit = {

    val kafkaUrl = "localhost:9092"

    val schemaRegistryURL = "http://localhost:8081"

    val topic = "confluent-in-prices"

    val conf = new SparkConf()
                  .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                  .setAppName(APPLICATION_NAME)

    val spark = SparkSession.builder.config(conf).getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .load()

//    val utils = new ConfluentSparkAvroUtils(schemaRegistryURL)
//    val keyDes = utils.deserializerForSubject(topic + "-key")
//    val valDes = utils.deserializerForSubject(topic + "-value")
    
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
