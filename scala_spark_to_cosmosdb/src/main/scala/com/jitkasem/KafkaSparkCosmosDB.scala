package com.jitkasem

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object KafkaSparkCosmosDB {

  private val RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]"
  private val APPLICATION_NAME = "Spark Structured Streaming"

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
                  .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                  .setAppName(APPLICATION_NAME)

    val spark = SparkSession.builder.config(conf).getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

//  val sc = new SparkContext(conf)

    val hadoop_conf = spark.sparkContext.hadoopConfiguration
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3n.awsAccessKeyId", "your_aws_access_id")
    hadoop_conf.set("fs.s3n.awsSecretAccessKey", "your_aws_access_key")

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic_output")
      .option("failOnDataLoss", false)
      .option("kafkaConsumer.pollTimeoutMs", 4096)
      .load()


    val dss = df
                .selectExpr("CAST(value AS STRING)")
                .write
                .format("parquet")
                .option("path","s3n://bigdata-testing123/sparkapp")
                .save()

  }


}

//  val data = df.selectExpr("CAST(key AS STRING) as id", "CAST(value AS STRING) as word_length")
//  .as[(String, String)]
//
//  COMMAND ----------

//    val sinkConfigMap = Map(
//      "Endpoint" -> "https://jitkasem.documents.azure.com:443/",
//      "Masterkey" -> "Y9zV6AJW8QO9M5ey3ZXUtZJ2tmZc1LNHh0pJWxec3rK13quO9kXWT1xolenqYajUYxLLMYG7ABBA4V5QKu34Zw==",
//      "Database" -> "maindb",
//      "Collection" -> "maindata",
//      "checkpointLocation" -> "checkpoint",
//      "WritingBatchSize" -> "10",
//      "Upsert" -> "true")

// Start the stream writer
//    val streamingQueryWriter = df.writeStream
//      .format(classOf[CosmosDBSinkProvider].getName)
//      .outputMode("append")
//      .options(sinkConfigMap)
//      .start()
//      .awaitTermination()
