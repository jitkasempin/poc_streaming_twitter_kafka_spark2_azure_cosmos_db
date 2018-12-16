package com.jitkasem

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object KafkaSparkCosmosDB {

  private val RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]"
  private val APPLICATION_NAME = "SparkStructuredStreaming"

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
                  .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                  .setAppName(APPLICATION_NAME)

    val spark = SparkSession.builder.config(conf).getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

//  val sc = new SparkContext(conf)

    //val hadoop_conf = spark.sparkContext.hadoopConfiguration
    //hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    // hadoop_conf.set("fs.s3a.multiobjectdelete.enable", "false")
    //hadoop_conf.set("fs.s3a.awsAccessKeyId", "AKIAIVJZJEHE4ATMTPJQ")
    //hadoop_conf.set("fs.s3a.awsSecretAccessKey", "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")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "iot_topic")
      .option("failOnDataLoss", false)
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
