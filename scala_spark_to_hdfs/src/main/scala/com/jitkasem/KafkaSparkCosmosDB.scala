package com.jitkasem

import abris.avro.read.confluent.SchemaManager
import abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
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

    val schemaRegistryConfs = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> schemaRegistryURL,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC        -> topic,
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME, // choose a subject name strategy
      SchemaManager.PARAM_VALUE_SCHEMA_ID              -> "latest" // set to "" if you want the latest schema version to used
    )

    val conf = new SparkConf()
                  .setMaster(RUN_LOCAL_WITH_AVAILABLE_CORES)
                  .setAppName(APPLICATION_NAME)

    val spark = SparkSession.builder.config(conf).getOrCreate()

//    import spark.implicits._
    // import Spark Avro Dataframes
//    import za.co.absa.abris.avro.AvroSerDe._

    import abris.avro.AvroSerDe._


    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topic)
      .fromConfluentAvro("value", None,  Some(schemaRegistryConfs))(RETAIN_SELECTED_COLUMN_ONLY)


    val dss = df.writeStream.format("console").start().awaitTermination()

//                .selectExpr("CAST(value AS STRING)")
//                .writeStream
//                .format("csv")
//                .option("checkpointLocation", "chkpointjittempdir")
//                .option("path","hdfs://localhost:9000/user")
//                .start()
//                .awaitTermination()

  }


}
