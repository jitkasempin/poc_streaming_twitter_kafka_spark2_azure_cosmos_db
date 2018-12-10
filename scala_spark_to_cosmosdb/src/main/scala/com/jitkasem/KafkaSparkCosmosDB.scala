package com.jitkasem

import org.apache.spark.sql.SparkSession

import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming._

object KafkaSparkCosmosDB {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic_output")
      .option("failOnDataLoss", "false")
      .load()

    //  val data = df.selectExpr("CAST(key AS STRING) as id", "CAST(value AS STRING) as word_length")
    //  .as[(String, String)]
    //
    //  COMMAND ----------

    val sinkConfigMap = Map(
      "Endpoint" -> "https://jitkasem.documents.azure.com:443/",
      "Masterkey" -> "Y9zV6AJW8QO9M5ey3ZXUtZJ2tmZc1LNHh0pJWxec3rK13quO9kXWT1xolenqYajUYxLLMYG7ABBA4V5QKu34Zw==",
      "Database" -> "maindb",
      "Collection" -> "maindata",
      "checkpointLocation" -> "checkpoint",
      "WritingBatchSize" -> "10",
      "Upsert" -> "true")

    // Start the stream writer
    val streamingQueryWriter = df.writeStream
      .format(classOf[CosmosDBSinkProvider].getName)
      .outputMode("append")
      .options(sinkConfigMap)
      .start()
      .awaitTermination()


  }


}
