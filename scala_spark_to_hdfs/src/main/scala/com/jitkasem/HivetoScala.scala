package com.jitkasem

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.hortonworks.hwc.HiveWarehouseSession

object HivetoScala {

  private val APPLICATION_NAME = "SparkHiveIntegration"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(APPLICATION_NAME)

    val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val hive = HiveWarehouseSession.session(spark).build()

    println("Create of Hive Table--------------------")

    hive.createTable("test_hiveserver2")
      .column("first", "string")
      .column("second", "string")
      .create()

    import spark.implicits._

    val employees = Seq(
      ( "SMITH", "CLERK"),
      ( "ALLEN", "SALESMAN"),
      ( "WARD", "SALESMAN"),
      ( "JONES", "MANAGER"),
      ( "MARTIN", "SALESMAN"),
      ( "BLAKE", "MANAGER"),
      ( "CLARK", "MANAGER"),
      ( "SCOTT", "ANALYST"),
      ( "KING", "PRESIDENT"),
      ( "TURNER", "SALESMAN"),
      ( "ADAMS", "CLERK")
    )

    val empRDD = spark.sparkContext.parallelize(employees,1)

    val employeeDataF = empRDD.toDF("first", "second")

    employeeDataF.show()

    employeeDataF.printSchema()

    employeeDataF
      .write
      .format(HiveWarehouseSession.DATAFRAME_TO_STREAM)
      .option("table", "test_hiveserver2")
      .save()


    val df = spark.read.json("hdfs://abc.internal:8020/user/root/gcs_files/data_1.json")

//    df.write.format("orc").mode("overwrite").insertInto(tableName)

    df.show()

    df.printSchema()

    println("End of SQL session-------------------")

  }
}
