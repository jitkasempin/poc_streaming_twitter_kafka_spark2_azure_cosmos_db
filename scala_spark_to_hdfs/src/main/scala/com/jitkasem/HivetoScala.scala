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

//    val tableName = "test_hiveserver2"

//    val hiveLocation = "hdfs://dp-hdp-master-1.c.dataplatform-1363.internal:8020/user/hive/warehouse"

//    val spark = SparkSession.builder().appName("SparkHiveExample").master("local[*]").config("spark.sql.warehouse.dir", hiveLocation).config("spark.driver.allowMultipleContexts", "true").getOrCreate()

//    val arraylist: Array[(String, String)] = Array(("id", "dong"), ("x4", "jitkasem"), ("x5", "pintaya"))

//    val rdd = sc.parallelize (arraylist).map (x => Row(x._1, x._2.asInstanceOf[Number].doubleValue()))

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
    //    spark.sql("create external table if not exists %s (active_flg string, last_upd_by string,merchant_type_rv string, created long, last_upd long, merchant_name string, merchant_id string, row_id string, created_by string) STORED AS ORC TBLPROPERTIES ('transactional'='true') LOCATION '/user/dong/'".format(tableName)).show()

    employeeDataF.show()

    employeeDataF.printSchema()

    employeeDataF
      .write
      .format(HiveWarehouseSession.DATAFRAME_TO_STREAM)
      .option("table", "test_hiveserver2")
      .save()


    val df = spark.read.json("hdfs://dp-hdp-master-1.c.dataplatform-1363.internal:8020/user/root/gcs_files/cpg711/ti_cpg_merchant/2017/ti_cpg_merchant_20171006_1.json")

//    df.write.format("orc").mode("overwrite").insertInto(tableName)

    df.show()

    df.printSchema()

    println("End of SQL session-------------------")

  }
}
