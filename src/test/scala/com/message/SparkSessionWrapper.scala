package com.message

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val master = "local"
  val appName = "MessageAnalyzerTest"

  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  lazy val spark = SparkSession.builder().config(conf).getOrCreate()
}
