package com.message

import com.message.model.MessageAnalyzer
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    lazy val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    val messageAnalyzer = new MessageAnalyzer(spark)
    messageAnalyzer.analyze().show()
    spark.stop()

  }
}
