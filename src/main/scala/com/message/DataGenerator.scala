package com.message

import org.apache.spark.sql.SparkSession

object DataGenerator {

  def generateUserDir(spark: SparkSession): Unit = {
   val users = Seq(
     (1, "Robert", "Smith"),
     (2, "John", "Johnson"),
     (3, "Ivan", "Ivanov"),
     (4, "Dmitro", "Dmitriev"),
     (5, "Petro", "Petrow"),
     (6, "Maksym", "Maksymov"),
     (7, "Till", "Jones"),
     (8, "Vadym", "Vadymov"),
     (9, "Alex", "Jones"),
     (10, "Jame", "Hilton"),
     (11, "Donatelo", "Jones"),
     (12, "Ivan", "Petrov"),
     (13, "Maks", "Alexov"),
     (14, "File", "FileSystem")
   )
    import spark.sqlContext.implicits._
    users.toDF("USER_ID", "FIRST_NAME", "LAST_NAME").write.format("avro").save("./input/user_dir.avro")
  }

  def generateMessageDir(spark: SparkSession): Unit = {
    val messages = Seq(
      (11, "text11"),
      (12, "text12"),
      (13, "text13"),
      (14, "text14"),
      (15, "text15"),
      (16, "text16"),
      (17, "text17"),
      (18, "text18"),
      (19, "text19"),
      (20, "text20"),
      (21, "text21"),
      (22, "text22"),
      (23, "text23"),
      (24, "text24")
    )
    import spark.sqlContext.implicits._
    messages.toDF("MESSAGE_ID", "TEXT").write.format("avro").save("./input/message_dir.avro")
  }

  def generateMessages(spark: SparkSession): Unit = {
    val messages = Seq(
      (1, 11),
      (2, 12),
      (3, 13),
      (4, 14),
      (5, 15),
      (6, 16),
      (7, 17),
      (8, 18),
      (9, 19),
      (10, 20),
      (11, 21),
      (12, 22)
    )
    import spark.sqlContext.implicits._
    messages.toDF("USER_ID", "MESSAGE_ID").write.format("avro").save("./input/com.message.avro")
  }

  def generateRetweet(spark: SparkSession): Unit = {
    val retweets = Seq(
      (1, 2, 11),
      (1, 3, 11),
      (2, 5, 11),
      (3, 7, 11),
      (7, 14, 11),
      (5, 33, 11),
      (2, 4, 12),
      (3, 8, 13),
      (10, 11, 20),
      (10, 15, 20),
      (11, 17, 20),
      (11, 14, 21),
      (14, 22, 20)
    )
    import spark.sqlContext.implicits._
    retweets.toDF("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID").write.format("avro").save("./input/retweet.avro")
  }

}
