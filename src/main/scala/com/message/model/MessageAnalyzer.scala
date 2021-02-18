package com.message.model

import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.{DataFrame, SparkSession}

class MessageAnalyzer(val spark: SparkSession) {
  import spark.sqlContext.implicits._

  def analyze(): DataFrame = {
    def getFrameFromAvroFile(pathToAvroFile: String, pathToAvroSchema: String): DataFrame = {
      val avroSchema =
        new Schema.Parser()
          .parse(new File(pathToAvroSchema))

      spark.read
        .format("avro")
        .option("avroSchema", avroSchema.toString)
        .load(pathToAvroFile)
    }

    val usersDf =
      getFrameFromAvroFile("input/user_dir.avro", "src/main/resources/tablesSchema/user.avsc")
        .as("USER_DIR")
    val messageDf = getFrameFromAvroFile("input/message.avro", "src/main/resources/tablesSchema/message.avsc")
      .as("MESSAGE")
    val messageDirDf = getFrameFromAvroFile("input/message_dir.avro", "src/main/resources/tablesSchema/messageDir.avsc")
      .as("MESSAGE_DIR")
    val retweetDf = getFrameFromAvroFile("input/retweet.avro", "src/main/resources/tablesSchema/retweet.avsc")
      .as("RETWEET")


    val firstWave = getFirstWave(retweetDf, messageDf)

    val secondWave = getSecondWave(retweetDf, firstWave)

    val countedMessage = getCountedMessage(getUnionWaves(firstWave, secondWave))
    getBestMessage(countedMessage, messageDf, messageDirDf, usersDf)
  }

  private def getFirstWave(retweets: DataFrame, messages: DataFrame): DataFrame =
    retweets
      .join(messages,retweets("USER_ID") === messages("USER_ID") && retweets("MESSAGE_ID") === messages("MESSAGE_ID"))
      .select($"RETWEET.USER_ID", $"RETWEET.SUBSCRIBER_ID", $"RETWEET.MESSAGE_ID")
      .as("FIRST_WAVE")

  private def getSecondWave(retweets: DataFrame, firstWave: DataFrame): DataFrame =
    retweets
      .join(firstWave, $"RETWEET.USER_ID" === $"FIRST_WAVE.SUBSCRIBER_ID", "leftsemi")
      .as("SECOND_WAVE")

  private def getUnionWaves(firstWave: DataFrame, secondWave: DataFrame): DataFrame =
    firstWave.unionAll(secondWave)

  private def getCountedMessage(unionWaves: DataFrame): DataFrame =
    unionWaves
      .groupBy($"MESSAGE_ID")
      .count()
      .withColumnRenamed("count", "RETWEET_NUMBERS")

  private def getBestMessage(
                        countedMessage: DataFrame,
                        message: DataFrame,
                        messageDir: DataFrame,
                        userDir: DataFrame): DataFrame =
    countedMessage.as("cm")
      .join(messageDir.as("md"), $"md.MESSAGE_ID" === $"cm.MESSAGE_ID")
      .join(message.as("m"), $"m.MESSAGE_ID" === $"cm.MESSAGE_ID")
      .join(userDir.as("ud"), $"ud.USER_ID" === $"m.USER_ID")
      .orderBy(desc("cm.RETWEET_NUMBERS"), asc("m.USER_ID"))
      .select($"m.USER_ID", $"FIRST_NAME", $"LAST_NAME", $"m.MESSAGE_ID", $"TEXT", $"RETWEET_NUMBERS")
      .limit(10)
}
