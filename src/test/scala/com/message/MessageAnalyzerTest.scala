package com.message

import com.message.model.MessageAnalyzer
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}
import org.scalatest.wordspec.AnyWordSpec

class MessageAnalyzerTest extends AnyWordSpec with BeforeAndAfterAll with PrivateMethodTester with SparkSessionWrapper {
  import spark.sqlContext.implicits._

  var messageAnalyzer: MessageAnalyzer = _
  var message: DataFrame = _
  var messageDir: DataFrame = _
  var retweet: DataFrame = _
  var userDir: DataFrame = _
  var firstWaveExpected: DataFrame =  _
  var secondWaveExpected: DataFrame = _
  var top10messagesExpected: DataFrame = _

  override def beforeAll() {
    setMessageAnalyzer()
    setMessage()
    setMessageDir()
    setRetweet()
    setUser()
    setFirstWaveExpected()
    setSecondWaveExpected()
    setTop10messages()
  }


  private def setMessageAnalyzer(): Unit = {
    messageAnalyzer = new MessageAnalyzer(spark)
  }

  private def setMessage(): Unit = {
    val messageSeq = Seq(
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
    message = messageSeq.toDF("USER_ID", "MESSAGE_ID").as("MESSAGE")
  }

  private def setMessageDir(): Unit = {
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
    messageDir = messages.toDF("MESSAGE_ID", "TEXT").as("MESSAGE_DIR")
  }

  private def setRetweet(): Unit = {
    val retweetSeq = Seq(
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
    retweet = retweetSeq.toDF("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID")as("RETWEET")
  }

  private def setUser(): Unit = {
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
    userDir = users.toDF("USER_ID", "FIRST_NAME", "LAST_NAME").as("USER_DIR")
  }

  private def setFirstWaveExpected(): Unit = {
    val firstWaveSeq = Seq(
      (1, 2, 11),
      (1,	3, 11),
      (2,	4, 12),
      (3,	8, 13),
      (10, 11, 20),
      (10, 15, 20),
      (11, 14, 21)
    )

    firstWaveExpected = firstWaveSeq.toDF("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID").as("FIRST_WAVE")
  }

  private def setSecondWaveExpected(): Unit = {
    val secondWaveSeq = Seq(
      (2,	5, 11),
      (3,	7, 11),
      (2,	4, 12),
      (3,	8, 13),
      (11, 17, 20),
      (11, 14, 21),
      (14, 22, 20)
    )

    secondWaveExpected = secondWaveSeq.toDF("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID").as("SECOND_WAVE")
  }

  private def setTop10messages(): Unit = {
    val messages = Seq(
      (1,	"Robert", "Smith",	11,	"text11",	4L),
      (10, "Jame",	"Hilton",	20,	"text20",	4L),
      (2,	"John",	"Johnson",	12,	"text12",	2L),
      (3,	"Ivan",	"Ivanov",	13,	"text13",	2L),
      (11, "Donatelo",	"Jones",	21,	"text21",	2L)
    )
    top10messagesExpected = messages.toDF("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "TEXT", "RETWEET_NUMBERS")
  }

  override def afterAll() {
    if (spark != null) {
      spark.stop()
    }
  }

  "Correct compute first wave" in {
    val firstWave: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getFirstWave)
    val result = messageAnalyzer invokePrivate firstWave(retweet, message)
    assert(firstWaveExpected.schema.equals(result.schema))
    assert(firstWaveExpected.collect().sameElements(result.collect()))
  }

  "Correct compute second wave" in {
    val secondWave: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('getSecondWave)
    val result = messageAnalyzer invokePrivate secondWave(retweet, firstWaveExpected)
    assert(secondWaveExpected.schema.equals(result.schema))
    assert(secondWaveExpected.collect().sameElements(result.collect()))
  }

  "Correct compute top 10 messages" in {
    val countMessageMethod = PrivateMethod[DataFrame]('getCountedMessage)
    val countedMessage = messageAnalyzer invokePrivate countMessageMethod(firstWaveExpected.unionAll(secondWaveExpected))

    val bestMessageMethod = PrivateMethod[DataFrame]('getBestMessage)
    val top10messages = messageAnalyzer invokePrivate bestMessageMethod(countedMessage, message, messageDir, userDir)

    assert(top10messagesExpected.schema.equals(top10messages.schema))
    assert(top10messagesExpected.collect().sameElements(top10messages.collect()))
  }

}
