# Message
## Second task from "Intro to Apache Spark" course

### Tasks description

A user posts a provocative message on Twitter. His subscribers do a retweet. Later, every subscriber's subscriber does retweet too.
* Read data from Avro files.
* Find the top ten users by a number of retweets in the first and second waves.

#### Input tables


* __Name__: USER_DIR.<br>
  __Description__: Table contains first and last names.

    |USER_ID|FIRST_NAME|LAST_NAME|
    |-------|------------|----------|
    |1      |"Robert"    |"Smith"   |
    |2      |"John"      |"Johnson" |
    |3      |"Alex"      |"Jones"   |


* __Name__: MESSAGE_DIR. <br>
  __Description__: Table contains text message.

    |MESSAGE_ID|TEXT
    |----------|------|
    |11        |"text"|
    |12        |"text"|
    |13        |"text"|
    
* __Name__: MESSAGE
  __Description__: Table contains information about posted messages.
  
  |USER_ID|MESSAGE_ID|
  |-------|----------|
  |1      |11        |
  |2      |12        |
  |3      |13        |
  
* __Name__: RETWEET
  __Description__: Table contains information about retweets.
  
  |USER_ID|SUBSCRIBER_ID|MESSAGE_ID|
  |-------|--------|---|
  |1      |2       |11| 
  |1      |3       |11| 
  |2      |5       |11| 
  |3      |7       |11|
  |7      |14      |11| 
  |5      |33      |11| 
  |2      |4       |12|
  |3      |8       |13|

 
#### Outputs: 

"Top one user" looks the following

|USER_ID|FIRST_NAME|LAST_NAME|MESSAGE_ID|TEXT|NUMBER_RETWEETS|
--------|----------|---------|---------|----|---|
|1      |"Robert"  |"Smith" |11        |"text"|4|


### Implementation

#### com.message.DataGenerator.scala

This is file for creating input avro files. Avro files save at the _input_ folder. 
The object writes to the files custom data.

### com.message.model.MessageAnalyzer.scala

This file includes a class _MessageAnalyzer(val spark: SparkSession)_. The class has one method for 
analysis input tables(``analyze(): DataFrame``). Input files must be at the "input" folder and have the specific name.

__analyze__ method works according to the following algorithm:

* load all input files
* find first wave
* find second wave
* compute count for each message of combining two waves
* using counted messages, the method finds top 10 user

#### Explanation of some functions

* ``getFirstWave(retweets: DataFrame, messages: DataFrame): DataFrame`` - returns first wave. Result is calculated by joining retweets
and messages tables by _USER_ID_ and _MESSAGE_ID_ columns. The result has following schema:
```
root
 |-- USER_ID: integer (nullable = true)
 |-- SUBSCRIBER_ID: integer (nullable = true)
 |-- MESSAGE_ID: integer (nullable = true)
```

* ``getSecondWave(retweets: DataFrame, firstWave: DataFrame): DataFrame`` - returns second wave. Result is calculated by left semi join
retweets and firstWave talbes by _RETWEET.USER_ID_ and _FIRST_WAVE.SUBSCRIBER_ID_ columns. The result's schema is the same as at first wave.

* ``getCountedMessage(unionWaves: DataFrame): DataFrame`` - counts the retweet numbers. The schema:
```
root
 |-- MESSAGE_ID: integer (nullable = true)
 |-- RETWEET_NUMBERS: long (nullable = false)
```

* ``getBestMessage(countedMessage: DataFrame, message: DataFrame, messageDir: DataFrame, userDir: DataFrame): DataFrame`` - 
returns top 10 users. Implementation is really easy: sort countedMessage by retweet numbers and add the extra information about user and message.
At the end, only first 10 rows are included. 


### com.message.MessageAnalyzerTest.scala

For testing, we need the input data from DataGenerator object, and prepared expected result. The WordSpec style was used.
Also, I used PrivateMethodTester for testing private methods. BeforeAndAfterAll was used for preparing expected result and close SparkSession.
<hr>