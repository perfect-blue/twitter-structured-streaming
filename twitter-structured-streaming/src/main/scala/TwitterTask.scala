import Utillities._
import TwitterSchema._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

class TwitterTask(sparkSession: SparkSession,bootstrapServers:String,topic:String) {
  import sparkSession.implicits._


  private val df=sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapServers)
    .option("subscribe",topic)
    .load()

  setupLogging()

  //ubah data frame menjadi string
  private val stringDF=df.selectExpr("CAST(Value AS STRING)")
  df.printSchema()
  private val tweetDF=stringDF.select(from_json($"Value",payloadStruct) as("tweet"))

  def subscribe(): Unit ={
    //data frame
    val averageWord=getAverageWord()
//    val tweetLocation=countTweetLocation()
//    val busyHour=getBusyHour()
//    val status=getStatus()

    //query console
    val query_average_word=writeQueryConsole(averageWord,"complete")
//    val query_status=writeQueryConsole(status,"append")
//    val query_tweet_location=writeQueryConsole(tweetLocation,"complete")
//    val query_busy_hour=writeQueryConsole(busyHour,"complete")

    //query file
//    val query_average_word_csv=writeQueryCSV(averageWord,"averageWord")
//    val query_status_csv=writeQueryCSV(status,"status")
//    val query_tweet_location_csv=writeQueryCSV(tweetLocation,"location")
//    val query_busy_hour_csv=writeQueryCSV(busyHour,"busyHour")

    //kafka
    writeQueryKafka(averageWord,"complete","twitter-average",this.bootstrapServers,"averageWord")
//    query file termination
//    query_average_word_csv.awaitTermination()
//    query_status.awaitTermination()
//    query_tweet_location_csv.awaitTermination()
//    query_busy_hour_csv.awaitTermination()

    //query console termination
    query_average_word.awaitTermination()
//    query_status_csv.awaitTermination()
//    query_tweet_location.awaitTermination()
//    query_busy_hour.awaitTermination()

    //kafka termination

  }

  def getStatus():Dataset[Row]={
    val statusDF=tweetDF.selectExpr("tweet.payload.Id",
      "tweet.payload.CreatedAt","tweet.payload.Text","tweet.payload.Source",
    "tweet.payload.FavoriteCount","tweet.payload.RetweetCount")
      .withColumn("time", from_unixtime(col("CreatedAt").divide(1000)))
      .where("tweet.payload.CreatedAt IS NOT NULL")

    statusDF
  }

  def getUser(dataFrame: DataFrame):StreamingQuery={
    val userDF=dataFrame.selectExpr("tweet.payload.User.*")
    val query=writeQueryConsole(userDF,"append")
    query
  }


  //ANALYTICS
  //TODO: add another analytics based on ABD project
  //average word
  def getAverageWord():Dataset[Row]={
    val statusDF=tweetDF
      .selectExpr("tweet.payload.Id","tweet.payload.CreatedAt","tweet.payload.Text")
      .withColumn("tweetLength",getTweetLength(col("Text")))
      .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))
      .where("tweet.payload.Text IS NOT NULL AND tweet.payload.CreatedAt IS NOT NULL")


    val windowedAverageTweet=statusDF
      .withWatermark("time","10 minutes")
      .groupBy(
      window($"time","10 minutes","5 minutes")
    ).avg("tweetLength")

    val windowedLength=windowedAverageTweet.select(col("window"),col("avg(tweetLength)").as("averageWord"))
    val result=windowedLength.selectExpr("window.start","window.end","averageWord")
    result
  }

  //location count
  def countTweetLocation():Dataset[Row]={
    val statusDF=tweetDF.selectExpr("tweet.payload.Id","tweet.payload.CreatedAt","tweet.payload.User.Location")
    val filteredStatus=statusDF
      .select("Location","CreatedAt","Id")
      .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))
      .where("Id IS NOT NULL AND Location Is Not Null").toDF()

    val countCountry=filteredStatus
      .withWatermark("time","10 minutes")
      .groupBy(window($"time","10 minutes","5 minutes"),
      $"Location").count()

    val result=countCountry.selectExpr("window.start","window.end","Location","Count")

    result
//    val query=writeQueryConsole(countCountry,"complete")
//    query
  }

  //most busy hour
  def getBusyHour():Dataset[Row]={
    val statusDF=tweetDF
      .selectExpr("tweet.payload.Id","tweet.payload.CreatedAt")
      .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))
      .where("tweet.payload.CreatedAt IS NOT NULL")

    val busyHour=statusDF
      .withWatermark("time","10 minutes")
      .groupBy(
      window($"time","10 minutes","5 minutes")
    ).count()

    val result=busyHour.selectExpr("window.start","window.end","count")
    result
//    val query=writeQueryConsole(busyHour,"complete")
//    query

  }
  //most used hashtag
  def countHashtags(text:String)={
    val words=Seq(text).flatMap(_.split(" "))
    val hashtags=words.filter(_.contains('#'))
    val result=hashtags.toString()
  }

  //UDF
  val getTweetLength=sparkSession.udf.register("getTweetLength",getLength)
  //get words
  def getLength:String=> Int =(column:String)=>{
    val textSplit=column.split(" ")
    textSplit.length
  }

  //convert epoch time
  val convertEpoch=sparkSession.udf.register("convertEpoch",convEpoch)
  def convEpoch:String=>Long=(column:String)=>{
    val time=BigInt.apply(column)
    val result=time/1000
    result.toLong
  }
}
