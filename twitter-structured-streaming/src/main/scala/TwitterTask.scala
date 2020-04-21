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


  private val watermarkSize="5 minutes"
  private val windowSize="5 minutes"
  private val slidingSize="1 minutes"
  private val getTweetLength=sparkSession.udf.register("getTweetLength",getLength)

  def getLength:String=> Int =(column:String)=>{
    val textSplit=column.split(" ")
    textSplit.length
  }

  private val df=sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapServers)
    .option("subscribe",topic)
    .load()

  setupLogging()

  //ubah data frame menjadi string
  private val stringDF=df.selectExpr("CAST(Value AS STRING)")
  private val tweetDF=stringDF.select(from_json($"Value",payloadStruct) as("tweet"))

  private val statusDF=tweetDF.selectExpr("tweet.payload.Id",
    "tweet.payload.CreatedAt","tweet.payload.Text","tweet.payload.Source",
    "tweet.payload.FavoriteCount",
    "tweet.payload.Retweet",
    "tweet.payload.RetweetCount",
    "tweet.payload.User.Id AS UserId",
    "tweet.payload.User.FollowersCount",
    "tweet.payload.User.FriendsCount",
    "tweet.payload.User.StatusesCount",
    "tweet.payload.User.verified",
    "tweet.payload.UserMentionEntities",
    "tweet.payload.Lang")
    .withColumn("tweetLength",getTweetLength(col("Text")))
    .withColumn("time", to_timestamp(from_unixtime(col("CreatedAt").divide(1000))))
    .where("tweet.payload.Text IS NOT NULL AND tweet.payload.CreatedAt IS NOT NULL")



  def subscribe(): Unit ={
//    //data frame
    val averageTweet=getCountAverageTweetByRetweet(0)
    val viralTweet=getCountAverageTweetByRetweet(100)
    val favoriteTweet=getCountAverageTweetByFavorite(50)
    val influencedTweet=getTweetByFollowerCount(300)
    val langTweet=getCountLanguage()
    val retweetTweet=getRetweetOrNot()

    //file
    val query_averageTweet_csv=writeQueryCSV(averageTweet,"average")
    val query_viralTweet_csv=writeQueryCSV(viralTweet,"viral")
    val query_favoriteTweet_csv=writeQueryCSV(favoriteTweet,"favorite")
    val query_influencedTweet_csv=writeQueryCSV(influencedTweet,"influenced")
    val query_langTweet_csv=writeQueryCSV(retweetTweet,"retweet")


    //await termination
    query_averageTweet_csv.awaitTermination()
    query_viralTweet_csv.awaitTermination()
    query_favoriteTweet_csv.awaitTermination()
    query_influencedTweet_csv.awaitTermination()
    query_langTweet_csv.awaitTermination()
  }

  //ANALYTICS
  /*
   * berapa banyak dan rata-rata panjang tweet pada selang waktu tertentu
   */
  def getCountAverageTweetByRetweet(retweet:Int):Dataset[Row]={

    //average word and count
    val windowedAverageTweet=statusDF
      .where("RetweetCount>="+retweet)
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize,slidingSize))
      .agg(
        expr("avg(tweetLength) AS averageWord"),
        expr("count(Id) AS countTweet")
      )

    windowedAverageTweet.select("window.start","window.end","averageWord","countTweet")
  }

  def getCountAverageTweetByFavorite(favorite:Int):Dataset[Row]={
    //average word and count
    val windowedAverageTweet=statusDF
      .where("RetweetCount>="+favorite)
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize,slidingSize))
      .agg(
        expr("avg(tweetLength) AS averageWord"),
        expr("count(Id) AS countTweet")
      )

    windowedAverageTweet.select("window.start","window.end","averageWord","countTweet")
  }

  /*
   * jumlah tweet dengan follower tertentu
   */
  def getTweetByFollowerCount(follower:Int):Dataset[Row]={
      val windowedUser=statusDF
          .where("verified=true OR FollowersCount>="+follower)
          .withWatermark("time",watermarkSize)
          .groupBy(window($"time",windowSize,slidingSize))
          .agg(
            expr("count(Id) AS InfluencedTweet")
          )
    windowedUser.selectExpr("window.start","window.end","influencedTweet")
  }

  def getViralTweet(retweet:Int):Dataset[Row]={
    val lengthDF=statusDF
    val windowedAverageTweet=lengthDF
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize,slidingSize))
      .agg(
        expr("avg(tweetLength) AS averageWord"),
        expr("count(Id) AS countTweet")
      )

    windowedAverageTweet.select("window.start","window.end","averageWord","countTweet")
  }

  def getRetweetOrNot():Dataset[Row]={
    val windowedRetweet=statusDF
      .withWatermark("time",watermarkSize)
      .groupBy(window($"time",windowSize,slidingSize),$"Retweet")
      .count()

    windowedRetweet.selectExpr("window.start","window.end","Retweet","count")
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

  }

  /*
   *Menghitung bahasa pada selang waktu tertentu
   */
  def getCountLanguage():Dataset[Row]={
    val windowedLanguage=statusDF
        .withWatermark("time",watermarkSize)
        .groupBy(window($"time",windowSize,slidingSize),$"Lang")
        .count()

    windowedLanguage.select("window.start","window.end","Lang","count")
  }

  //most used hashtag
  def countHashtags(text:String)={
    val words=Seq(text).flatMap(_.split(" "))
    val hashtags=words.filter(_.contains('#'))
    val result=hashtags.toString()
  }

  //UDF
  //convert epoch time
  val convertEpoch=sparkSession.udf.register("convertEpoch",convEpoch)
  def convEpoch:String=>Long=(column:String)=>{
    val time=BigInt.apply(column)
    val result=time/1000
    result.toLong
  }


}
