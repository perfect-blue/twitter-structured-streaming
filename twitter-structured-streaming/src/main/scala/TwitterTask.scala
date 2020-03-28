import Utillities._
import TwitterSchema._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

class TwitterTask(sparkSession: SparkSession,bootstrapServers:String,topic:String) {
  import sparkSession.implicits._

  val df=sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapServers)
    .option("subscribe",topic)
    .load()

  setupLogging()

  def subscribe(): Unit ={

    //ubah data frame menjadi string
    val stringDF=df.selectExpr("CAST(Value AS STRING)")
    val tweetDF=stringDF.select(from_json($"Value",payloadStruct) as("tweet"))

    whereThetweetComeFrom(tweetDF)
  

  }

  def whereThetweetComeFrom(dataFrame: DataFrame):Unit={
    val statusDF=dataFrame.selectExpr("tweet.payload.Id","tweet.payload.CreatedAt","tweet.payload.User.Location")
    val filteredStatus=statusDF
      .select("Location","CreatedAt","Id")
      .where("Id IS NOT NULL AND Location Is Not Null").toDF()

    val countCountry=filteredStatus.groupBy(
      window($"CreatedAt","10 minutes","5 minutes"),
      $"Location").count()

    val valueQuery=countCountry.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    valueQuery.awaitTermination()
  }

  def countHashtags(text:String)={
    val words=Seq(text).flatMap(_.split(" "))
    val hashtags=words.filter(_.contains('#'))
    val result=hashtags.toString()
  }

  /**
   * konfigurasi logger sehingga hanya menampilkan pesan ERROR saja
   * untuk menghindari log spam
   */
  def setupLogging()={
    val logger = Logger.getRootLogger()
    logger.setLevel(Level.ERROR)
  }
}
