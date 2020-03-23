import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import Utillities._
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

object Twitter {

  private val bootstrapServers="127.0.0.1:9092"

  def subscribe(sparkSession: SparkSession, topic:String): Unit ={

    import sparkSession.implicits._

    val df=sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapServers)
      .option("subscribe",topic)
      .load()

    setupLogging()

    //ambil value string
    val valueDF=df.selectExpr("CAST(value AS STRING)")

    //construct user schema
    val userStruct = new StructType()
      .add("id",DataTypes.StringType)
      .add("name",DataTypes.StringType)
      .add("screen_name",DataTypes.StringType)
      .add("location",DataTypes.StringType)
      .add("verified",DataTypes.BooleanType)
      .add("friends_count",DataTypes.IntegerType)
      .add("followers_count",DataTypes.IntegerType)
      .add("statues_count",DataTypes.IntegerType)

    //construct elements schema
    val elementStruct = new StructType()
      .add("hastags",DataTypes.createArrayType(DataTypes.StringType))

    //construct tweet schema
    val tweetStruct= new StructType()
      .add("id",DataTypes.StringType)
      .add("created_at",DataTypes.DateType)
      .add("user",userStruct)
      .add("text",DataTypes.StringType)
      .add("lang",DataTypes.StringType)
      .add("is_retweet",DataTypes.BooleanType)

    //construct payload schema
    val payloadStruct = new StructType()
      .add("payload", tweetStruct)

    val payloadDF=valueDF.select(from_json($"value",payloadStruct).as("value"))

    //create tweet table
    val tweetDF = payloadDF.selectExpr("value.payload.id","value.payload.created_at","value.payload.text")

    //create user table
    val userDF= payloadDF.selectExpr("value.payload.user.*")

    //hashtag table
    val hashtagDF= tweetDF.groupBy(window($"created_at","30 seconds","10 seconds")).count()
    val hashtagOutput=hashtagDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    hashtagOutput.awaitTermination()
    //    .writeStream
//      .outputMode("append")
//      .format("console")
//      .start()


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
