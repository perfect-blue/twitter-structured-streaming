import TwitterSchema.payloadStruct
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DataTypes, StructType}
import scala.concurrent.duration._

object Utillities {

//   val ID_FIELD="Id"
//   val TEXT_FIELD="Text"
//
//  val statusStruct:StructType = new StructType()
//    .add(ID_FIELD,DataTypes.StringType)
//    .add(TEXT_FIELD,DataTypes.StringType)
//
//  val payloadStruct:StructType= new StructType()
//    .add("payload",statusStruct)
  /**
   * konfigurasi logger sehingga hanya menampilkan pesan ERROR saja
   * untuk menghindari log spam
   */
  def setupLogging()={
    val logger = Logger.getRootLogger()
    logger.setLevel(Level.ERROR)
  }

  def writeQueryConsole(dataFrame: DataFrame,mode:String):StreamingQuery={
    val result=dataFrame.writeStream
      .outputMode(mode)
      .format("console")
      .start()

    result
  }

  //TODO: CSV file Still Empty, FIX IT!
  def writeQueryCSV(dataFrame: DataFrame,path:String):StreamingQuery={
    val result=dataFrame.writeStream
      .format("csv")
      .option("path","/home/hduser/Desktop/eksperimen/ravi/output/"+path)
      .option("checkpointLocation","/home/hduser/Desktop/eksperimen/ravi/checkpoint/"+path)
      .outputMode(OutputMode.Append)
      .start()

    result

  }

  //TODO: KAFKA WRITE ON SCHEMA

  //TODO: KAFKA WRITE ON AVRO
  def writeQueryKafka(query:DataFrame, mode:String,topic:String,hostPort:String,checkpoint:String):Unit={
    val topic_df=query.selectExpr("to_json(struct(start,end)) AS key","to_json(struct(*)) AS value")
    topic_df.printSchema()
//    val ds=topic_df.writeStream
//      .format("kafka")
//      .outputMode(mode)
//      .option("kafka.bootstrap.servers",hostPort)
//      .option("checkpointLocation", "/home/hduser/Desktop/checkpoint/twitter/"+checkpoint)
//      .option("topic",topic)
//      .start()
//
//    ds
  }

}
