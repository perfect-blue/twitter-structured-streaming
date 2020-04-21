import TwitterSchema.payloadStruct
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.concurrent.duration._

object Utillities {

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
//      .trigger(Trigger.ProcessingTime("130 seconds"))
      .format("console")
      .option("truncated",false)
      .start()

    result
  }

  //TODO: CSV file Still Empty, FIX IT!
  def writeQueryCSV(dataFrame: DataFrame,path:String):StreamingQuery={
    val result=dataFrame.writeStream
      .format("csv")
      .option("header",true)
      .option("truncated",true)
      .option("path","/home/hduser/Desktop/eksperimen/ravi/output/"+path)
      .option("checkpointLocation","/home/hduser/Desktop/eksperimen/ravi/checkpoint/"+path)
      .outputMode(OutputMode.Append)
      .start()

    result
  }

  //TODO: KAFKA WRITE ON SCHEMA

  def writeQueryKafka(dataFrame:DataFrame,topic:String,hostPort:String,checkpoint:String):StreamingQuery={
    val kafkaOutput = dataFrame.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", hostPort)
      .option("topic", topic)
      .option("checkpointLocation", "/home/hduser/Desktop/eksperimen/ravi/kafka/checkpoint/"+checkpoint)
      .outputMode("update")
      .start()

    kafkaOutput
  }

}
