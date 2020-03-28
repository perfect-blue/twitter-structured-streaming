import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, StructType}

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

  def writeTo(): Unit ={

  }

}
