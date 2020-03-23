import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}
import Twitter._

object Main {

  def main(args: Array[String]): Unit = {
    val session=initializeSpark("twitter-structured-streaming","local[*]")
    subscribe(session,"twitter.structured")
  }

  def initializeSpark(name:String, master:String): SparkSession={

    val session = SparkSession
      .builder()
      .appName(name)
      .master(master)
      .getOrCreate()


    return session;
  }


}
