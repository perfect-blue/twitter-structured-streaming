import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {

  /*
  TODO: USE ARGUMENTS TO INPUT PARAMETER(Master,Topic,Bootstrap,Window)
   */

  def main(args: Array[String]): Unit = {
    val bootstrapServers="127.0.0.1:9092"

    val session=initializeSpark("twitter-structured-streaming","local[4]")
    val twitterTask:TwitterTask=new TwitterTask(session,bootstrapServers,"twitter-topic")
    twitterTask.subscribe()
  }

  def initializeSpark(name:String, master:String): SparkSession={

    val session = SparkSession
      .builder()
      .appName(name)
      .master(master)
      .getOrCreate()


    session;
  }


}
