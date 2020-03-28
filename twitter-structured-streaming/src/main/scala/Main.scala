import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {
    val bootstrapServers="127.0.0.1:9092"

    val session=initializeSpark("twitter-structured-streaming","local[*]")
    val twitterTask:TwitterTask=new TwitterTask(session,bootstrapServers)
    twitterTask.subscribe(session,"twitter")
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
