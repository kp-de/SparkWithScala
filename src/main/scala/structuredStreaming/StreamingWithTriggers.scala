package structuredStreaming


import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import  scala.concurrent.duration._
import org.apache.log4j.{Level, Logger}


object StreamingWithTriggers {

  val spark = SparkSession.builder()
    .appName("First Streaming App")
    .master("local[1]")
    .getOrCreate()


  // Reading a Dataframe

  def DemoTriggers() = {
    val lines : DataFrame = spark.readStream.format("socket")
      .option("host","localhost")
      . option("port",12345)
      .load()

    // check if static or streaming DF
    println(lines.isStreaming)

    // Consuming a dataframe

    val query = lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))    /// create a new batch every 2 seconds
      // .trigger(Trigger.Once())                       /// Trigger only once and exit
      //.trigger(Trigger.Continuous(2.seconds))           /// Continuous triggers
      .start()

    query.awaitTermination()



  }

  def main(args: Array[String]): Unit = {
    DemoTriggers()

  }
}