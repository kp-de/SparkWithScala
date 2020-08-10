package structuredStreaming
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingDataFrames {

  val spark = SparkSession.builder()
    .appName("First Streaming App")
    .master("local[1]")
    .getOrCreate()

  // Reading a Dataframe

  def readFromSocket() = {
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
      .start()

    query.awaitTermination()



  }

  def main(args: Array[String]): Unit = {
    readFromSocket()

  }
}
