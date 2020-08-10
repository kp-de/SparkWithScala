package structuredStreaming
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {

  val spark = SparkSession.builder()
    .master("local")
    .appName("Streaming Aggregations")
    .getOrCreate()

  def StreamingAggCount() =
  {
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    val groupedLines: DataFrame = lines.selectExpr("count(*) as Rowcount")

    val writestream = groupedLines.writeStream
      .format("console")
      .outputMode("Complete")
      .start()
      .awaitTermination()


  }

  def StreamingSum() = {
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    val numbers = lines.select(col("value").cast("integer").as("numberfield"))
    val sum_of_numbers = numbers.select(sum(col("numberfield")))

    val writeStream = sum_of_numbers.writeStream
        .format("console")
      .outputMode("Complete")
      .start()
      .awaitTermination()


  }

  def GroupedbyName() = {
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    val CountNames = lines.select(col("value").as("name")).groupBy("name").count()

    val writeStream = CountNames.writeStream
      .format("console")
      .outputMode("complete")
      .start().awaitTermination()
  }

  def main(args: Array[String]): Unit = {
   //  StreamingAggCount()
   //  StreamingSum()
    GroupedbyName()

  }

}
