package PkgSparkScala
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



object HelloSpark extends App {

  println("First Spark Invocation from Scala Project")

  val spark = SparkSession
    .builder
    .appName("MyFirstSparkStreaming")
    .master("local[*]")
    .getOrCreate()

val df = spark.sparkContext.textFile("sample-file.txt")
val ls = df.collect()
println(ls.size)


}
