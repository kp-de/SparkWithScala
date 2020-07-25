package PkgSparkScala

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, TypedColumn}
import org.apache.spark._

object AirportCSVIntoDataframe extends App{

  val spark = SparkSession
    .builder
    .appName("SparkWithScala")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.format("csv").load("resources/airports.txt")
  println(df.count())
  println(df.getClass)



}
