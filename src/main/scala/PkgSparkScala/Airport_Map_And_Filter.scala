package PkgSparkScala

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._




object Airport_Map_And_Filter extends App {



    println("First Spark Invocation from Scala Project")

    val spark = SparkSession
      .builder
      .appName("MyFirstSparkStreaming")
      .master("local[*]")
      .getOrCreate()

    val airport_df = spark.sparkContext.textFile("resources/airports.txt")
    //print(airport_df.first())

  // creating a new df with only Canada airports
  // fourth value in each record is country

  // To get the type of object
  println(airport_df.getClass)

  val filtered_rdd = airport_df.filter(airport => airport.split(",")(3) == "\"Canada\"" )
  println(filtered_rdd.count())

  val canada_airports = filtered_rdd.map(airport => airport.split(",")(1))
  canada_airports.collect().foreach(println)






  // split_df.collect().foreach(println)


}
