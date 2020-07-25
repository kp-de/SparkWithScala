package PkgSparkScala

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, TypedColumn}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.io.Source
import org.apache.spark
import org.apache.spark.rdd._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}




object Airport_Analysis_Classes extends App {

  class AirportData (FileLocation:String){

    def getData:RDD[String] = {
      val spark = SparkSession
        .builder
        .appName("SparkWithScala")
        .master("local[*]")
        .getOrCreate()

      val data_RDD = spark.sparkContext.textFile(this.FileLocation)
      data_RDD

    }

    def filterCountry(Data: RDD[String], Country: String):RDD[String] = {
      println(Country)
      println(Data.count())
    Data.filter(record => record.split(",")(3) == "\"" + Country+"\"")
    }



  }

  val file = "resources/airports.txt"
  val airports = new AirportData(file)
  val data_rdd = airports.getData


  //data_rdd.collect().foreach(println)

  // Now filtering data based on country
  val filteredCountryData = airports.filterCountry(data_rdd,"Canada")
  filteredCountryData.collect().foreach(println)





}
