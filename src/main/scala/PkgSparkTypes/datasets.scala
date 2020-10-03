package PkgSparkTypes

import java.sql.Date

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object datasets extends App {

  val spark = SparkSession.builder().master("local[*]").appName("Regex_Search").getOrCreate()

  val carsDF = spark.read
    .option("inferSchema","true")
    .json("resources/cars.json")

  carsDF.write
    .mode("Overwrite")
    .saveAsTable("Cars_Table")

  case class Cars(
                   Name: String,
                  Miles_per_Gallon: Double,
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Long,
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: Date,
                  Origin: String
                 )

  import spark.implicits._

  val carsDS = carsDF.as[Cars]
  carsDS.show()


}
