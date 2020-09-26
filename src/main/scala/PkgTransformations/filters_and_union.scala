package PkgTransformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object filters_and_union extends App {

  val spark = SparkSession.builder().appName("WriteParquet").master("local[*]").getOrCreate()

  val mySchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),             // Date type declaration
    StructField("Origin", StringType)
  ))

  val newDF = spark.read
    .schema(mySchema)
    .option("dateFormat","YYYY-MM-DD")
    .json("resources/cars.json")

  // Only USA origin cars
  val UScarsDF = newDF.filter("Origin = 'USA'")
  UScarsDF.show()

  // multiple filters = filter cascading
  val USandJapancarsDF = newDF.filter("Origin = 'USA' or Origin = 'Japan'")
  USandJapancarsDF.show()
  USandJapancarsDF.select("Origin").distinct().show()

  // another way for multiple filters

  val USandEUCars = newDF.filter(col("Origin") ==="USA" or col("Origin") === "Europe")
  USandEUCars.show()
  USandEUCars.select("Origin").distinct().show()

  // union
  val US_EU_Japan_cars = USandJapancarsDF.union(USandEUCars)
  US_EU_Japan_cars.select("Origin").distinct().show()


}
