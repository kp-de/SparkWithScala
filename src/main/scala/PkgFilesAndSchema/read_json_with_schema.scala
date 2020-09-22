package PkgFilesAndSchema

import org.apache.spark.sql._
import org.apache.spark.sql.types._


object read_json_with_schema extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("ReadFromJSONWithSchema")
    .getOrCreate()

  // Allowing Spark to infer schema
  val jsonDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("resources/cars.json")

  jsonDF.show()
  jsonDF.printSchema()

  // Now using a prescribed schema

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val jsonDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("resources/cars.json")

  jsonDFWithSchema.printSchema()




}
