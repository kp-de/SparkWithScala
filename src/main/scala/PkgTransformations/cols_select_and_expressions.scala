package PkgTransformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object cols_select_and_expressions extends App {


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

  // Declare a column object using a field in the DF
  val nameCol = newDF.col("Name")
  // Use select method to read a field from the DF
  val nameDF = newDF.select(nameCol)
  nameDF.show()

  // adding a column to the DF

  val newColDF = newDF.withColumn("DoubleHP",newDF.col("Horsepower")*2)
  newColDF.show()

  // renaming a column
  val renamedColDF = newColDF.withColumnRenamed("Origin","Country_of_Origin")
  renamedColDF.show()

  // selectExpr

  val selectExprDF = newDF.selectExpr(
    "Name",
    "Horsepower",
    "Horsepower * 2 as DoubleHorsePower"
  )

  selectExprDF.show()

}
