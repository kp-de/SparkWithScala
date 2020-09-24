package PkgFilesAndSchema
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object json_schema_with_date extends App {

  val spark = SparkSession.builder().appName("JSON_With_Date").master("local[*]").getOrCreate()

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

  newDF.show()


}

