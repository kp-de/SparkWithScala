package PkgFilesAndSchema
import PkgFilesAndSchema.json_schema_with_date.spark
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object write_to_parquet extends App {

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

  newDF.write
    .parquet("resources/cars_parquet.parquet")

}
