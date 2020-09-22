package PkgFilesAndSchema

import org.apache.spark.sql._

object read_from_file extends App {

val spark = SparkSession
      .builder()
      .appName("ReadingFiles")
      .master("local[*]")
      .getOrCreate()

val file_loc = "resources/airports.txt"

val csvfileDF = spark.read
  .textFile(file_loc)

csvfileDF.show()

  // Now inferring schema

 val anotherFileDF = spark.read
     .format("csv")
   .option("inferSchema", "true")
   .load(file_loc)

 anotherFileDF.show()

// now printing the inferred schema

  val mySchema = anotherFileDF.schema
  println(mySchema)

}
