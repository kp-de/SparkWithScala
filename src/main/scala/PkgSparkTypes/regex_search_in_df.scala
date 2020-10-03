package PkgSparkTypes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}

object regex_search_in_df extends App {
  val spark = SparkSession.builder().master("local[*]").appName("Regex_Search").getOrCreate()

  val carsDF = spark.read
      .option("inferSchema","true")
    .json("resources/cars.json")

  carsDF.show()

  // problem statement : find cars which have name like chevrolet, ford or volkswagen (this given as a list)

  val list_Names = List("chevrolet","volkswagen","ford")

  def getRegexString( names: List[String]):String =
  {
  names.mkString("|")

  }

  // Filter out all the records where our regex extract is null
  val filteredCarsDF = carsDF.select(col("Name"),regexp_extract(col("Name"),getRegexString(list_Names),0).as("Regex_column"))
    .where(col("Regex_column") =!= "")

  filteredCarsDF.show()



}
