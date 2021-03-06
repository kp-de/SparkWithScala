package PkgFilesAndSchema

import org.apache.spark.sql.SparkSession

object readFromDB extends App {

  val spark = SparkSession.builder().appName("ReadingFromDB").master("local[*]").getOrCreate()

  val dbDF = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  dbDF.show()
}
