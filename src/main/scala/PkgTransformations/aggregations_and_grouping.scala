package PkgTransformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._




object aggregations_and_grouping extends App {

  val spark = SparkSession.builder().appName("WriteParquet").master("local[*]").getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema" , "True")
    .json("resources/movies.json")

  moviesDF.printSchema()


  // total count of records including nulls
  println(moviesDF.count())

  // avg ratings per genre

 val avg_rating_by_genre = moviesDF.groupBy("Major_Genre").avg("Rotten_Tomatoes_Rating")
 avg_rating_by_genre.show()

 // Distinct count of directors

  val distinct_dir_count = moviesDF.select(countDistinct("Director"))
  (distinct_dir_count).show()

  // Avg Profit

  val avg_profit = moviesDF.selectExpr("avg(US_Gross + Worldwide_Gross + US_DVD_Sales - Production_Budget)  as avg_profit")
  avg_profit.printSchema()
  avg_profit.show()

  // multiple aggregations - descending order


 val multiple_aggs = moviesDF.groupBy("Major_Genre").
   agg(sum("US_Gross").as("US_Gross_Revenue"),
     count("Title").as("Movies_Count"))
   .orderBy(desc("US_Gross_Revenue"))

  multiple_aggs.show()


}
