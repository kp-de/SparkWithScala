package PkgTransformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object joins_using_db_tables extends App {


  val spark = SparkSession.builder().appName("ReadingFromDB").master("local[*]").getOrCreate()


def getData(tablename:String ) =  spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable",s"public.$tablename")
    .load()

val salaries_DF = getData("salaries")
val emp_DF = getData("employees")


  // Problem statement: Get max salary of every employee (salaries change over time)

  val max_salary_DF = salaries_DF.groupBy("emp_no").max("salary")
  val emp_max_sal_DF = emp_DF.join(max_salary_DF, emp_DF.col("emp_no")=== max_salary_DF.col("emp_no"))
  emp_max_sal_DF.show()

  // Problem statement: Get all employees who were never managers

  val dept_DF = getData("dept_manager")
  val managers_DF = dept_DF.select("emp_no").distinct()

  val emp_never_manager_DF = emp_DF.join(managers_DF,emp_DF.col("emp_no")=== managers_DF.col("emp_no"),"left_anti")
  emp_never_manager_DF.show()

}
