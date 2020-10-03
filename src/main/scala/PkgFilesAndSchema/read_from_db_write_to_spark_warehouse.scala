package PkgFilesAndSchema

import org.apache.spark.sql.{SaveMode, SparkSession}

object read_from_db_write_to_spark_warehouse extends App {

  val spark = SparkSession.builder().appName("ReadingFromDB").master("local[*]")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
    .getOrCreate()

  def readTableFromDB(tableName: String) = {
    spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://localhost:5432/rtjvm")
      .option("user","docker")
      .option("password","docker")
      .option("dbtable",s"public.$tableName")
      .load()

  }

  def readMultipleTablesWritetoSpark(tableList: List[String]) =  tableList.foreach { tableName =>
    val tableDF = readTableFromDB(tableName)
    tableDF.write
        .mode("Overwrite")
        .saveAsTable(tableName)

  }

 val tables = List("employees","salaries","departments","dept_manager")
 readMultipleTablesWritetoSpark(tables)


}
