package PkgFilesAndSchema

import org.apache.spark.sql._
object df_from_tuple_and_implicits extends App {

  val spark = SparkSession
    .builder()
    .appName("DF_From_Tuple&Implicits")
    .master("local[*]")
    .getOrCreate()

  val myTuple = Seq(
  (6839,"Allakaket Airport","Allakaket","United States","AET","PFAL",66.5519,-152.6222,441,-9,"A","America/Anchorage"),
    (2910,"Astana Intl","Tselinograd","Kazakhstan","TSE","UACC",51.022222,71.466944,1165,6,"U","Asia/Qyzylorda"),
    (2911,"Taraz","Dzhambul","Kazakhstan","DMB","UADD",42.853611,71.303611,2184,6,"U","Asia/Qyzylorda"),
    (2912,"Manas","Bishkek","Kyrgyzstan","FRU","UAFM",43.061306,74.477556,2058,6,"U","Asia/Bishkek"),
    (2913,"Osh","Osh","Kyrgyzstan","OSS","UAFO",40.608989,72.793269,2927,6,"U","Asia/Bishkek"),
    (2914,"Shymkent","Chimkent","Kazakhstan","CIT","UAII",42.364167,69.478889,1385,6,"U","Asia/Qyzylorda")
  )

  val TupleDF = spark
    .createDataFrame(myTuple)

  TupleDF.show()

  import spark.implicits._
  val TupleDFWithSchema = myTuple.toDF()

TupleDFWithSchema.printSchema()

}
