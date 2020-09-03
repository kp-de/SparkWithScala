Setting up a new IntelliJ project for Spark with Scala:

Step 1: 
Right click on your project, "Add Framework support" and choose Scala framework, then by right click on the packages you can create Scala Class.

After this, right click on src > Mark directory as > Sources Root.

Step 2: Add dependencies for Spark, check the versions on your system and change accordingly

name := "SparkWithScala"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.6",    
  "org.apache.spark" %% "spark-sql" % "2.4.6",    
  // for kafka dependency    
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.6"
  
)

