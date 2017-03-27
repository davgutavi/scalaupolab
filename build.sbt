name := "UpoLab"

version := "1.0"

//para Spark 1.6.2 sólo se puede usar Scala 2.10.0
//si se va a usar Java 8, la versión de Scala tiene que ser igual o superior a la 2.10.3
scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0",
  //se realiza la importación de spark_csv por usar la versión 1.6.2, a partir de la versión 2.0.0 de
  //Spark la lectura de csvs va incluida
  "com.databricks" % "spark-csv_2.10" % "1.5.0"
)