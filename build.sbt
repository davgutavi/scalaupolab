import sbt.Keys.scalaVersion

lazy val root = (project in file(".")).
  settings(
    name := "endesa_xgboost_nabogoldo"
    ,version := "20"
    ,scalaVersion := "2.11.11"
    ,mainClass in Compile := Some("es.upo.datalab.datamining.GBTpendientes")
//    ,mainClass in Compile := Some("es.upo.datalab.entrypoints.tests.Test01")
//    ,mainClass in Compile := Some("es.upo.datalab.entrypoints.procesos.Tab24PreProcessing")
//    ,mainClass in Compile := Some("es.upo.datalab.entrypoints.procesos.McE")
//    ,mainClass in Compile := Some("es.upo.datalab.utilities.CsvToParquet")
    ,assemblyJarName in assembly := name+""+version+".jar"
    ,test in assembly := {}
    ,fork := true
  )

val sparkVersion = "2.2.0"

//intellij

//libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-core_2.11" % sparkVersion
// ,"org.apache.spark" % "spark-sql_2.11" % sparkVersion
// ,"org.apache.spark" % "spark-mllib_2.11" % sparkVersion
//// ,"org.apache.hadoop" % "hadoop-aws" % "2.8.1"
//// ,"com.googlecode.netlib-java" % "netlib-java" % "1.1"
// ,"com.typesafe" % "config" % "1.3.1"
//)

//submit


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11"  % sparkVersion % "provided"
 ,"org.apache.spark" % "spark-sql_2.11"   % sparkVersion % "provided"
 ,"org.apache.spark" % "spark-mllib_2.11" % sparkVersion % "provided"
 ,"org.apache.spark" % "spark-hive_2.11"  % sparkVersion % "provided"
  ,"com.typesafe" % "config" % "1.3.1"
)


//libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-core_2.11"  % sparkVersion
//  ,"org.apache.spark" % "spark-sql_2.11"   % sparkVersion
//  ,"org.apache.spark" % "spark-mllib_2.11" % sparkVersion
//  ,"org.apache.spark" % "spark-hive_2.11"  % sparkVersion
//  ,"com.typesafe" % "config" % "1.3.1"
//)


assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

}