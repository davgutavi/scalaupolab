import sbt.Keys.scalaVersion

name         := "endesa"
version      := "1.0.0"
scalaVersion := "2.11.11"

val sparkVersion = "2.2.1"

//intellij
libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-core_2.11"  % sparkVersion
  , "org.apache.spark" % "spark-sql_2.11"   % sparkVersion
  , "org.apache.spark" % "spark-mllib_2.11" % sparkVersion
  , "com.typesafe"     % "config"           % "1.3.1"
)

//AWS
//libraryDependencies ++= Seq(
//   "org.apache.spark" % "spark-core_2.11" % sparkVersion
//  ,"org.apache.spark" % "spark-sql_2.11" % sparkVersion
//  ,"org.apache.spark" % "spark-mllib_2.11" % sparkVersion
//  ,"org.apache.hadoop" % "hadoop-aws" % "2.8.1"
//  ,"com.googlecode.netlib-java" % "netlib-java" % "1.1"
//  ,"com.typesafe" % "config" % "1.3.1"
//)

//submit
//libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-core_2.11"  % sparkVersion % "provided"
// ,"org.apache.spark" % "spark-sql_2.11"   % sparkVersion % "provided"
// ,"org.apache.spark" % "spark-mllib_2.11" % sparkVersion % "provided"
// ,"org.apache.spark" % "spark-hive_2.11"  % sparkVersion % "provided"
//)