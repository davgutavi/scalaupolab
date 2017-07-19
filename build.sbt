import sbt.Keys.scalaVersion
import sbtassembly.AssemblyPlugin.autoImport.{assemblyJarName, _}

lazy val root = (project in file(".")).
  settings(
    name := "endesa"
    ,version := "1.2.1"
    ,scalaVersion := "2.11.6"
    ,mainClass in Compile := Some("es.upo.datalab.entrypoints.general.CstToParquet")
    //,assemblyJarName in assembly := "endesa_"+version+".jar"
    ,test in assembly := {}
    ,fork := true
  )

val sparkVersion = "2.1.1"

//intellij

//libraryDependencies ++= Seq(
//
//  "org.apache.spark" % "spark-core_2.11" % sparkVersion
//  ,"org.apache.spark" % "spark-sql_2.11" % sparkVersion
//  ,"org.apache.spark" % "spark-mllib_2.11" % sparkVersion
//  ,"org.apache.spark" % "spark-hive_2.11" % sparkVersion
//  //,"org.slf4j" % "slf4j-api" % "1.7.15"
//  //,"org.slf4j" % "slf4j-simple" % "1.7.15"
//)

//submit

libraryDependencies ++= Seq(
   "org.apache.spark" % "spark-core_2.11"  % sparkVersion % "provided"
  ,"org.apache.spark" % "spark-sql_2.11"   % sparkVersion % "provided"
  ,"org.apache.spark" % "spark-mllib_2.11" % sparkVersion % "provided"
  ,"org.apache.spark" % "spark-hive_2.11"  % sparkVersion % "provided"
)

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}

