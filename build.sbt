import sbt.Keys.scalaVersion
import sbtassembly.AssemblyPlugin.autoImport.{assemblyJarName, _}

lazy val root = (project in file(".")).
  settings(
    name := "endesa"
    ,version := "1.0.1"
    ,scalaVersion := "2.11.6"
    ,mainClass in Compile := Some("es.upo.datalab.entrypoints.general.CstToParquet")
    //,assemblyJarName in assembly := "endesa_"+version+".jar"
    ,test in assembly := {}
    ,fork := true
  )


val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
  ,"org.apache.spark" %% "spark-sql" % sparkVersion
  ,"org.apache.spark" %% "spark-mllib" % sparkVersion
  ,"org.apache.spark" %% "spark-hive" % sparkVersion
)

//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion% "provided"
//  ,"org.apache.spark" %% "spark-sql" % sparkVersion% "provided"
//  ,"org.apache.spark" %% "spark-mllib" % sparkVersion% "provided"
//  ,"org.apache.spark" %% "spark-hive" % sparkVersion% "provided"
//)

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}

