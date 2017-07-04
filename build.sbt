name := "UpoLab"
version := "1.0.0"
//para Spark 1.6.2 sólo se puede usar Scala 2.10.0
//si se va a usar Java 8, la versión de Scala tiene que ser igual o superior a la 2.10.3
//scalaVersion := "2.10.6"
scalaVersion := "2.11.8"
val sparkVersion = "2.1.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
  ,"org.apache.spark" %% "spark-sql" % sparkVersion
  //,"org.apache.spark" %% "spark-mllib" % sparkVersion
  //,"org.apache.spark" %% "spark-hive" % sparkVersion
)


assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := "endesa_"+version+".jar"
mainClass in assembly := Some("es.upo.datalab.entrypoints.datasets.LecturasIA")
mainClass := Some("es.upo.datalab.entrypoints.datasets.LecturasIA")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}






/* you need to be able to undo the "provided" annotation on the deps when running your spark
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
//fullClasspath in Runtime := (fullClasspath in (Compile, run)).value