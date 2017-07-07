package es.upo.datalab.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}


object SparkSessionUtils {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  //############################Configuración en intellij
    val sparkSession = SparkSession.builder().
      appName("upolab").
      master("local[*]").
      config("spark.scheduler.mode", "FAIR").
      config("spark.speculation", "false").
      config("spark.network.timeout","10000000").
      config("spark.executor.heartbeatInterval","10000000").
      config("spark.local.dir","/mnt/datos/tempSpark").
      config("spark.driver.memory","10g").
      config("spark.executor.memory","10g").
      config("spark.local.dir","/mnt/datos/tempSpark").
      getOrCreate()

//############################Configuración para el submit
//  val sparkSession = SparkSession.builder().
//    appName("upolab").
//    config("spark.speculation", "false").
//    config("spark.network.timeout","10000000").
//    config("spark.executor.heartbeatInterval","10000000").
//    getOrCreate()

  sparkSession.sparkContext.setCheckpointDir("hdfs://192.168.47.247/user/gutierrez/checkpointSpark")


  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext


}