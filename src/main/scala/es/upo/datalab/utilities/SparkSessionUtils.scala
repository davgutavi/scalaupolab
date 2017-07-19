package es.upo.datalab.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}


object SparkSessionUtils {


//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)


  //############################Configuración en intellij
//    val sparkSession = SparkSession.builder().
//      appName("upolab").
//      master("local[*]").
//      config("spark.local.dir","/mnt/datos/tempSpark").
//      config("spark.scheduler.mode", "FAIR").
//      config("spark.network.timeout","10000000").
//      config("spark.executor.heartbeatInterval","10000").
//      config("spark.driver.memory","10g").
//      config("spark.executor.memory","10g").
//      getOrCreate()
  //############################

  //############################Submit en local
    val sparkSession = SparkSession.builder().
      appName("upolab").
      master("local[*]").
      config("spark.scheduler.mode", "FAIR").
      config("spark.network.timeout","10000000").
      config("spark.executor.heartbeatInterval","10000").
      config("spark.executor.memory","20g").
      config("spark.logConf","true").
      getOrCreate()
  //############################

//############################Submit en cluster
//  val sparkSession = SparkSession.builder().
//    appName("upolab").
////    master("mesos://192.168.47.247:7078").
////    config("spark.scheduler.mode", "FAIR").
////    config("spark.network.timeout","10000000").
////    config("spark.executor.heartbeatInterval","10000").
//    config("spark.driver.memory","40g").
//    config("spark.executor.memory","40g").
//    getOrCreate()
  //############################




  sparkSession.sparkContext.setCheckpointDir("hdfs://192.168.47.247/user/gutierrez/checkpointSpark")


  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext


}