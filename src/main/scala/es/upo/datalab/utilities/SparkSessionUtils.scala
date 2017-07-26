package es.upo.datalab.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}


object SparkSessionUtils {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  //############################Configuración en intellij
   val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("endesa").
    set("spark.local.dir","/mnt/datos/tempSpark").
    set("spark.speculation", "false").
    set("spark.scheduler.mode", "FAIR").
    set("spark.network.timeout","10000000").
    set("spark.executor.heartbeatInterval","10000").
    set("spark.executor.memory","20g").
    set("spark.driver.memory","10g").
    set("spark.executor.memory","10g").
    set("spark.logConf","true")

  //############################Submit en local
//  val conf = new SparkConf().
//    setMaster("local[*]").
//    setAppName("endesa").
//    set("spark.local.dir","hdfs://192.168.47.247/user/tempSpark").
//    set("spark.speculation", "false").
//    set("spark.scheduler.mode", "FAIR").
//    set("spark.network.timeout","10000000").
//    set("spark.executor.heartbeatInterval","10000").
//    set("spark.executor.memory","20g").
//    set("spark.logConf","true")

  //############################Submit en cluster
//  val conf = new SparkConf().
//    setMaster("mesos://192.168.47.247:5050").
//    setAppName("endesa").
//    set("spark.local.dir","hdfs://192.168.47.247/user/tempSpark").
//    set("spark.speculation", "false").
//    set("spark.scheduler.mode", "FAIR").
//    set("spark.network.timeout","10000000").
//    set("spark.executor.heartbeatInterval","10000").
//    set("spark.driver.memory","40g").
//    set("spark.executor.memory","40g")



  val session = SparkSession.builder().config(conf).getOrCreate()
  val context = session.sparkContext
  context.setCheckpointDir("hdfs://192.168.47.247/user/checkpointSpark")
  val sql = session.sqlContext


}