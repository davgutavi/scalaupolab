package es.upo.datalab.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}


object SparkSessionUtils {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  //############################Configuración en intellij
//   val conf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("endesa_modo_desarrollo")
//      .set("spark.local.dir","/mnt/datos/tempSpark")
//      .set("spark.network.timeout","10000000")
//      .set("spark.executor.heartbeatInterval","10000")
//      .set("spark.executor.memory","20g")
//      .set("spark.driver.memory","10g")
//      .set("spark.executor.memory","10g")


  //############################Submit desde gutierrez en cluster mode
  val conf = new SparkConf()
    .setMaster("mesos://192.168.47.247:5050")
    .setAppName("tab24n_modo_cluster_david")
    .set("spark.local.dir","hdfs://192.168.47.247/user/tempSpark")
    .set("spark.cores.max", "48")




  //############################Submit desde gutierrez en client mode
//    val conf = new SparkConf()
//      .setMaster("mesos://192.168.47.247:5050")
//      .setAppName("endesa_modo_cliente")
//      .set("spark.local.dir","hdfs://192.168.47.247/user/tempSpark")
//      .set("spark.cores.max", "48")
    .set("spark.executor.uri", "hdfs://192.168.47.247/user/spark-2.1.1-bin-hadoop2.6.tgz")


    val session = SparkSession.builder().config(conf).getOrCreate()
    val context = session.sparkContext
    context.setCheckpointDir("hdfs://192.168.47.247/user/checkpointSpark")
    val sql = session.sqlContext




// configuraciones antiguas
//      config("spark.speculation", "false").
//      config("spark.network.timeout","10000000").
//      config("spark.executor.heartbeatInterval","10000000").

//  val session = SparkSession.builder().
//  val session = SparkSession.builder().
//    appName("upolab").
//    config("spark.local.dir","hdfs://192.168.47.247/user/tempSpark").
//    config("spark.cores.max", "48").
//    getOrCreate()


}