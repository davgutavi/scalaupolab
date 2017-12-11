package es.upo.datalab.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.io.File

import com.typesafe.config.ConfigFactory



object SparkSessionUtils {

//  final val propFile = "/Users/davgutavi/Desktop/sparkProperties.properties"
  final val propFile = "/home/davgutavi/Escritorio/sparkProperties.properties"
  final val emptyValue = ""

  //############################LECTURA DE CONFIGURACIÓN LOCAL

  val config            = ConfigFactory.parseFile(new File((propFile)))


  val checkPointDir     = config.getString("checkPointDir")

  val fsS3aImpl         = config.getString("fs.s3a.impl")
  val fsS3aAccess       = config.getString("fs.s3a.access.key")
  val fsS3aSecret       = config.getString("fs.s3a.secret.key")
  val master            = config.getString("master")
  val appName           = config.getString("app_name")
  val localDir          = config.getString("spark.local.dir")
  val timeout           = config.getString("spark.network.timeout")
  val heartbeatInterval = config.getString("spark.executor.heartbeatInterval")
  val executorMemory    = config.getString("spark.executor.memory")
  val driverMemory      = config.getString("spark.driver.memory")
  val coresMax          = config.getString("spark.cores.max")
  val executorUri       = config.getString("spark.executor.uri")
  val org = config.getString("org")
  if (!org.equalsIgnoreCase(""))   Logger.getLogger("org").setLevel(Level.OFF)
  val akka = config.getString("akka")
  if (!akka.equalsIgnoreCase(""))   Logger.getLogger("akka").setLevel(Level.OFF)


  //############################LECTURA DE CONFIGURACIÓN CLUSTER-CLIENT

//    val appName           = "nabo_chico_454d_con_slo"
//    val appName           = "nabo_chico_454d_nrl_slo"
//    val appName           = "nabo_chico_364d_con_slo"
//  val appName           = "nabo_chico_364d_nrl_slo"

//    val appName           = "nabo_gordo_454d_con_slo"
////    val appName           = "nabo_gordo_454d_nrl_slo"
////   val appName           = "nabo_gordo_364d_con_slo"
////    val appName           = "nabo_gordo_364d_nrl_slo"
//
//
//    val checkPointDir     = "hdfs://192.168.47.247/user/checkpointSpark"
//    val master            = "mesos://192.168.47.247:5050"
//
//    val localDir          = "hdfs://192.168.47.247/user/tempSpark"
//    val timeout           = ""
//    val heartbeatInterval = ""
//    val executorMemory    = ""
//    val driverMemory      = ""
//    val coresMax          = ""
//    val executorUri       = "hdfs://192.168.47.247/user/spark-2.2.0-bin-hadoop2.7.tgz"
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)


  //############################LECTURA DE CONFIGURACIÓN CLUSTER-CLUSTER

//  //    val appName           = "nabo_chico_454d_con_slo"
//  //    val appName           = "nabo_chico_454d_nrl_slo"
//  //    val appName           = "nabo_chico_364d_con_slo"
//  //  val appName           = "nabo_chico_364d_nrl_slo"
//
////  val appName           = "nabo_gordo_454d_con_slo"
////      val appName           = "nabo_gordo_454d_nrl_slo"
//     val appName           = "nabo_gordo_364d_con_slo"
//  //    val appName           = "nabo_gordo_364d_nrl_slo"
//
//
//  val checkPointDir     = "hdfs://192.168.47.247/user/checkpointSpark"
//  val master            = "mesos://192.168.47.247:7077"
//  val localDir          = "hdfs://192.168.47.247/user/tempSpark"
//  val timeout           = ""
//  val heartbeatInterval = ""
//  val executorMemory    = ""
//  val driverMemory      = ""
//  val coresMax          = ""
//  val executorUri       = ""
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)


  //############################LECTURA DE CONFIGURACIÓN LOCAL-TERMINAL
//  val checkPointDir     = "/mnt/datos/checkpointSpark"
//  val master            = "local"
//  val appName           = "endesa_xgboost_02"
//  val localDir          = "/mnt/datos/tempSpark"
//  val timeout           = ""
//  val heartbeatInterval = ""
//  val executorMemory    = ""
//  val driverMemory      = ""
//  val coresMax          =  ""
//  val executorUri       = ""
//  Logger.getLogger("org").setLevel(Level.OFF)
//  Logger.getLogger("akka").setLevel(Level.OFF)







  //############################CONFIGURACIÓN DE SESIÓN
  val session = SparkSession.builder().master(master).appName(appName).getOrCreate()

  if (!localDir.equalsIgnoreCase(emptyValue))          session.conf.set("spark.local.dir",localDir)
  if (!timeout.equalsIgnoreCase(emptyValue))           session.conf.set("spark.network.timeout",timeout)
  if (!heartbeatInterval.equalsIgnoreCase(emptyValue)) session.conf.set("spark.executor.heartbeatInterval",heartbeatInterval)
  if (!executorMemory.equalsIgnoreCase(emptyValue))    session.conf.set("spark.executor.memory",executorMemory)
  if (!driverMemory.equalsIgnoreCase(emptyValue))      session.conf.set("spark.driver.memory",driverMemory)
  if (!coresMax.equalsIgnoreCase(emptyValue))          session.conf.set("spark.cores.max",coresMax)
  if (!executorUri.equalsIgnoreCase(emptyValue))       session.conf.set("spark.executor.uri",executorUri)

  //############################CONFIGURACIÓN DE AWS
//  if (!fsS3aImpl.equalsIgnoreCase(""))   session.sparkContext.hadoopConfiguration.set("fs.s3a.impl",fsS3aImpl)
//  if (!fsS3aAccess.equalsIgnoreCase("")) session.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",fsS3aAccess)
//  if (!fsS3aSecret.equalsIgnoreCase("")) session.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",fsS3aSecret)

  //############################CONFIGURACIÓN DE CHECKPOINT
//  if (!checkPointDir.equalsIgnoreCase(emptyValue)) session.sparkContext.setCheckpointDir(checkPointDir)

  //############################ACCESOS
  val sc = session.sparkContext
  val sql = session.sqlContext

}