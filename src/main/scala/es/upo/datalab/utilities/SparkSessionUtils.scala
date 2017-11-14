package es.upo.datalab.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.io.File

object SparkSessionUtils {

  //############################LECTURA DE CONFIGURACIÓN LOCAL
//  val config            = ConfigFactory.parseFile(new File(("/home/davgutavi/Escritorio/sparkProperties.properties")))
//  val checkPointDir     = config.getString("checkPointDir")
//  val fsS3aImpl         = config.getString("fs.s3a.impl")
//  val fsS3aAccess       = config.getString("fs.s3a.access.key")
//  val fsS3aSecret       = config.getString("fs.s3a.secret.key")
//  val master            = config.getString("master")
//  val appName           = config.getString("app_name")
//  val localDir          = config.getString("spark.local.dir")
//  val timeout           = config.getString("spark.network.timeout")
//  val heartbeatInterval = config.getString("spark.executor.heartbeatInterval")
//  val executorMemory    = config.getString("spark.executor.memory")
//  val driverMemory      = config.getString("spark.driver.memory")
//  val coresMax          = config.getString("spark.cores.max")
//  val executorUri       = config.getString("spark.executor.uri")
//  val org = config.getString("org")
//  if (!org.equalsIgnoreCase(""))   Logger.getLogger("org").setLevel(Level.OFF)
//  val akka = config.getString("akka")
//  if (!akka.equalsIgnoreCase(""))   Logger.getLogger("akka").setLevel(Level.OFF)


  //############################LECTURA DE CONFIGURACIÓN CLUSTER-CLIENT
//    val checkPointDir     = "hdfs://192.168.47.247/user/checkpointSpark"
//    val master            = "mesos://192.168.47.247:5050"
//    val appName           = "endesa_xgboost_01"
//    val localDir          = "hdfs://192.168.47.247/user/tempSpark"
////  val timeout           = config.getString("spark.network.timeout")
////  val heartbeatInterval = config.getString("spark.executor.heartbeatInterval")
//    val executorMemory    = "20g"
//    val driverMemory      = "10g"
//    val coresMax          =  "48"
////  val executorUri       = config.getString("spark.executor.uri")
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)


  //############################LECTURA DE CONFIGURACIÓN LOCAL-TERMINAL
  val checkPointDir     = "/mnt/datos/checkpointSpark"
  val master            = "local"
  val appName           = "endesa_xgboost_02"
  val localDir          = "/mnt/datos/tempSpark"
    val timeout           = ""
    val heartbeatInterval = ""
  val executorMemory    = ""
  val driverMemory      = ""
  val coresMax          =  ""
    val executorUri       = ""
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)







  //############################CONFIGURACIÓN DE SESIÓN
  val session = SparkSession.builder().master(master).appName(appName).getOrCreate()

  if (!localDir.equalsIgnoreCase(""))          session.conf.set("spark.local.dir",localDir)
//  if (!timeout.equalsIgnoreCase(""))           session.conf.set("spark.network.timeout",timeout)
//  if (!heartbeatInterval.equalsIgnoreCase("")) session.conf.set("spark.executor.heartbeatInterval",heartbeatInterval)
  if (!executorMemory.equalsIgnoreCase(""))    session.conf.set("spark.executor.memory",executorMemory)
  if (!driverMemory.equalsIgnoreCase(""))      session.conf.set("spark.driver.memory",driverMemory)
  if (!coresMax.equalsIgnoreCase(""))          session.conf.set("spark.cores.max",coresMax)
//  if (!executorUri.equalsIgnoreCase(""))       session.conf.set("spark.executor.uri",executorUri)

  //############################CONFIGURACIÓN DE AWS
//  if (!fsS3aImpl.equalsIgnoreCase(""))   session.sparkContext.hadoopConfiguration.set("fs.s3a.impl",fsS3aImpl)
//  if (!fsS3aAccess.equalsIgnoreCase("")) session.sparkContext.hadoopConfiguration.set("fs.s3a.access.key",fsS3aAccess)
//  if (!fsS3aSecret.equalsIgnoreCase("")) session.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",fsS3aSecret)

  //############################CONFIGURACIÓN DE CHECKPOINT
//  if (!checkPointDir.equalsIgnoreCase("")) session.sparkContext.setCheckpointDir(checkPointDir)

  //############################ACCESOS
  val sc = session.sparkContext
  val sql = session.sqlContext

}