package com.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object SparkSessionUtils {


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  //SPARK 1.6.2
//  val sparConf:SparkConf = new SparkConf()
//    .setAppName("upolab")
//    .setMaster("local[*]")
//    .setMaster("spark://192.168.47.241:7077")
//    .set("spark.scheduler.mode", "FAIR")
//  val sc:SparkContext = new SparkContext(sparConf)
//  val sqlContext:SQLContext =  new org.apache.spark.sql.SQLContext(sc)


  //SPARK 2.0.2

    val sparkSession = SparkSession.builder().
      appName("upolab").
      master("local[*]").
//      master("spark://192.168.1.10:7077")
//      config("spark.scheduler.mode", "FAIR").
      getOrCreate()
    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

}