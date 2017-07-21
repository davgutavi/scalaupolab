package es.upo.datalab.entrypoints.tests

import java.sql.{Date, Timestamp}
import java.util.Calendar

import es.upo.datalab.entrypoints.procesos.FromStringToTimestamp
import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 12/07/17.
  */
object Test03 {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    val sparkSession = SparkSessionUtils.sparkSession

    import sqlContext._


    TimingUtils.time {

      val df00C = LoadTableParquet.loadTable(TabPaths.TAB00C)
      df00C.persist(nivel)
      df00C.createOrReplaceTempView("MC")

      df00C.show(10,truncate = false)

      import sparkSession.implicits._
      import sqlContext.implicits._


      df00C.map(FromStringToTimestamp,encoder = Encoders.TIMESTAMP ).show(20)






      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

  def p1 (fecha:Date):Timestamp =  {

    val calendar = Calendar.getInstance()

    calendar.setTime(fecha)

    new Timestamp(calendar.getTimeInMillis())


  }









}
