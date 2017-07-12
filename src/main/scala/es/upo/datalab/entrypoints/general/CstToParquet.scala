package es.upo.datalab.entrypoints.general


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.sql.Date
import java.util.Calendar

import es.upo.datalab.utilities.{LoadTableCsv, LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils, FunctionUtilities}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.storage.StorageLevel


/**
  * Created by davgutavi on 12/05/17.
  */
object CstToParquet {

  val sqlContext = SparkSessionUtils.sqlContext

  import sqlContext._

  import org.apache.spark.sql._


  def main(args: Array[String]): Unit = {

    TimingUtils.time {

      println("almacenando TAB00C")
      val df00C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB00C",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB00C_headers.csv")
      df00C.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB00C")
//      df00C.show(40)
      println("TAB00C almacenada")

      println("almacenando TAB00E")
      val df00E = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB00E",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB00E_headers.csv")
      df00E.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB00E")
//      df00E.show(40)
      println("TAB00E almacenada")

      println("almacenando TAB01")
      val df01 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB01",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB01_headers.csv")
      df01.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB01")
//      df01.show(40)
      println("TAB01 almacenada")

      println("almacenando TAB02")
      val df02 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB02",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB02_headers.csv")
      df02.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB02")
      df02.show(40)
      println("TAB02 almacenada")

      println("almacenando TAB06")
      val df06 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB06",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB06_headers.csv")
      df06.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB06")
//      df06.show(40)
      println("TAB06 almacenada")


      println("almacenando TAB08")
      val df08 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB08",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB08_headers.csv")
      df08.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB08")
//      df08.show(40)
      println("TAB08 almacenada")


      println("almacenando TAB15A")
      val df15A = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15A",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB15A_headers.csv")
      df15A.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB15C")
//      df15A.show(40)
      println("TAB15A almacenada")

      println("almacenando TAB15B")
      val df15B = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15B",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB15B_headers.csv")
      df15B.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB15B")
//      df15B.show(40)
      println("TAB15B almacenada")

      println("almacenando TAB15C")
      val df15C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15C",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB15C_headers.csv")
      df15C.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB15C")
//      df15C.show(40)
      println("TAB15C almacenada")


      println("almacenando TAB16")
      val df16 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB16",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB16_headers.csv")
      df16.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB16")
//      df16.show(40)
      println("TAB16 almacenada")


      println("almacenando TAB24")
      val df24 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB24",
        "/media/davgutavi/ushdportatil/entregas/headers/TAB24_headers.csv")
      df24.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB24")
//      df24.show(40, truncate = false)
      println("TAB24 almacenada")


      //      println("almacenando TAB05A")
      //      val df05A = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05A",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05A_headers.csv")
      //      //      df05A.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05A")
      //      df05A.show(40,truncate = false)
      //      println("TAB05A almacenada")
      //
      //      println("almacenando TAB05B")
      //      val df05B = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05B",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05B_headers.csv")
      //      //      df05B.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05B")
      //      df05B.show(40,truncate = false)
      //      println("TAB05B almacenada")
      //
      //      println("almacenando TAB05C")
      //      val df05C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05C",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05C_headers.csv")
      //      //      df05C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05C")
      //      df05C.show(40,truncate = false)
      //      println("TAB05C almacenada")
      //
      //      println("almacenando TAB05D")
      //      val df05D = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05D",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05D_headers.csv")
      //      //      df05D.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05D")
      //      df05D.show(40,truncate = false)
      //      println("TAB05D almacenada")


      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}
