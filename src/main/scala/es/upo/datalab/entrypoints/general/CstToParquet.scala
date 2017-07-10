package es.upo.datalab.entrypoints.general


import java.text.SimpleDateFormat

import es.upo.datalab.utilities.{LoadTableCsv, LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.storage.StorageLevel


/**
  * Created by davgutavi on 12/05/17.
  */
object CstToParquet {

  val sqlContext = SparkSessionUtils.sqlContext

  import sqlContext._

  def main(args: Array[String]): Unit = {

    TimingUtils.time {

      println("almacenando TAB00C")
      val df00C = LoadTableCsv.loadTable("/media/davgutavi/Modelo Datos ENDESA/Entregas/Absolutas (2010-2016)/descomprimidas/TAB00C/", "/media/davgutavi/Modelo Datos ENDESA/Entregas/headers/TAB00C_headers.csv")
//      df00C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
      df00C.show(40)
      println("TAB00C almacenada")


//      val loader = SparkSessionUtils.sparkSession.read
//        .option("delimiter", ";")
//        .option("ignoreLeadingWhiteSpace", "true")
//        .option("ignoreTrailingWhiteSpace", "true")
//        .option("inferSchema","true")
////                .option("charset","ASCII")
//        //      .option("mode", "DROPMALFORMED")
//        //      .option("nullValue","Null")
//
////      val df00C = loader.csv(TabPaths.TAB00C_csv)
//
//      val df00C = loader.csv("/media/davgutavi/Modelo Datos ENDESA/Entregas/Absolutas (2010-2016)/descomprimidas/TAB00C/")
//
//      df00C.show(20)
//
//      println("almacenando TAB00E")
//      val df00E = LoadTableCsv.loadTable(TabPaths.TAB00E_csv, TabPaths.TAB00E_headers)
//      df00E.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00E")
//      println("TAB00E almacenada")
//
//      println("almacenando TAB01")
//      val df01 = LoadTableCsv.loadTable(TabPaths.TAB01_csv, TabPaths.TAB01_headers)
//      df01.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB01")
//      println("TAB01 almacenada")
//
//      println("almacenando TAB02")
//      val df02 = LoadTableCsv.loadTable(TabPaths.TAB02_csv, TabPaths.TAB02_headers)
//      df02.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB02")
//      println("TAB02 almacenada")
//
////      println("almacenando TAB05A")
////      val df05A = LoadTableCsv.loadTable(TabPaths.TAB05A_csv, TabPaths.TAB05A_headers)
////      df05A.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05A")
////      println("TAB05A almacenada")
////
////      println("almacenando TAB05B")
////      val df05B = LoadTableCsv.loadTable(TabPaths.TAB05B_csv, TabPaths.TAB05B_headers)
////      df05B.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05B")
////      println("TAB05B almacenada")
////
////      println("almacenando TAB05C")
////      val df05C = LoadTableCsv.loadTable(TabPaths.TAB05C_csv, TabPaths.TAB05C_headers)
////      df05C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05C")
////      println("TAB05C almacenada")
////
////      println("almacenando TAB05D")
////      val df05D = LoadTableCsv.loadTable(TabPaths.TAB05D_csv, TabPaths.TAB05D_headers)
////      df05D.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05D")
////      println("TAB05D almacenada")
//
//      println("almacenando TAB06")
//      val df06 = LoadTableCsv.loadTable(TabPaths.TAB06_csv, TabPaths.TAB06_headers)
//      df06.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB06")
//      println("TAB06 almacenada")

//      println("almacenando TAB08:"+TabPaths.TAB08_csv)
//      val df08 = LoadTableCsv.loadTable(TabPaths.TAB08_csv, TabPaths.TAB08_headers)
//
//      df08.show(40)
//      df08.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB08")
//      println("TAB08 almacenada")

//      println("almacenando TAB15A")
//      val df15A = LoadTableCsv.loadTable(TabPaths.TAB15A_csv, TabPaths.TAB15A_headers)
//      df15A.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB15A")
//      println("TAB15A almacenada")
//
//      println("almacenando TAB15B")
//      val df15B = LoadTableCsv.loadTable(TabPaths.TAB15B_csv, TabPaths.TAB15B_headers)
//      df15B.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05B")
//      println("TAB15B almacenada")
//
//      println("almacenando TAB15C")
//      val df15C = LoadTableCsv.loadTable(TabPaths.TAB15C_csv, TabPaths.TAB15C_headers)
//      df15C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB15C")
//      println("TAB15C almacenada")
//
//      println("almacenando TAB16")
//      val df16 = LoadTableCsv.loadTable(TabPaths.TAB16_csv, TabPaths.TAB16_headers)
//      df16.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB16")
//      println("TAB16 almacenada")
//
//      println("almacenando TAB24")
//      val df24 = LoadTableCsv.loadTable(TabPaths.TAB24_csv, TabPaths.TAB24_headers)
//
//      df24.show(10,truncate=false)
//
//      val fl = df24.head().getString(1)
//
//      val  f = new SimpleDateFormat("EEE MMM dd HH:mm:ss z YYYY")
//
//      println("Apply = "+f.applyPattern(fl))
//
//
//      //df24.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB24")
//      println("TAB24 almacenada")

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}
