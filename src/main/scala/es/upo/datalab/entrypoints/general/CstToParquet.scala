package es.upo.datalab.entrypoints.general

import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 12/05/17.
  */
object CstToParquet {

  final val HDFSPATH = "hdfs://192.168.47.247/user/gutierrez/endesa/database_parquet/"

  def main(args: Array[String]): Unit = {

    TimingUtils.time {

//      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
//      df_00C.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_00C")
//      println("TAB_01")

//      val df_03 = LoadTable.loadTable(TabPaths.TAB_03, TabPaths.TAB_03_headers)
//      df_03.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_03")
//      println("TAB_03")

//      val df_04 = LoadTable.loadTable(TabPaths.TAB_04, TabPaths.TAB_04_headers)
//      df_03.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_04")
//      println("TAB_04")

//      val df_05B = LoadTable.loadTable(TabPaths.TAB_05B, TabPaths.TAB_05B_headers)
//      df_05B.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_05B")
//      println("TAB_05B")

//      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers,dropDuplicates = true)
//      df_05C.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_05C")
//      println("TAB_05C")

//      val df_05D = LoadTable.loadTable(TabPaths.TAB_05D, TabPaths.TAB_05D_headers)
//      df_05D.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_05D")
//      println("TAB_05D")

//      val df_15A = LoadTable.loadTable(TabPaths.TAB_15A, TabPaths.TAB_15A_headers)
//      df_15A.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_15A")
//      println("TAB_15A")

//      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
//      df_16.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_16")
//      println("TAB_16")

//      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers)
//      df_00E.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_00E")
//      println("TAB_00E")


//      val df_01 = LoadTableCsv.loadTable(TabPaths.TAB_01, TabPaths.TAB_01_headers)
//      df_01.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01")
//      println("TAB_01")

//      val df_01_10 = LoadTableCsv.loadTable(TabPaths.TAB_01_10_csv, TabPaths.TAB_01_headers)
//      df_01_10.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01_10")
//      println("TAB_01_10")
//
//      val df_01_11 = LoadTableCsv.loadTable(TabPaths.TAB_01_11_csv, TabPaths.TAB_01_headers)
//      df_01_11.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01_11")
//      println("TAB_01_11")
//
//      val df_01_12 = LoadTableCsv.loadTable(TabPaths.TAB_01_12_csv, TabPaths.TAB_01_headers)
//      df_01_12.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01_12")
//      println("TAB_01_12")
//
//      val df_01_13 = LoadTableCsv.loadTable(TabPaths.TAB_01_13_csv, TabPaths.TAB_01_headers)
//      df_01_13.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01_13")
//      println("TAB_01_13")
//
//      val df_01_14 = LoadTableCsv.loadTable(TabPaths.TAB_01_14_csv, TabPaths.TAB_01_headers)
//      df_01_14.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01_14")
//      println("TAB_01_14")
//
//      val df_01_15 = LoadTableCsv.loadTable(TabPaths.TAB_01_15_csv, TabPaths.TAB_01_headers)
//      df_01_15.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01_15")
//      println("TAB_01_15")
//
//      val df_01_16 = LoadTableCsv.loadTable(TabPaths.TAB_01_16_csv, TabPaths.TAB_01_headers)
//      df_01_16.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_01_16")
//      println("TAB_01_16")

//      val df_02 = LoadTable.loadTable(TabPaths.TAB_02, TabPaths.TAB_02_headers, dropDuplicates = true)
//      df_02.coalesce(1).write.option("header","true").save(HDFSPATH+"TAB_02")
//      println("TAB_02")

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}
