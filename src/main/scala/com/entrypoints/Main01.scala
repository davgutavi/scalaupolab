package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.SparkSessionUtils

/**
  * Created by davgutavi on 15/03/17.
  */
object Main01 {

  def main( args:Array[String] ):Unit = {

    //import sqlContext.implicits._
    //val df = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_01_10,TabPaths.TAB_01_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_02_14,TabPaths.TAB_02_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_03,TabPaths.TAB_03_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_04_10,TabPaths.TAB_04_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_10,TabPaths.TAB_10_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_12_10_12,TabPaths.TAB_12_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_15A,TabPaths.TAB_15A_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_15B,TabPaths.TAB_15B_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_16,TabPaths.TAB_16_headers)
    //val df = LoadTable.loadTable(TabPaths.TAB_18_ago_16,TabPaths.TAB_18_headers)

    val df_00C = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)

    val df_00E = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)

    val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10,TabPaths.TAB_01_headers)

    println("Join Contratos-Aparatos\n")

    val df_con_apa = df_00C.join(df_00E,Seq("origen","cupsree2","cpuntmed"))

    df_con_apa.cache()

    df_con_apa.show(10)

    df_con_apa.printSchema()

    println("Join Contratos-Aparatos-Curvas de Carga\n")

    val df_con_apa_cur = df_con_apa.join(df_01_10, Seq("origen","cpuntmed"))

    df_con_apa_cur.printSchema()

    df_con_apa_cur.show(10)

    SparkSessionUtils.sc.stop()

  }

}
