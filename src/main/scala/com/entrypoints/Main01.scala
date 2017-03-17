package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.SparkSessionUtils

/**
  * Created by davgutavi on 15/03/17.
  */
object Main01 {

  def main( args:Array[String] ):Unit = {


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

    import SparkSessionUtils.sqlContext.implicits._
    import SparkSessionUtils.sqlContext.sql

    val df_00C = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)
    df_00C.cache()

    val df_05C = LoadTable.loadTable(TabPaths.TAB_05C,TabPaths.TAB_05C_headers)
    df_05C.cache()

    val df_00E = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)
    df_00E.cache()

    val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10,TabPaths.TAB_01_headers)
    df_01_10.cache()

    df_00C.registerTempTable("contratos")
    df_05C.registerTempTable("clientes")
    df_00E.registerTempTable("aparatos")
    df_01_10.registerTempTable("cargas")

    val j1 = sql(
      """SELECT contratos.origen, contratos.cupsree2, contratos.cpuntmed, clientes.ccliente, clientes.dapersoc, clientes.dnombcli
         FROM contratos JOIN clientes
         ON contratos.origen = clientes.origen AND contratos.cemptitu = clientes.cemptitu AND contratos.ccontrat = clientes.ccontrat AND contratos.cnumscct = clientes.cnumscct
      """)

    j1.cache()
    println("Join contratos clientes\n")
    j1.show(5)
    j1.registerTempTable("con_cli")

    val j2 = sql(
      """SELECT aparatos.origen, aparatos.cpuntmed, con_cli.ccliente, con_cli.dapersoc, con_cli.dnombcli  FROM con_cli JOIN aparatos
         ON con_cli.origen = aparatos.origen AND con_cli.cupsree2 = aparatos.cupsree2 AND con_cli.cpuntmed = aparatos.cpuntmed
      """)

    j2.cache()
    println("Join contratos clientes aparatos\n")
    j2.show(5)
    j2.registerTempTable("con_cli_apa")

    val j3 = sql(
      """SELECT cargas.origen, cargas.cpuntmed, con_cli_apa.ccliente, con_cli_apa.dapersoc, con_cli_apa.dnombcli,
         cargas.hora_01, cargas.1q_consumo_01, cargas.2q_consumo_01, cargas.3q_consumo_01, cargas.3q_consumo_01  FROM con_cli_apa JOIN cargas
         ON con_cli_apa.origen = cargas.origen AND con_cli_apa.cpuntmed = cargas.cpuntmed
      """)

    j3.cache()
    println("Join contratos clientes aparatos curvas\n")
    j3.show(5)

//    println("Join Contratos-Aparatos\n")
//
//    val df_con_apa = df_00C.join(df_00E,Seq("origen","cupsree2","cpuntmed"))
//
//    df_con_apa.cache()
//
//    df_con_apa.show(10)
//
//    df_con_apa.printSchema()
//
//    println("Join Contratos-Aparatos-Curvas de Carga\n")
//
//    val df_con_apa_cur = df_con_apa.join(df_01_10, Seq("origen","cpuntmed"))
//
//    df_con_apa_cur.printSchema()
//
//    df_con_apa_cur.show(10)

    SparkSessionUtils.sc.stop()

  }

}
