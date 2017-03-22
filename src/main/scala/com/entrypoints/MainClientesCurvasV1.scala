package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.{SparkSessionUtils, TimingUtils}

/**
  * Created by davgutavi on 15/03/17.
  */
object MainClientesCurvasV1 {

  def main( args:Array[String] ):Unit = {

    import SparkSessionUtils.sqlContext.sql


    TimingUtils.time{

    val df_00C = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)
    df_00C.cache()
    df_00C.registerTempTable("contratos")

    val df_05C = LoadTable.loadTable(TabPaths.TAB_05C,TabPaths.TAB_05C_headers)
    df_05C.cache()
    df_05C.registerTempTable("clientes")

    val df_00E = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)
    df_00E.cache()
    df_00E.registerTempTable("aparatos")

    val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10,TabPaths.TAB_01_headers)
    df_01_10.cache()
    df_01_10.registerTempTable("cargas")


    val j1 = sql(
      """SELECT contratos.origen, contratos.cemptitu, contratos.ccontrat, contratos.cnumscct, contratos.cupsree2, contratos.cpuntmed, clientes.ccliente, clientes.dapersoc, clientes.dnombcli
         FROM contratos JOIN clientes
         ON contratos.origen = clientes.origen AND contratos.cemptitu = clientes.cemptitu AND contratos.ccontrat = clientes.ccontrat AND contratos.cnumscct = clientes.cnumscct
      """)

    j1.cache()
    j1.registerTempTable("con_cli")
    println("\nJoin contratos-clientes ("+j1.count()+" registros)\n")
    j1.show(5)


    val j2 = sql(
      """SELECT aparatos.origen, aparatos.cpuntmed, con_cli.ccliente, con_cli.dapersoc, con_cli.dnombcli  FROM con_cli JOIN aparatos
         ON con_cli.origen = aparatos.origen AND con_cli.cupsree2 = aparatos.cupsree2 AND con_cli.cpuntmed = aparatos.cpuntmed
      """)

    j2.cache()
    j2.registerTempTable("con_cli_apa")
    println("\nJoin contratos-clientes-aparatos ("+j2.count()+" registros)\n")
    j2.show(5)


    val j3 = sql(
      """SELECT cargas.origen, cargas.cpuntmed, con_cli_apa.ccliente, con_cli_apa.dapersoc, con_cli_apa.dnombcli,
         cargas.hora_01, cargas.1q_consumo_01, cargas.2q_consumo_01, cargas.3q_consumo_01, cargas.3q_consumo_01  FROM con_cli_apa JOIN cargas
         ON con_cli_apa.origen = cargas.origen AND con_cli_apa.cpuntmed = cargas.cpuntmed
      """)

    j3.cache()
    println("\nJoin contratos-clientes-aparatos-curvas ("+j3.count()+" registros)\n")
    j3.show(5)

    }

    SparkSessionUtils.sc.stop()

  }

}
