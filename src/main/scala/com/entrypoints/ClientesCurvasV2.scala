package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.{SparkSessionUtils, TimingUtils}

/**
  * Created by davgutavi on 21/03/17.
  */
object ClientesCurvasV2 {

  def main( args:Array[String] ):Unit = {

    import SparkSessionUtils.sqlContext.sql


    TimingUtils.time {

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.cache()
      df_00C.registerTempTable("contratos")

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
      df_05C.cache()
      df_05C.registerTempTable("clientes")

      val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10, TabPaths.TAB_01_headers)
      df_01_10.cache()
      df_01_10.registerTempTable("cargas")

      val j1 = sql(
        """SELECT contratos.origen, contratos.cemptitu, contratos.ccontrat, contratos.cnumscct, contratos.cupsree2, contratos.cpuntmed, clientes.ccliente, clientes.dapersoc, clientes.dnombcli
         FROM contratos JOIN clientes
         ON contratos.origen = clientes.origen AND contratos.cemptitu = clientes.cemptitu AND contratos.ccontrat = clientes.ccontrat AND contratos.cnumscct = clientes.cnumscct
      """)

      j1.cache()
      j1.registerTempTable("con_cli")
      println("\nJoin contratos-clientes (" + j1.count() + " registros)\n")
      j1.show(5)

      val j2 = sql(
        """SELECT cargas.origen, cargas.cpuntmed, con_cli.dapersoc, con_cli.dnombcli, con_cli.ccliente,
         cargas.hora_01, cargas.1q_consumo_01, cargas.2q_consumo_01, cargas.3q_consumo_01, cargas.3q_consumo_01,
         cargas.hora_02, cargas.1q_consumo_02, cargas.2q_consumo_02, cargas.3q_consumo_02, cargas.3q_consumo_02
         FROM con_cli JOIN cargas
         ON con_cli.origen = cargas.origen AND con_cli.cpuntmed = cargas.cpuntmed
      """)

      j2.cache()
      println("\nJoin contratos-clientes-curvas (" + j2.count() + " registros)\n")
      j2.show(5)

    }

  }

}
