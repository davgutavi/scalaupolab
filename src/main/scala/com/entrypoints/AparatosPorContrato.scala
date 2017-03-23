package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 23/03/17.
  */
object AparatosPorContrato {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    import SparkSessionUtils.sqlContext.sql

    TimingUtils.time{

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)

      println("Persistiendo Contratos\n")
      df_00C.persist(nivel)
      println("Registrando Contratos")
      df_00C.registerTempTable("contratos")

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C,TabPaths.TAB_05C_headers)

      println("Persistiendo Clientes\n")
      df_05C.persist(nivel)
      println("Registrando Clientes")
      df_05C.registerTempTable("clientes")

      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)

      println("Persistiendo Aparatos\n")
      df_00E.persist(nivel)
      println("Registrando Aparatos")
      df_00E.registerTempTable("aparatos")

      println("Construyendo J1\n")

      val j1 = sql(
        """SELECT contratos.origen, contratos.cemptitu, contratos.ccontrat, contratos.cnumscct, contratos.cupsree2, contratos.cpuntmed, clientes.ccliente, clientes.dapersoc, clientes.dnombcli
            FROM contratos JOIN clientes
            ON contratos.origen = clientes.origen AND contratos.cemptitu = clientes.cemptitu AND contratos.ccontrat = clientes.ccontrat AND contratos.cnumscct = clientes.cnumscct
        """)

      println("Persistiendo J1\n")
      j1.persist(nivel)
      println("Registrando Tabla J1\n")
      j1.registerTempTable("con_cli")

      println("\nJoin contratos-clientes ("+j1.count()+" registros)\n")
      j1.show(5)

      println("Borrando Contratos, Clientes\n")
      df_00C.unpersist()
      df_05C.unpersist()

      println("Construyendo J2\n")

      val j2 = sql(
        """SELECT aparatos.origen, aparatos.cpuntmed, aparatos.cupsree, aparatos.cupsree2, con_cli.ccliente, con_cli.dapersoc, con_cli.dnombcli
         FROM con_cli JOIN aparatos
         ON con_cli.origen = aparatos.origen AND con_cli.cupsree2 = aparatos.cupsree2 AND con_cli.cpuntmed = aparatos.cpuntmed
      """)

      println("Persistiendo J2\n")
      j2.persist(nivel)

      println("Registrando Tabla J2\n")
      j2.registerTempTable("con_cli_apa")

      println("\nJoin contratos-clientes-aparatos ("+j2.count()+" registros)\n")
      j2.show(5)

      println("Borrando Aparatos, J1\n")
      df_00E.unpersist()
      j1.unpersist()


      val j3 = sql(
        """
          SELECT origen, cupsree2, cpuntmed, ccliente, count(cupsree) AS SumCupsree
          FROM con_cli_apa
          GROUP BY origen, cupsree2, cpuntmed, ccliente
        """
      )
      println("Persistiendo J3\n")
      j3.persist(nivel)

      println("Registrando Tabla J3\n")
      j3.registerTempTable("cli_apa")

      println("Borrando J2\n")
      j2.unpersist()

      println("\nContador Aparatos por clientes ("+j3.count()+" registros)\n")
      j3.show(5)


      val j4 = sql ("""
          SELECT ccliente, SumCupsree
          FROM cli_apa
          WHERE SumCupsree > 1
        """
      )

      println("\nContador Aparatos > 1 por clientes ("+j4.count()+" registros)\n")
      j4.show(5)

    }

  }

}
