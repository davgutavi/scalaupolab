package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 23/03/17.
  */
object ExpedientesFraude {

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


      val df_16 = LoadTable.loadTable(TabPaths.TAB_16,TabPaths.TAB_16_headers)

      println("Persistiendo Expedientes\n")
      df_16.persist(nivel)
      println("Registrando Expedientes")
      df_16.registerTempTable("expedientes")

      println("Construyendo Contratos-Clientes\n")

      val j1 = sql(
        """SELECT contratos.origen, contratos.cfinca, contratos.cptoserv, contratos.cderind, contratos.fpsercon, contratos.ffinvesu, clientes.ccliente, clientes.dapersoc
            FROM contratos JOIN clientes
            ON contratos.origen = clientes.origen AND contratos.cemptitu = clientes.cemptitu AND contratos.ccontrat = clientes.ccontrat AND contratos.cnumscct = clientes.cnumscct
        """)

      println("Persistiendo Contratos-Clientes\n")
      j1.persist(nivel)
      println("Registrando Tabla Contratos-Clientes\n")
      j1.registerTempTable("con_cli")

      println("\nJoin Contratos-Clientes ("+j1.count()+" registros)\n")
      j1.show(5)

      println("Borrando Contratos, Clientes\n")
      df_00C.unpersist()
      df_05C.unpersist()


      println("Construyendo Contratos-Clientes-Expedientes\n")

      val j2 = sql(
        """SELECT con_cli.ccliente, con_cli.dapersoc, expedientes.irregularidad
            FROM con_cli JOIN expedientes
            ON con_cli.origen = expedientes.origen AND con_cli.cfinca = expedientes.cfinca AND con_cli.cptoserv = expedientes.cptoserv AND con_cli.cderind = expedientes.cderind
      """)


      println("Persistiendo Contratos-Clientes-Expedientes\n")
      j2.persist(nivel)

      println("Registrando Tabla Contratos-Clientes-Expedientes\n")
      j2.registerTempTable("con_cli_exp")

      println("Borrando Expedientes, Contratos-Clientes\n")
      df_16.unpersist()
      j1.unpersist()

      println("Construyendo Expedientes Fraudulentos\n")

      val j3 = sql(
        """SELECT ccliente, dapersoc, irregularidad
           FROM con_cli_exp
            WHERE irregularidad = 'S'
      """)

      j2.unpersist()

      println("\nExpedientes Fraudulentos ("+j3.count()+" registros)\n")
      j3.show(5)

    }



  }

}
