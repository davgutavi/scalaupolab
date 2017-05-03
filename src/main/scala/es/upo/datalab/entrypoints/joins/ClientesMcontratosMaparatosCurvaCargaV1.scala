package es.upo.datalab.entrypoints.joins

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 15/03/17.
  */
object ClientesMcontratosMaparatosCurvaCargaV1 {


  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    TimingUtils.time {

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, true)
      df_05C.persist(nivel)
      df_05C.createOrReplaceTempView("Clientes")

      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers)
      df_00E.createOrReplaceTempView("MaestroAparatos")

      val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10, TabPaths.TAB_01_headers)
      df_01_10.persist(nivel)
      df_01_10.createOrReplaceTempView("CurvasCarga")


      val maestroContratosClientes = sql(
        """SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
           MaestroContratos.ccounips, MaestroContratos.cupsree2, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
           MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
           Clientes.ccliente, Clientes.fechamov, Clientes.tindfiju, Clientes.cnifdnic, Clientes.dapersoc, Clientes.dnombcli
           FROM MaestroContratos JOIN Clientes
           ON MaestroContratos.origen = Clientes.origen AND MaestroContratos.cemptitu = Clientes.cemptitu AND MaestroContratos.ccontrat = Clientes.ccontrat AND MaestroContratos.cnumscct = Clientes.cnumscct
        """)

      df_00C.unpersist()
      df_05C.unpersist()
      maestroContratosClientes.persist(nivel)
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")
      //maestroContratosClientes.show(5, false)


      val maestroContratosClientesMaestroAparatos = sql(
        """SELECT MaestroContratosClientes.origen, MaestroContratosClientes.cptocred, MaestroContratosClientes.cfinca, MaestroContratosClientes.cptoserv,
           MaestroContratosClientes.cderind, MaestroContratosClientes.cupsree, MaestroContratosClientes.ccounips, MaestroContratosClientes.cupsree2,
           MaestroContratosClientes.cpuntmed, MaestroContratosClientes.tpuntmed, MaestroContratosClientes.vparsist, MaestroContratosClientes.cemptitu,
          MaestroContratosClientes.ccontrat, MaestroContratosClientes.cnumscct, MaestroContratosClientes.fpsercon, MaestroContratosClientes.ffinvesu,
          MaestroContratosClientes.ccliente, MaestroContratosClientes.fechamov, MaestroContratosClientes.tindfiju, MaestroContratosClientes.cnifdnic,
          MaestroContratosClientes.dapersoc, MaestroContratosClientes.dnombcli,
          MaestroAparatos.csecptom, MaestroAparatos.fvigorpm, MaestroAparatos.fbajapm,MaestroAparatos.caparmed
          FROM MaestroContratosClientes JOIN MaestroAparatos
         ON MaestroContratosClientes.origen = MaestroAparatos.origen AND MaestroContratosClientes.cupsree2 = MaestroAparatos.cupsree2 AND MaestroContratosClientes.cpuntmed = MaestroAparatos.cpuntmed
      """)

      df_00E.unpersist()
      maestroContratosClientes.unpersist()

      maestroContratosClientesMaestroAparatos.persist(nivel)
      maestroContratosClientesMaestroAparatos.createOrReplaceTempView("MaestroContratosClientesMaestroAparatos")
      //maestroContratosClientesMaestroAparatos.show(5, false)

      println("Join MaestroContratosClientesMaestroAparatosCurvasCarga")

      val maestroContratosClientesMaestroAparatosCurvasCarga = sql(
        """SELECT MaestroContratosClientesMaestroAparatos.origen, MaestroContratosClientesMaestroAparatos.cptocred, MaestroContratosClientesMaestroAparatos.cfinca,
               MaestroContratosClientesMaestroAparatos.cptoserv, MaestroContratosClientesMaestroAparatos.cderind, MaestroContratosClientesMaestroAparatos.cupsree,
               MaestroContratosClientesMaestroAparatos.ccounips, MaestroContratosClientesMaestroAparatos.cupsree2,MaestroContratosClientesMaestroAparatos.cpuntmed,
               MaestroContratosClientesMaestroAparatos.tpuntmed, MaestroContratosClientesMaestroAparatos.vparsist, MaestroContratosClientesMaestroAparatos.cemptitu,
               MaestroContratosClientesMaestroAparatos.ccontrat, MaestroContratosClientesMaestroAparatos.cnumscct, MaestroContratosClientesMaestroAparatos.fpsercon,
               MaestroContratosClientesMaestroAparatos.ffinvesu, MaestroContratosClientesMaestroAparatos.ccliente, MaestroContratosClientesMaestroAparatos.fechamov,
               MaestroContratosClientesMaestroAparatos.tindfiju, MaestroContratosClientesMaestroAparatos.cnifdnic, MaestroContratosClientesMaestroAparatos.dapersoc,
               MaestroContratosClientesMaestroAparatos.dnombcli, MaestroContratosClientesMaestroAparatos.csecptom, MaestroContratosClientesMaestroAparatos.fvigorpm,
               MaestroContratosClientesMaestroAparatos.fbajapm,MaestroContratosClientesMaestroAparatos.caparmed,
              CurvasCarga.flectreg, CurvasCarga.testcaco, CurvasCarga.obiscode, CurvasCarga.vsecccar,
                CurvasCarga.hora_01, CurvasCarga.1q_consumo_01, CurvasCarga.2q_consumo_01, CurvasCarga.3q_consumo_01, CurvasCarga.4q_consumo_01,
                 CurvasCarga.hora_02, CurvasCarga.1q_consumo_02, CurvasCarga.2q_consumo_02, CurvasCarga.3q_consumo_02, CurvasCarga.4q_consumo_02,
                 CurvasCarga.hora_03, CurvasCarga.1q_consumo_03, CurvasCarga.2q_consumo_03, CurvasCarga.3q_consumo_03, CurvasCarga.4q_consumo_03,
                 CurvasCarga.hora_04, CurvasCarga.1q_consumo_04, CurvasCarga.2q_consumo_04, CurvasCarga.3q_consumo_04, CurvasCarga.4q_consumo_04,
                 CurvasCarga.hora_05, CurvasCarga.1q_consumo_05, CurvasCarga.2q_consumo_05, CurvasCarga.3q_consumo_05, CurvasCarga.4q_consumo_05,
                 CurvasCarga.hora_06, CurvasCarga.1q_consumo_06, CurvasCarga.2q_consumo_06, CurvasCarga.3q_consumo_06, CurvasCarga.4q_consumo_06,
                 CurvasCarga.hora_07, CurvasCarga.1q_consumo_07, CurvasCarga.2q_consumo_07, CurvasCarga.3q_consumo_07, CurvasCarga.4q_consumo_07,
                 CurvasCarga.hora_08, CurvasCarga.1q_consumo_08, CurvasCarga.2q_consumo_08, CurvasCarga.3q_consumo_08, CurvasCarga.4q_consumo_08,
                 CurvasCarga.hora_09, CurvasCarga.1q_consumo_09, CurvasCarga.2q_consumo_09, CurvasCarga.3q_consumo_09, CurvasCarga.4q_consumo_09,
                 CurvasCarga.hora_10, CurvasCarga.1q_consumo_10, CurvasCarga.2q_consumo_10, CurvasCarga.3q_consumo_10, CurvasCarga.4q_consumo_10,
                 CurvasCarga.hora_11, CurvasCarga.1q_consumo_11, CurvasCarga.2q_consumo_11, CurvasCarga.3q_consumo_11, CurvasCarga.4q_consumo_11,
                 CurvasCarga.hora_12, CurvasCarga.1q_consumo_12, CurvasCarga.2q_consumo_12, CurvasCarga.3q_consumo_12, CurvasCarga.4q_consumo_12,
                 CurvasCarga.hora_13, CurvasCarga.1q_consumo_13, CurvasCarga.2q_consumo_13, CurvasCarga.3q_consumo_13, CurvasCarga.4q_consumo_13,
                 CurvasCarga.hora_14, CurvasCarga.1q_consumo_14, CurvasCarga.2q_consumo_14, CurvasCarga.3q_consumo_14, CurvasCarga.4q_consumo_14,
                 CurvasCarga.hora_15, CurvasCarga.1q_consumo_15, CurvasCarga.2q_consumo_15, CurvasCarga.3q_consumo_15, CurvasCarga.4q_consumo_15,
                 CurvasCarga.hora_16, CurvasCarga.1q_consumo_16, CurvasCarga.2q_consumo_16, CurvasCarga.3q_consumo_16, CurvasCarga.4q_consumo_16,
                 CurvasCarga.hora_17, CurvasCarga.1q_consumo_17, CurvasCarga.2q_consumo_17, CurvasCarga.3q_consumo_17, CurvasCarga.4q_consumo_17,
                 CurvasCarga.hora_18, CurvasCarga.1q_consumo_18, CurvasCarga.2q_consumo_18, CurvasCarga.3q_consumo_18, CurvasCarga.4q_consumo_18,
                 CurvasCarga.hora_19, CurvasCarga.1q_consumo_19, CurvasCarga.2q_consumo_19, CurvasCarga.3q_consumo_19, CurvasCarga.4q_consumo_19,
                 CurvasCarga.hora_20, CurvasCarga.1q_consumo_20, CurvasCarga.2q_consumo_20, CurvasCarga.3q_consumo_20, CurvasCarga.4q_consumo_20,
                 CurvasCarga.hora_21, CurvasCarga.1q_consumo_21, CurvasCarga.2q_consumo_21, CurvasCarga.3q_consumo_21, CurvasCarga.4q_consumo_21,
                 CurvasCarga.hora_22, CurvasCarga.1q_consumo_22, CurvasCarga.2q_consumo_22, CurvasCarga.3q_consumo_22, CurvasCarga.4q_consumo_22,
                 CurvasCarga.hora_23, CurvasCarga.1q_consumo_23, CurvasCarga.2q_consumo_23, CurvasCarga.3q_consumo_23, CurvasCarga.4q_consumo_23,
                 CurvasCarga.hora_24, CurvasCarga.1q_consumo_24, CurvasCarga.2q_consumo_24, CurvasCarga.3q_consumo_24, CurvasCarga.4q_consumo_24,
                 CurvasCarga.hora_25, CurvasCarga.1q_consumo_25, CurvasCarga.2q_consumo_25, CurvasCarga.3q_consumo_25, CurvasCarga.4q_consumo_25
         FROM MaestroContratosClientesMaestroAparatos JOIN CurvasCarga
         ON MaestroContratosClientesMaestroAparatos.origen = CurvasCarga.origen AND MaestroContratosClientesMaestroAparatos.cpuntmed = CurvasCarga.cpuntmed
      """)

      maestroContratosClientesMaestroAparatosCurvasCarga.persist(nivel)
      maestroContratosClientesMaestroAparatosCurvasCarga.createOrReplaceTempView("MaestroContratosClientesMaestroAparatosCurvasCarga")

      val maestroContratosClientesMaestroAparatosCurvasCurvasCarga = maestroContratosClientesMaestroAparatosCurvasCarga.dropDuplicates()
      val mccma = maestroContratosClientesMaestroAparatosCurvasCarga.count()
      val mccmas = maestroContratosClientesMaestroAparatosCurvasCurvasCarga.count()


      println("\nMaestroContratosClientesMaestroAparatosCurvasCarga (" + mccma + " registros)\n")
      println("MaestroContratosClientesMaestroAparatosCurvasCarga (" + mccmas + " registros sin repeticion)\n")
      println("Diferencia = " + (mccma - mccmas))

      maestroContratosClientesMaestroAparatosCurvasCarga.show(10)

      maestroContratosClientesMaestroAparatosCurvasCurvasCarga.show(10)

    }

    SparkSessionUtils.sc.stop()

  }

}