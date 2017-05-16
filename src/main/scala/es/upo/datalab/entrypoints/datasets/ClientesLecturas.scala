package es.upo.datalab.entrypoints.datasets

import es.upo.datalab.utilities.TabPaths
import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 10/05/17.
  */
object ClientesLecturas {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    TimingUtils.time {

      val df_05C = LoadTableCsv.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, dropDuplicates = true)
      df_05C.persist(nivel)
      df_05C.createOrReplaceTempView("Clientes")

      val df_00C = LoadTableCsv.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

      val df_00E = LoadTableCsv.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers)
      df_00E.persist(nivel)
      df_00E.createOrReplaceTempView("MaestroAparatos")

      val df_16 = LoadTableCsv.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("Expedientes")

      val df_01_10 = LoadTableCsv.loadTable(TabPaths.TAB_01_10_csv, TabPaths.TAB_01_headers)
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

      println("Join MaestroContratosClientesMaestroAparatosCurvasCarga")

      //      val maestroContratosClientesMaestroAparatosCurvasCarga = sql(
      //        """SELECT MaestroContratosClientesMaestroAparatos.origen, MaestroContratosClientesMaestroAparatos.cptocred, MaestroContratosClientesMaestroAparatos.cfinca,
      //               MaestroContratosClientesMaestroAparatos.cptoserv, MaestroContratosClientesMaestroAparatos.cderind, MaestroContratosClientesMaestroAparatos.cupsree,
      //               MaestroContratosClientesMaestroAparatos.ccounips, MaestroContratosClientesMaestroAparatos.cupsree2,MaestroContratosClientesMaestroAparatos.cpuntmed,
      //               MaestroContratosClientesMaestroAparatos.tpuntmed, MaestroContratosClientesMaestroAparatos.vparsist, MaestroContratosClientesMaestroAparatos.cemptitu,
      //               MaestroContratosClientesMaestroAparatos.ccontrat, MaestroContratosClientesMaestroAparatos.cnumscct, MaestroContratosClientesMaestroAparatos.fpsercon,
      //               MaestroContratosClientesMaestroAparatos.ffinvesu, MaestroContratosClientesMaestroAparatos.ccliente, MaestroContratosClientesMaestroAparatos.fechamov,
      //               MaestroContratosClientesMaestroAparatos.tindfiju, MaestroContratosClientesMaestroAparatos.cnifdnic, MaestroContratosClientesMaestroAparatos.dapersoc,
      //               MaestroContratosClientesMaestroAparatos.dnombcli, MaestroContratosClientesMaestroAparatos.csecptom, MaestroContratosClientesMaestroAparatos.fvigorpm,
      //               MaestroContratosClientesMaestroAparatos.fbajapm,MaestroContratosClientesMaestroAparatos.caparmed,
      //               CurvasCarga.flectreg, CurvasCarga.testcaco, CurvasCarga.obiscode, CurvasCarga.vsecccar,
      //      CurvasCarga.hora_01, CurvasCarga.1q_consumo_01, CurvasCarga.2q_consumo_01, CurvasCarga.3q_consumo_01, CurvasCarga.4q_consumo_01,CurvasCarga.substatus_01,CurvasCarga.testmenn_01,CurvasCarga.testmecnn_01,
      //      CurvasCarga.hora_02, CurvasCarga.1q_consumo_02, CurvasCarga.2q_consumo_02, CurvasCarga.3q_consumo_02, CurvasCarga.4q_consumo_02,CurvasCarga.substatus_02,CurvasCarga.testmenn_02,CurvasCarga.testmecnn_02,
      //      CurvasCarga.hora_03, CurvasCarga.1q_consumo_03, CurvasCarga.2q_consumo_03, CurvasCarga.3q_consumo_03, CurvasCarga.4q_consumo_03,CurvasCarga.substatus_03,CurvasCarga.testmenn_03,CurvasCarga.testmecnn_03,
      //      CurvasCarga.hora_04, CurvasCarga.1q_consumo_04, CurvasCarga.2q_consumo_04, CurvasCarga.3q_consumo_04, CurvasCarga.4q_consumo_04,CurvasCarga.substatus_04,CurvasCarga.testmenn_04,CurvasCarga.testmecnn_04,
      //      CurvasCarga.hora_05, CurvasCarga.1q_consumo_05, CurvasCarga.2q_consumo_05, CurvasCarga.3q_consumo_05, CurvasCarga.4q_consumo_05,CurvasCarga.substatus_05,CurvasCarga.testmenn_05,CurvasCarga.testmecnn_05,
      //      CurvasCarga.hora_06, CurvasCarga.1q_consumo_06, CurvasCarga.2q_consumo_06, CurvasCarga.3q_consumo_06, CurvasCarga.4q_consumo_06,CurvasCarga.substatus_06,CurvasCarga.testmenn_06,CurvasCarga.testmecnn_06,
      //      CurvasCarga.hora_07, CurvasCarga.1q_consumo_07, CurvasCarga.2q_consumo_07, CurvasCarga.3q_consumo_07, CurvasCarga.4q_consumo_07,CurvasCarga.substatus_07,CurvasCarga.testmenn_07,CurvasCarga.testmecnn_07,
      //      CurvasCarga.hora_08, CurvasCarga.1q_consumo_08, CurvasCarga.2q_consumo_08, CurvasCarga.3q_consumo_08, CurvasCarga.4q_consumo_08,CurvasCarga.substatus_08,CurvasCarga.testmenn_08,CurvasCarga.testmecnn_08,
      //      CurvasCarga.hora_09, CurvasCarga.1q_consumo_09, CurvasCarga.2q_consumo_09, CurvasCarga.3q_consumo_09, CurvasCarga.4q_consumo_09,CurvasCarga.substatus_09,CurvasCarga.testmenn_09,CurvasCarga.testmecnn_09,
      //      CurvasCarga.hora_10, CurvasCarga.1q_consumo_10, CurvasCarga.2q_consumo_10, CurvasCarga.3q_consumo_10, CurvasCarga.4q_consumo_10,CurvasCarga.substatus_10,CurvasCarga.testmenn_10,CurvasCarga.testmecnn_10,
      //      CurvasCarga.hora_11, CurvasCarga.1q_consumo_11, CurvasCarga.2q_consumo_11, CurvasCarga.3q_consumo_11, CurvasCarga.4q_consumo_11,CurvasCarga.substatus_11,CurvasCarga.testmenn_11,CurvasCarga.testmecnn_11,
      //      CurvasCarga.hora_12, CurvasCarga.1q_consumo_12, CurvasCarga.2q_consumo_12, CurvasCarga.3q_consumo_12, CurvasCarga.4q_consumo_12,CurvasCarga.substatus_12,CurvasCarga.testmenn_12,CurvasCarga.testmecnn_12,
      //      CurvasCarga.hora_13, CurvasCarga.1q_consumo_13, CurvasCarga.2q_consumo_13, CurvasCarga.3q_consumo_13, CurvasCarga.4q_consumo_13,CurvasCarga.substatus_13,CurvasCarga.testmenn_13,CurvasCarga.testmecnn_13,
      //      CurvasCarga.hora_14, CurvasCarga.1q_consumo_14, CurvasCarga.2q_consumo_14, CurvasCarga.3q_consumo_14, CurvasCarga.4q_consumo_14,CurvasCarga.substatus_14,CurvasCarga.testmenn_14,CurvasCarga.testmecnn_14,
      //      CurvasCarga.hora_15, CurvasCarga.1q_consumo_15, CurvasCarga.2q_consumo_15, CurvasCarga.3q_consumo_15, CurvasCarga.4q_consumo_15, CurvasCarga.substatus_15, CurvasCarga.testmenn_15, CurvasCarga.testmecnn_15,
      //      CurvasCarga.hora_16, CurvasCarga.1q_consumo_16, CurvasCarga.2q_consumo_16, CurvasCarga.3q_consumo_16, CurvasCarga.4q_consumo_16, CurvasCarga.substatus_16, CurvasCarga.testmenn_16, CurvasCarga.testmecnn_16
      //      CurvasCarga.hora_17, CurvasCarga.1q_consumo_17, CurvasCarga.2q_consumo_17, CurvasCarga.3q_consumo_17, CurvasCarga.4q_consumo_17, CurvasCarga.substatus_17, CurvasCarga.testmenn_17, CurvasCarga.testmecnn_17
      //      CurvasCarga.hora_18, CurvasCarga.1q_consumo_18, CurvasCarga.2q_consumo_18, CurvasCarga.3q_consumo_18, CurvasCarga.4q_consumo_18, CurvasCarga.substatus_18, CurvasCarga.testmenn_18, CurvasCarga.testmecnn_18
      //      CurvasCarga.hora_19, CurvasCarga.1q_consumo_19, CurvasCarga.2q_consumo_19, CurvasCarga.3q_consumo_19, CurvasCarga.4q_consumo_19, CurvasCarga.substatus_19, CurvasCarga.testmenn_19, CurvasCarga.testmecnn_19
      //      CurvasCarga.hora_20, CurvasCarga.1q_consumo_20, CurvasCarga.2q_consumo_20, CurvasCarga.3q_consumo_20, CurvasCarga.4q_consumo_20, CurvasCarga.substatus_20, CurvasCarga.testmenn_20, CurvasCarga.testmecnn_20
      //      CurvasCarga.hora_21, CurvasCarga.1q_consumo_21, CurvasCarga.2q_consumo_21, CurvasCarga.3q_consumo_21, CurvasCarga.4q_consumo_21, CurvasCarga.substatus_21, CurvasCarga.testmenn_21, CurvasCarga.testmecnn_21
      //      CurvasCarga.hora_22, CurvasCarga.1q_consumo_22, CurvasCarga.2q_consumo_22, CurvasCarga.3q_consumo_22, CurvasCarga.4q_consumo_22, CurvasCarga.substatus_22, CurvasCarga.testmenn_22, CurvasCarga.testmecnn_22
      //      CurvasCarga.hora_23, CurvasCarga.1q_consumo_23, CurvasCarga.2q_consumo_23, CurvasCarga.3q_consumo_23, CurvasCarga.4q_consumo_23, CurvasCarga.substatus_23, CurvasCarga.testmenn_23, CurvasCarga.testmecnn_23
      //      CurvasCarga.hora_24, CurvasCarga.1q_consumo_24, CurvasCarga.2q_consumo_24, CurvasCarga.3q_consumo_24, CurvasCarga.4q_consumo_24, CurvasCarga.substatus_24, CurvasCarga.testmenn_24, CurvasCarga.testmecnn_24
      //      CurvasCarga.hora_25, CurvasCarga.1q_consumo_25, CurvasCarga.2q_consumo_25, CurvasCarga.3q_consumo_25, CurvasCarga.4q_consumo_25, CurvasCarga.substatus_25, CurvasCarga.testmenn_25, CurvasCarga.testmecnn_25

      //         FROM MaestroContratosClientesMaestroAparatos JOIN CurvasCarga
      //         ON MaestroContratosClientesMaestroAparatos.origen = CurvasCarga.origen AND MaestroContratosClientesMaestroAparatos.cpuntmed = CurvasCarga.cpuntmed
      //      """)

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
               CurvasCarga.hora_01, CurvasCarga.1q_consumo_01, CurvasCarga.2q_consumo_01, CurvasCarga.3q_consumo_01, CurvasCarga.4q_consumo_01,CurvasCarga.substatus_01,CurvasCarga.testmenn_01,CurvasCarga.testmecnn_01,
               CurvasCarga.hora_02, CurvasCarga.1q_consumo_02, CurvasCarga.2q_consumo_02, CurvasCarga.3q_consumo_02, CurvasCarga.4q_consumo_02,CurvasCarga.substatus_02,CurvasCarga.testmenn_02,CurvasCarga.testmecnn_02,
               CurvasCarga.hora_03, CurvasCarga.1q_consumo_03, CurvasCarga.2q_consumo_03, CurvasCarga.3q_consumo_03, CurvasCarga.4q_consumo_03,CurvasCarga.substatus_03,CurvasCarga.testmenn_03,CurvasCarga.testmecnn_03,
               CurvasCarga.hora_04, CurvasCarga.1q_consumo_04, CurvasCarga.2q_consumo_04, CurvasCarga.3q_consumo_04, CurvasCarga.4q_consumo_04,CurvasCarga.substatus_04,CurvasCarga.testmenn_04,CurvasCarga.testmecnn_04,
               CurvasCarga.hora_05, CurvasCarga.1q_consumo_05, CurvasCarga.2q_consumo_05, CurvasCarga.3q_consumo_05, CurvasCarga.4q_consumo_05,CurvasCarga.substatus_05,CurvasCarga.testmenn_05,CurvasCarga.testmecnn_05
               FROM MaestroContratosClientesMaestroAparatos JOIN CurvasCarga
               ON MaestroContratosClientesMaestroAparatos.origen = CurvasCarga.origen AND MaestroContratosClientesMaestroAparatos.cpuntmed = CurvasCarga.cpuntmed
      """)

      df_01_10.unpersist()

      maestroContratosClientesMaestroAparatos.unpersist()

      maestroContratosClientesMaestroAparatosCurvasCarga.persist(nivel)
      maestroContratosClientesMaestroAparatosCurvasCarga.createOrReplaceTempView("MCClMACC")


      val maestroContratosClientesMaestroAparatosCurvasCargaExpediente = sql(
        """SELECT  MCClMACC.origen,   MCClMACC.cptocred, MCClMACC.cfinca, MCClMACC.cptoserv, MCClMACC.cderind, MCClMACC.cupsree, MCClMACC.ccounips, MCClMACC.cupsree2,MCClMACC.cpuntmed,
                   MCClMACC.tpuntmed, MCClMACC.vparsist, MCClMACC.cemptitu,MCClMACC.ccontrat, MCClMACC.cnumscct, MCClMACC.fpsercon,MCClMACC.ffinvesu, MCClMACC.ccliente, MCClMACC.fechamov,
                   MCClMACC.tindfiju, MCClMACC.cnifdnic, MCClMACC.dapersoc,MCClMACC.dnombcli, MCClMACC.csecptom, MCClMACC.fvigorpm,MCClMACC.fbajapm,MCClMACC.caparmed, MCClMACC.flectreg,
                   MCClMACC.testcaco, MCClMACC.obiscode, MCClMACC.vsecccar,
                   MCClMACC.hora_01, MCClMACC.1q_consumo_01, MCClMACC.2q_consumo_01, MCClMACC.3q_consumo_01, MCClMACC.4q_consumo_01,MCClMACC.substatus_01,MCClMACC.testmenn_01,testmecnn_01,
                   MCClMACC.hora_02, MCClMACC.1q_consumo_02, MCClMACC.2q_consumo_02, MCClMACC.3q_consumo_02, MCClMACC.4q_consumo_02,MCClMACC.substatus_02,MCClMACC.testmenn_02,MCClMACC.testmecnn_02,
                   MCClMACC.hora_03, MCClMACC.1q_consumo_03, MCClMACC.2q_consumo_03, MCClMACC.3q_consumo_03, MCClMACC.4q_consumo_03,MCClMACC.substatus_03,MCClMACC.testmenn_03,MCClMACC.testmecnn_03,
                   MCClMACC.hora_04, MCClMACC.1q_consumo_04, MCClMACC.2q_consumo_04, MCClMACC.3q_consumo_04, MCClMACC.4q_consumo_04,MCClMACC.substatus_04,MCClMACC.testmenn_04,MCClMACC.testmecnn_04,
                   MCClMACC.hora_05, MCClMACC.1q_consumo_05, MCClMACC.2q_consumo_05, MCClMACC.3q_consumo_05, MCClMACC.4q_consumo_05,MCClMACC.substatus_05,MCClMACC.testmenn_05,MCClMACC.testmecnn_05,
                   Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
                   Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,
                   Expedientes.fciexped
                   FROM MCClMACC JOIN Expedientes
                   ON MCClMACC.origen=Expedientes.origen AND MCClMACC.cemptitu=Expedientes.cemptitu AND MCClMACC.cfinca=Expedientes.cfinca AND MCClMACC.cptoserv=Expedientes.cptoserv
                   """)

      maestroContratosClientesMaestroAparatosCurvasCarga.unpersist()

      maestroContratosClientesMaestroAparatosCurvasCargaExpediente.persist(nivel)


      val maestroContratosClientesMaestroAparatosCurvasCargaIrregularidad = maestroContratosClientesMaestroAparatosCurvasCargaExpediente.where("irregularidad='S'")
      maestroContratosClientesMaestroAparatosCurvasCargaIrregularidad.persist(nivel)

      val maestroContratosClientesMaestroAparatosCurvasCargaAnomalia = maestroContratosClientesMaestroAparatosCurvasCargaExpediente.where("anomalia='S'")
      maestroContratosClientesMaestroAparatosCurvasCargaAnomalia.persist(nivel)

//      val maestroContratosClientesMaestroAparatosCurvasCargaNormal = (maestroContratosClientesMaestroAparatosCurvasCargaExpediente.except(maestroContratosClientesMaestroAparatosCurvasCargaIrregularidad)).except(maestroContratosClientesMaestroAparatosCurvasCargaAnomalia)
//      maestroContratosClientesMaestroAparatosCurvasCargaNormal.persist(nivel)

      maestroContratosClientesMaestroAparatosCurvasCargaExpediente.unpersist()


      val datasetCurvasIrregularidad = maestroContratosClientesMaestroAparatosCurvasCargaIrregularidad.select(
        "cnifdnic","cfinca","ccliente","cupsree","ccontrat", "cnumscct", "cptoserv", "csecexpe", "finifran", "ffinfran", "fapexpd", "fciexped",
        "flectreg", "testcaco", "obiscode", "vsecccar",
        "hora_01","1q_consumo_01", "2q_consumo_01", "3q_consumo_01","4q_consumo_01","substatus_01","testmenn_01","testmecnn_01",
        "hora_02","1q_consumo_02", "2q_consumo_02", "3q_consumo_02","4q_consumo_02","substatus_02","testmenn_02","testmecnn_02",
        "hora_03","1q_consumo_03", "2q_consumo_03", "3q_consumo_03","4q_consumo_03","substatus_03","testmenn_03","testmecnn_03",
        "hora_04","1q_consumo_04", "2q_consumo_04", "3q_consumo_04","4q_consumo_04","substatus_04","testmenn_04","testmecnn_04",
        "hora_05","1q_consumo_05", "2q_consumo_05", "3q_consumo_05","4q_consumo_05","substatus_05","testmenn_05","testmecnn_05")

      maestroContratosClientesMaestroAparatosCurvasCargaIrregularidad.unpersist()
//      datasetCurvasIrregularidad.show(10,false)

      println("datasetCurvasIrregularidad = "+datasetCurvasIrregularidad.count()+" registros")

      val datasetCurvasAnomalia = maestroContratosClientesMaestroAparatosCurvasCargaAnomalia.select(
        "cnifdnic","cfinca","ccliente","cupsree","ccontrat", "cnumscct", "cptoserv", "csecexpe", "finifran", "ffinfran", "fapexpd", "fciexped",
        "flectreg", "testcaco", "obiscode", "vsecccar",
        "hora_01","1q_consumo_01", "2q_consumo_01", "3q_consumo_01","4q_consumo_01","substatus_01","testmenn_01","testmecnn_01",
        "hora_02","1q_consumo_02", "2q_consumo_02", "3q_consumo_02","4q_consumo_02","substatus_02","testmenn_02","testmecnn_02",
        "hora_03","1q_consumo_03", "2q_consumo_03", "3q_consumo_03","4q_consumo_03","substatus_03","testmenn_03","testmecnn_03",
        "hora_04","1q_consumo_04", "2q_consumo_04", "3q_consumo_04","4q_consumo_04","substatus_04","testmenn_04","testmecnn_04",
        "hora_05","1q_consumo_05", "2q_consumo_05", "3q_consumo_05","4q_consumo_05","substatus_05","testmenn_05","testmecnn_05")

      maestroContratosClientesMaestroAparatosCurvasCargaAnomalia.unpersist()
//      datasetCurvasAnomalia.show(10,false)

      println("datasetCurvasAnomalia = "+datasetCurvasAnomalia.count()+" registros")


      val datasetCurvasTablaNormal = sql(
        """SELECT  MCClMACC.origen,   MCClMACC.cptocred, MCClMACC.cfinca, MCClMACC.cptoserv, MCClMACC.cderind, MCClMACC.cupsree, MCClMACC.ccounips, MCClMACC.cupsree2,MCClMACC.cpuntmed,
                   MCClMACC.tpuntmed, MCClMACC.vparsist, MCClMACC.cemptitu,MCClMACC.ccontrat, MCClMACC.cnumscct, MCClMACC.fpsercon,MCClMACC.ffinvesu, MCClMACC.ccliente, MCClMACC.fechamov,
                   MCClMACC.tindfiju, MCClMACC.cnifdnic, MCClMACC.dapersoc,MCClMACC.dnombcli, MCClMACC.csecptom, MCClMACC.fvigorpm,MCClMACC.fbajapm,MCClMACC.caparmed, MCClMACC.flectreg,
                   MCClMACC.testcaco, MCClMACC.obiscode, MCClMACC.vsecccar,
                   MCClMACC.hora_01, MCClMACC.1q_consumo_01, MCClMACC.2q_consumo_01, MCClMACC.3q_consumo_01, MCClMACC.4q_consumo_01,MCClMACC.substatus_01,MCClMACC.testmenn_01,testmecnn_01,
                   MCClMACC.hora_02, MCClMACC.1q_consumo_02, MCClMACC.2q_consumo_02, MCClMACC.3q_consumo_02, MCClMACC.4q_consumo_02,MCClMACC.substatus_02,MCClMACC.testmenn_02,MCClMACC.testmecnn_02,
                   MCClMACC.hora_03, MCClMACC.1q_consumo_03, MCClMACC.2q_consumo_03, MCClMACC.3q_consumo_03, MCClMACC.4q_consumo_03,MCClMACC.substatus_03,MCClMACC.testmenn_03,MCClMACC.testmecnn_03,
                   MCClMACC.hora_04, MCClMACC.1q_consumo_04, MCClMACC.2q_consumo_04, MCClMACC.3q_consumo_04, MCClMACC.4q_consumo_04,MCClMACC.substatus_04,MCClMACC.testmenn_04,MCClMACC.testmecnn_04,
                   MCClMACC.hora_05, MCClMACC.1q_consumo_05, MCClMACC.2q_consumo_05, MCClMACC.3q_consumo_05, MCClMACC.4q_consumo_05,MCClMACC.substatus_05,MCClMACC.testmenn_05,MCClMACC.testmecnn_05,
                   Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
                   Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali,
                   Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,Expedientes.fciexped
                   FROM MCClMACC LEFT JOIN Expedientes
                   ON MCClMACC.origen=Expedientes.origen AND MCClMACC.cemptitu=Expedientes.cemptitu AND MCClMACC.cfinca=Expedientes.cfinca AND MCClMACC.cptoserv=Expedientes.cptoserv
                   WHERE Expedientes.origen IS NULL AND Expedientes.cemptitu IS NULL AND Expedientes.cfinca IS NULL AND Expedientes.cptoserv IS NULL
                   """)

      df_16.unpersist()

            val datasetCurvasNormal = datasetCurvasTablaNormal.select(
              "cnifdnic","cfinca","ccliente","cupsree","ccontrat", "cnumscct", "cptoserv", "csecexpe", "finifran", "ffinfran", "fapexpd", "fciexped",
              "flectreg", "testcaco", "obiscode", "vsecccar",
              "hora_01","1q_consumo_01", "2q_consumo_01", "3q_consumo_01","4q_consumo_01","substatus_01","testmenn_01","testmecnn_01",
              "hora_02","1q_consumo_02", "2q_consumo_02", "3q_consumo_02","4q_consumo_02","substatus_02","testmenn_02","testmecnn_02",
              "hora_03","1q_consumo_03", "2q_consumo_03", "3q_consumo_03","4q_consumo_03","substatus_03","testmenn_03","testmecnn_03",
              "hora_04","1q_consumo_04", "2q_consumo_04", "3q_consumo_04","4q_consumo_04","substatus_04","testmenn_04","testmecnn_04",
              "hora_05","1q_consumo_05", "2q_consumo_05", "3q_consumo_05","4q_consumo_05","substatus_05","testmenn_05","testmecnn_05")

          datasetCurvasTablaNormal.unpersist()

      println("datasetCurvasNormal = "+datasetCurvasNormal.count()+" registros")


      datasetCurvasNormal.show(10,truncate = false)


//      datasetCurvasIrregularidad.coalesce(1).write.option("header","true").csv(TabPaths.prefix+"datasets/CurvasIrregularidad")
//      datasetCurvasAnomalia.coalesce(1).write.option("header","true").csv(TabPaths.prefix+"datasets/CurvasAnomalia")
//      datasetCurvasNormal.coalesce(1).write.option("header","true").csv(TabPaths.prefix+"datasets/CurvasNormal")


      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }



}


























