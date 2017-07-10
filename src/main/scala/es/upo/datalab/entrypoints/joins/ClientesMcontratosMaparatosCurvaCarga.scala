package es.upo.datalab.entrypoints.joins


import es.upo.datalab.utilities._
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 15/03/17.
  */
object ClientesMcontratosMaparatosCurvaCarga {


  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    TimingUtils.time {


      val df_00C = LoadTableParquet.loadTable(TabPaths.TAB00C)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

      val df_05C = LoadTableParquet.loadTable(TabPaths.TAB05C)
      df_05C.persist(nivel)
      df_05C.createOrReplaceTempView("Clientes")

      val df_00E =LoadTableParquet.loadTable(TabPaths.TAB00E)
      df_00E.persist(nivel)
      df_00E.createOrReplaceTempView("MaestroAparatos")

      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB01)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CurvasCarga")


      val maestroContratosClientes = sql(
       """ SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips, MaestroContratos.cupsree2,
      MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
      Clientes.ccliente, Clientes.fechamov, Clientes.tindfiju, Clientes.cnifdnic, Clientes.dapersoc, Clientes.dnombcli
      FROM MaestroContratos JOIN Clientes
        ON MaestroContratos.origen = Clientes.origen AND MaestroContratos.cemptitu = Clientes.cemptitu AND MaestroContratos.ccontrat = Clientes.ccontrat AND MaestroContratos.cnumscct = Clientes.cnumscct
      """)
      df_00C.unpersist()
      df_05C.unpersist()
      maestroContratosClientes.persist()
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")
//
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
////
//      maestroContratosClientesMaestroAparatos.show(20,truncate = false)
//      maestroContratosClientesMaestroAparatos.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosClientesMaestroAparatos")
//      println("MaestroContratosClientesMaestroAparatos guardado en parquet")
//      maestroContratosClientesMaestroAparatos.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosClientesMaestroAparatos")
//      println("MaestroContratosClientesMaestroAparatos guardado en csv")
//      df_00E.unpersist()
//      maestroContratosClientes.unpersist()

//      val maestroContratosClientesMaestroAparatos = LoadTableParquet.loadTable(TabPaths.MaestroContratosClientesMaestroAparatos)

      maestroContratosClientesMaestroAparatos.persist(nivel)
      maestroContratosClientesMaestroAparatos.createOrReplaceTempView("MaestroContratosClientesMaestroAparatos")
      println ("Registros de MaestroContratosClientesMaestroAparatos = "+maestroContratosClientesMaestroAparatos.count())

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
      CurvasCarga.hora_01, CurvasCarga.1q_consumo_01, CurvasCarga.2q_consumo_01, CurvasCarga.3q_consumo_01, CurvasCarga.4q_consumo_01,CurvasCarga.substatus_01,CurvasCarga.testmenn_01,CurvasCarga.testmecnn_01,
      CurvasCarga.hora_02, CurvasCarga.1q_consumo_02, CurvasCarga.2q_consumo_02, CurvasCarga.3q_consumo_02, CurvasCarga.4q_consumo_02,CurvasCarga.substatus_02,CurvasCarga.testmenn_02,CurvasCarga.testmecnn_02,
      CurvasCarga.hora_03, CurvasCarga.1q_consumo_03, CurvasCarga.2q_consumo_03, CurvasCarga.3q_consumo_03, CurvasCarga.4q_consumo_03,CurvasCarga.substatus_03,CurvasCarga.testmenn_03,CurvasCarga.testmecnn_03,
      CurvasCarga.hora_04, CurvasCarga.1q_consumo_04, CurvasCarga.2q_consumo_04, CurvasCarga.3q_consumo_04, CurvasCarga.4q_consumo_04,CurvasCarga.substatus_04,CurvasCarga.testmenn_04,CurvasCarga.testmecnn_04,
      CurvasCarga.hora_05, CurvasCarga.1q_consumo_05, CurvasCarga.2q_consumo_05, CurvasCarga.3q_consumo_05, CurvasCarga.4q_consumo_05,CurvasCarga.substatus_05,CurvasCarga.testmenn_05,CurvasCarga.testmecnn_05,
      CurvasCarga.hora_06, CurvasCarga.1q_consumo_06, CurvasCarga.2q_consumo_06, CurvasCarga.3q_consumo_06, CurvasCarga.4q_consumo_06,CurvasCarga.substatus_06,CurvasCarga.testmenn_06,CurvasCarga.testmecnn_06,
      CurvasCarga.hora_07, CurvasCarga.1q_consumo_07, CurvasCarga.2q_consumo_07, CurvasCarga.3q_consumo_07, CurvasCarga.4q_consumo_07,CurvasCarga.substatus_07,CurvasCarga.testmenn_07,CurvasCarga.testmecnn_07,
      CurvasCarga.hora_08, CurvasCarga.1q_consumo_08, CurvasCarga.2q_consumo_08, CurvasCarga.3q_consumo_08, CurvasCarga.4q_consumo_08,CurvasCarga.substatus_08,CurvasCarga.testmenn_08,CurvasCarga.testmecnn_08,
      CurvasCarga.hora_09, CurvasCarga.1q_consumo_09, CurvasCarga.2q_consumo_09, CurvasCarga.3q_consumo_09, CurvasCarga.4q_consumo_09,CurvasCarga.substatus_09,CurvasCarga.testmenn_09,CurvasCarga.testmecnn_09,
      CurvasCarga.hora_10, CurvasCarga.1q_consumo_10, CurvasCarga.2q_consumo_10, CurvasCarga.3q_consumo_10, CurvasCarga.4q_consumo_10,CurvasCarga.substatus_10,CurvasCarga.testmenn_10,CurvasCarga.testmecnn_10,
      CurvasCarga.hora_11, CurvasCarga.1q_consumo_11, CurvasCarga.2q_consumo_11, CurvasCarga.3q_consumo_11, CurvasCarga.4q_consumo_11,CurvasCarga.substatus_11,CurvasCarga.testmenn_11,CurvasCarga.testmecnn_11,
      CurvasCarga.hora_12, CurvasCarga.1q_consumo_12, CurvasCarga.2q_consumo_12, CurvasCarga.3q_consumo_12, CurvasCarga.4q_consumo_12,CurvasCarga.substatus_12,CurvasCarga.testmenn_12,CurvasCarga.testmecnn_12,
      CurvasCarga.hora_13, CurvasCarga.1q_consumo_13, CurvasCarga.2q_consumo_13, CurvasCarga.3q_consumo_13, CurvasCarga.4q_consumo_13,CurvasCarga.substatus_13,CurvasCarga.testmenn_13,CurvasCarga.testmecnn_13,
      CurvasCarga.hora_14, CurvasCarga.1q_consumo_14, CurvasCarga.2q_consumo_14, CurvasCarga.3q_consumo_14, CurvasCarga.4q_consumo_14,CurvasCarga.substatus_14,CurvasCarga.testmenn_14,CurvasCarga.testmecnn_14,
      CurvasCarga.hora_15, CurvasCarga.1q_consumo_15, CurvasCarga.2q_consumo_15, CurvasCarga.3q_consumo_15, CurvasCarga.4q_consumo_15, CurvasCarga.substatus_15, CurvasCarga.testmenn_15, CurvasCarga.testmecnn_15,
      CurvasCarga.hora_16, CurvasCarga.1q_consumo_16, CurvasCarga.2q_consumo_16, CurvasCarga.3q_consumo_16, CurvasCarga.4q_consumo_16, CurvasCarga.substatus_16, CurvasCarga.testmenn_16, CurvasCarga.testmecnn_16,
      CurvasCarga.hora_17, CurvasCarga.1q_consumo_17, CurvasCarga.2q_consumo_17, CurvasCarga.3q_consumo_17, CurvasCarga.4q_consumo_17, CurvasCarga.substatus_17, CurvasCarga.testmenn_17, CurvasCarga.testmecnn_17,
      CurvasCarga.hora_18, CurvasCarga.1q_consumo_18, CurvasCarga.2q_consumo_18, CurvasCarga.3q_consumo_18, CurvasCarga.4q_consumo_18, CurvasCarga.substatus_18, CurvasCarga.testmenn_18, CurvasCarga.testmecnn_18,
      CurvasCarga.hora_19, CurvasCarga.1q_consumo_19, CurvasCarga.2q_consumo_19, CurvasCarga.3q_consumo_19, CurvasCarga.4q_consumo_19, CurvasCarga.substatus_19, CurvasCarga.testmenn_19, CurvasCarga.testmecnn_19,
      CurvasCarga.hora_20, CurvasCarga.1q_consumo_20, CurvasCarga.2q_consumo_20, CurvasCarga.3q_consumo_20, CurvasCarga.4q_consumo_20, CurvasCarga.substatus_20, CurvasCarga.testmenn_20, CurvasCarga.testmecnn_20,
      CurvasCarga.hora_21, CurvasCarga.1q_consumo_21, CurvasCarga.2q_consumo_21, CurvasCarga.3q_consumo_21, CurvasCarga.4q_consumo_21, CurvasCarga.substatus_21, CurvasCarga.testmenn_21, CurvasCarga.testmecnn_21,
      CurvasCarga.hora_22, CurvasCarga.1q_consumo_22, CurvasCarga.2q_consumo_22, CurvasCarga.3q_consumo_22, CurvasCarga.4q_consumo_22, CurvasCarga.substatus_22, CurvasCarga.testmenn_22, CurvasCarga.testmecnn_22,
      CurvasCarga.hora_23, CurvasCarga.1q_consumo_23, CurvasCarga.2q_consumo_23, CurvasCarga.3q_consumo_23, CurvasCarga.4q_consumo_23, CurvasCarga.substatus_23, CurvasCarga.testmenn_23, CurvasCarga.testmecnn_23,
      CurvasCarga.hora_24, CurvasCarga.1q_consumo_24, CurvasCarga.2q_consumo_24, CurvasCarga.3q_consumo_24, CurvasCarga.4q_consumo_24, CurvasCarga.substatus_24, CurvasCarga.testmenn_24, CurvasCarga.testmecnn_24,
      CurvasCarga.hora_25, CurvasCarga.1q_consumo_25, CurvasCarga.2q_consumo_25, CurvasCarga.3q_consumo_25, CurvasCarga.4q_consumo_25, CurvasCarga.substatus_25, CurvasCarga.testmenn_25, CurvasCarga.testmecnn_25
      FROM MaestroContratosClientesMaestroAparatos JOIN CurvasCarga
      ON MaestroContratosClientesMaestroAparatos.origen = CurvasCarga.origen AND MaestroContratosClientesMaestroAparatos.cpuntmed = CurvasCarga.cpuntmed
      """)

      maestroContratosClientesMaestroAparatos.unpersist()

      maestroContratosClientesMaestroAparatosCurvasCarga.persist(nivel)

      println("Escritura MaestroContratosClientesMaestroAparatosCurvasCarga")

//      maestroContratosClientesMaestroAparatosCurvasCarga.coalesce(1).write.option("header","true").save(TabPaths._05+"MaestroContratosClientesMaestroAparatosCurvasCarga_10")
//      println("MaestroContratosClientesMaestroAparatos guardado en parquet")
//      maestroContratosClientesMaestroAparatosCurvasCarga.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosClientesMaestroAparatosCurvasCarga_10")
//      println("MaestroContratosClientesMaestroAparatos guardado en csv")

      println("\nMaestroContratosClientesMaestroAparatosCurvasCarga_10 (" + maestroContratosClientesMaestroAparatosCurvasCarga.count() + " registros)\n")

      maestroContratosClientesMaestroAparatosCurvasCarga.show(5,truncate = false)


    }

    SparkSessionUtils.sc.stop()

  }

}