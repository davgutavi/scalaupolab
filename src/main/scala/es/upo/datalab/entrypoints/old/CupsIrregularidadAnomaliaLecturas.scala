package es.upo.datalab.entrypoints.old

import es.upo.datalab.utilities._
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 12/05/17.
  */
object CupsIrregularidadAnomaliaLecturas {


  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.DISK_ONLY

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    TimingUtils.time {

//      val df_00C = LoadTableParquet.loadTable(TabPaths.TAB_00C)
//      df_00C.persist(nivel)
//      df_00C.createOrReplaceTempView("MaestroContratos")
//
//      val df_16 = LoadTableParquet.loadTable(TabPaths.TAB_16)
//      df_16.persist(nivel)
//      df_16.createOrReplaceTempView("Expedientes")
//
//      val df_00E = LoadTableParquet.loadTable(TabPaths.TAB_00E)
//      df_00E.persist(nivel)
//      df_00E.createOrReplaceTempView("MaestroAparatos")

      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB_01)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CurvasCarga")


      //**************************************************************************MAESTRO CONTRATOS EXPEDIENTES**************************************************************************************************
//      SIN FECHAS
//      val maestroContratosExpedientes = sql(
//        """
//          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
//                 MaestroContratos.ccounips,MaestroContratos.cupsree2,MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
//                 MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,Expedientes.csecexpe, Expedientes.fapexpd,
//                 Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp,
//                 Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
//                 FROM MaestroContratos JOIN Expedientes
//                 ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cemptitu=Expedientes.cemptitu AND
//                     MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv
//        """
//      )


//            val maestroContratosExpedientes = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientes_sin_fechas)


//      LOGICAS
//      val maestroContratosExpedientes = sql(
//        """
//          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
//                 MaestroContratos.ccounips,MaestroContratos.cupsree2,MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
//                 MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,Expedientes.csecexpe, Expedientes.fapexpd,
//                 Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp,
//                 Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
//                 FROM MaestroContratos JOIN Expedientes
//                 ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND
//                    MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind AND
//                    Expedientes.fapexpd >= MaestroContratos.fpsercon AND Expedientes.fapexpd <= MaestroContratos.ffinvesu
//        """
//      )

//                  val maestroContratosExpedientes = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientes_fechas_endesa)
//


//      ENDESA
//            val maestroContratosExpedientes = sql(
//              """
//                SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
//                       MaestroContratos.ccounips,MaestroContratos.cupsree2,MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
//                       MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,Expedientes.csecexpe, Expedientes.fapexpd,
//                       Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp,
//                       Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
//                       FROM MaestroContratos JOIN Expedientes
//                       ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND
//                          MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind AND
//                          Expedientes.fapexpd <= MaestroContratos.fpsercon AND Expedientes.fapexpd <= MaestroContratos.ffinvesu
//              """
//            )
//
//            df_00C.unpersist()
//            df_16.unpersist()
//
//
////
//      maestroContratosExpedientes.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosExpedientes_sin_fechas")
//            println("maestroContratosExpedientes almacenado en parquet")
//      maestroContratosExpedientes.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosExpedientes_sin_fechas")
//            println("maestroContratosExpedientes almacenado en csv")

//      maestroContratosExpedientes.persist(nivel)
//      maestroContratosExpedientes.createOrReplaceTempView("mCe")
      //###############################################################################################MAESTRO CONTRATOS EXPEDIENTES###############################################################################################



      //**************************************************************************MAESTRO CONTRATOS EXPEDIENTES MAESTRO APARATOS**************************************************************************************************

//      val maestroContratosExpedientesMaestroAparatos = sql(
//        """SELECT mCe.origen, mCe.cptocred, mCe.cfinca, mCe.cptoserv, mCe.cderind, mCe.cupsree, mCe.ccounips, mCe.cupsree2,mCe.cpuntmed, mCe.tpuntmed, mCe.vparsist,
//                  mCe.cemptitu, mCe.ccontrat, mCe.cnumscct, mCe.fpsercon, mCe.ffinvesu, mCe.csecexpe, mCe.fapexpd, mCe.finifran, mCe.ffinfran, mCe.anomalia,
//                  mCe.irregularidad, mCe.venacord, mCe.vennofai, mCe.torigexp, mCe.texpedie, mCe.expclass, mCe.testexpe,
//                  mCe.fnormali, mCe.cplan, mCe.ccampa, mCe.cempresa, mCe.fciexped,
//                  MaestroAparatos.csecptom, MaestroAparatos.fvigorpm, MaestroAparatos.fbajapm,MaestroAparatos.caparmed
//                  FROM mCe JOIN MaestroAparatos
//                  ON mCe.origen = MaestroAparatos.origen AND mCe.cupsree2 = MaestroAparatos.cupsree2 AND mCe.cpuntmed = MaestroAparatos.cpuntmed
//      """)

//      df_00E.unpersist()
//      maestroContratosExpedientes.unpersist()

//      maestroContratosExpedientesMaestroAparatos.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosExpedientesMaestroAparatos_sin_fechas")
//                  println("maestroContratosExpedientesMaestroAparatos almacenado en parquet")
//      maestroContratosExpedientesMaestroAparatos.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosExpedientesMaestroAparatos_sin_fechas")
//                  println("maestroContratosExpedientesMaestroAparatos almacenado en csv")


      val maestroContratosExpedientesMaestroAparatos = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientesMaestroAparatos_fechas_endesa)

            maestroContratosExpedientesMaestroAparatos.persist(nivel)
            maestroContratosExpedientesMaestroAparatos.createOrReplaceTempView("mCeMa")

      //###############################################################################################MAESTRO CONTRATOS EXPEDIENTES MAESTRO APARATOS###############################################################################################






      //**************************************************************************MAESTRO CONTRATOS EXPEDIENTES MAESTRO APARATOS CURVAS DE CARGA**************************************************************************************************

//      val maestroContratosExpedientesMaestroAparatosCurvasCarga = sql(
//        """SELECT mCe.origen, mCe.cptocred, mCe.cfinca, mCe.cptoserv, mCe.cderind, mCe.cupsree, mCe.ccounips, mCe.cupsree2,mCe.cpuntmed, mCe.tpuntmed, mCe.vparsist,
//                  mCe.cemptitu, mCe.ccontrat, mCe.cnumscct, mCe.fpsercon, mCe.ffinvesu, mCe.csecexpe, mCe.fapexpd, mCe.finifran, mCe.ffinfran, mCe.anomalia,
//                  mCe.irregularidad, mCe.venacord, mCe.vennofai, mCe.torigexp, mCe.texpedie, mCe.expclass, mCe.testexpe,mCe.fnormali, mCe.cplan, mCe.ccampa,
//                  mCe.cempresa, mCe.fciexped,
//                  CurvasCarga.flectreg, CurvasCarga.testcaco, CurvasCarga.obiscode, CurvasCarga.vsecccar,
//                  CurvasCarga.hora_01, CurvasCarga.1q_consumo_01, CurvasCarga.2q_consumo_01, CurvasCarga.3q_consumo_01, CurvasCarga.4q_consumo_01,CurvasCarga.substatus_01,CurvasCarga.testmenn_01,CurvasCarga.testmecnn_01,
//                  CurvasCarga.hora_02, CurvasCarga.1q_consumo_02, CurvasCarga.2q_consumo_02, CurvasCarga.3q_consumo_02, CurvasCarga.4q_consumo_02,CurvasCarga.substatus_02,CurvasCarga.testmenn_02,CurvasCarga.testmecnn_02,
//                  CurvasCarga.hora_03, CurvasCarga.1q_consumo_03, CurvasCarga.2q_consumo_03, CurvasCarga.3q_consumo_03, CurvasCarga.4q_consumo_03,CurvasCarga.substatus_03,CurvasCarga.testmenn_03,CurvasCarga.testmecnn_03,
//                  CurvasCarga.hora_04, CurvasCarga.1q_consumo_04, CurvasCarga.2q_consumo_04, CurvasCarga.3q_consumo_04, CurvasCarga.4q_consumo_04,CurvasCarga.substatus_04,CurvasCarga.testmenn_04,CurvasCarga.testmecnn_04,
//                  CurvasCarga.hora_05, CurvasCarga.1q_consumo_05, CurvasCarga.2q_consumo_05, CurvasCarga.3q_consumo_05, CurvasCarga.4q_consumo_05,CurvasCarga.substatus_05,CurvasCarga.testmenn_05,CurvasCarga.testmecnn_05,
//                  CurvasCarga.hora_06, CurvasCarga.1q_consumo_06, CurvasCarga.2q_consumo_06, CurvasCarga.3q_consumo_06, CurvasCarga.4q_consumo_06,CurvasCarga.substatus_06,CurvasCarga.testmenn_06,CurvasCarga.testmecnn_06,
//                  CurvasCarga.hora_07, CurvasCarga.1q_consumo_07, CurvasCarga.2q_consumo_07, CurvasCarga.3q_consumo_07, CurvasCarga.4q_consumo_07,CurvasCarga.substatus_07,CurvasCarga.testmenn_07,CurvasCarga.testmecnn_07,
//                  CurvasCarga.hora_08, CurvasCarga.1q_consumo_08, CurvasCarga.2q_consumo_08, CurvasCarga.3q_consumo_08, CurvasCarga.4q_consumo_08,CurvasCarga.substatus_08,CurvasCarga.testmenn_08,CurvasCarga.testmecnn_08,
//                  CurvasCarga.hora_09, CurvasCarga.1q_consumo_09, CurvasCarga.2q_consumo_09, CurvasCarga.3q_consumo_09, CurvasCarga.4q_consumo_09,CurvasCarga.substatus_09,CurvasCarga.testmenn_09,CurvasCarga.testmecnn_09,
//                  CurvasCarga.hora_10, CurvasCarga.1q_consumo_10, CurvasCarga.2q_consumo_10, CurvasCarga.3q_consumo_10, CurvasCarga.4q_consumo_10,CurvasCarga.substatus_10,CurvasCarga.testmenn_10,CurvasCarga.testmecnn_10,
//                  CurvasCarga.hora_11, CurvasCarga.1q_consumo_11, CurvasCarga.2q_consumo_11, CurvasCarga.3q_consumo_11, CurvasCarga.4q_consumo_11,CurvasCarga.substatus_11,CurvasCarga.testmenn_11,CurvasCarga.testmecnn_11,
//                  CurvasCarga.hora_12, CurvasCarga.1q_consumo_12, CurvasCarga.2q_consumo_12, CurvasCarga.3q_consumo_12, CurvasCarga.4q_consumo_12,CurvasCarga.substatus_12,CurvasCarga.testmenn_12,CurvasCarga.testmecnn_12,
//                  CurvasCarga.hora_13, CurvasCarga.1q_consumo_13, CurvasCarga.2q_consumo_13, CurvasCarga.3q_consumo_13, CurvasCarga.4q_consumo_13,CurvasCarga.substatus_13,CurvasCarga.testmenn_13,CurvasCarga.testmecnn_13,
//                  CurvasCarga.hora_14, CurvasCarga.1q_consumo_14, CurvasCarga.2q_consumo_14, CurvasCarga.3q_consumo_14, CurvasCarga.4q_consumo_14,CurvasCarga.substatus_14,CurvasCarga.testmenn_14,CurvasCarga.testmecnn_14,
//                  CurvasCarga.hora_15, CurvasCarga.1q_consumo_15, CurvasCarga.2q_consumo_15, CurvasCarga.3q_consumo_15, CurvasCarga.4q_consumo_15, CurvasCarga.substatus_15, CurvasCarga.testmenn_15, CurvasCarga.testmecnn_15,
//                  CurvasCarga.hora_16, CurvasCarga.1q_consumo_16, CurvasCarga.2q_consumo_16, CurvasCarga.3q_consumo_16, CurvasCarga.4q_consumo_16, CurvasCarga.substatus_16, CurvasCarga.testmenn_16, CurvasCarga.testmecnn_16,
//                  CurvasCarga.hora_17, CurvasCarga.1q_consumo_17, CurvasCarga.2q_consumo_17, CurvasCarga.3q_consumo_17, CurvasCarga.4q_consumo_17, CurvasCarga.substatus_17, CurvasCarga.testmenn_17, CurvasCarga.testmecnn_17,
//                  CurvasCarga.hora_18, CurvasCarga.1q_consumo_18, CurvasCarga.2q_consumo_18, CurvasCarga.3q_consumo_18, CurvasCarga.4q_consumo_18, CurvasCarga.substatus_18, CurvasCarga.testmenn_18, CurvasCarga.testmecnn_18,
//                  CurvasCarga.hora_19, CurvasCarga.1q_consumo_19, CurvasCarga.2q_consumo_19, CurvasCarga.3q_consumo_19, CurvasCarga.4q_consumo_19, CurvasCarga.substatus_19, CurvasCarga.testmenn_19, CurvasCarga.testmecnn_19,
//                  CurvasCarga.hora_20, CurvasCarga.1q_consumo_20, CurvasCarga.2q_consumo_20, CurvasCarga.3q_consumo_20, CurvasCarga.4q_consumo_20, CurvasCarga.substatus_20, CurvasCarga.testmenn_20, CurvasCarga.testmecnn_20,
//                  CurvasCarga.hora_21, CurvasCarga.1q_consumo_21, CurvasCarga.2q_consumo_21, CurvasCarga.3q_consumo_21, CurvasCarga.4q_consumo_21, CurvasCarga.substatus_21, CurvasCarga.testmenn_21, CurvasCarga.testmecnn_21,
//                  CurvasCarga.hora_22, CurvasCarga.1q_consumo_22, CurvasCarga.2q_consumo_22, CurvasCarga.3q_consumo_22, CurvasCarga.4q_consumo_22, CurvasCarga.substatus_22, CurvasCarga.testmenn_22, CurvasCarga.testmecnn_22,
//                  CurvasCarga.hora_23, CurvasCarga.1q_consumo_23, CurvasCarga.2q_consumo_23, CurvasCarga.3q_consumo_23, CurvasCarga.4q_consumo_23, CurvasCarga.substatus_23, CurvasCarga.testmenn_23, CurvasCarga.testmecnn_23,
//                  CurvasCarga.hora_24, CurvasCarga.1q_consumo_24, CurvasCarga.2q_consumo_24, CurvasCarga.3q_consumo_24, CurvasCarga.4q_consumo_24, CurvasCarga.substatus_24, CurvasCarga.testmenn_24, CurvasCarga.testmecnn_24,
//                  CurvasCarga.hora_25, CurvasCarga.1q_consumo_25, CurvasCarga.2q_consumo_25, CurvasCarga.3q_consumo_25, CurvasCarga.4q_consumo_25, CurvasCarga.substatus_25, CurvasCarga.testmenn_25, CurvasCarga.testmecnn_25
//                  FROM mCe JOIN CurvasCarga
//                  ON mCe.origen = CurvasCarga.origen AND mCe.cpuntmed = CurvasCarga.cpuntmed AND CurvasCarga.obiscode = 'A' AND CurvasCarga.testcaco = 'R'
//                  """)





      val maestroContratosExpedientesMaestroAparatosCurvasCarga = sql(
        """SELECT mCeMa.origen, mCeMa.cptocred, mCeMa.cfinca, mCeMa.cptoserv, mCeMa.cderind, mCeMa.cupsree, mCeMa.ccounips, mCeMa.cupsree2,mCeMa.cpuntmed, mCeMa.tpuntmed, mCeMa.vparsist,
                  mCeMa.cemptitu, mCeMa.ccontrat, mCeMa.cnumscct, mCeMa.fpsercon, mCeMa.ffinvesu, mCeMa.csecexpe, mCeMa.fapexpd, mCeMa.finifran, mCeMa.ffinfran, mCeMa.anomalia,
                  mCeMa.irregularidad, mCeMa.venacord, mCeMa.vennofai, mCeMa.torigexp, mCeMa.texpedie, mCeMa.expclass, mCeMa.testexpe,mCeMa.fnormali, mCeMa.cplan, mCeMa.ccampa,
                  mCeMa.cempresa, mCeMa.fciexped,mCeMa.csecptom, mCeMa.fvigorpm, mCeMa.fbajapm,mCeMa.caparmed,
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
                  FROM mCeMa JOIN CurvasCarga
                  ON mCeMa.origen = CurvasCarga.origen AND mCeMa.cpuntmed = CurvasCarga.cpuntmed AND CurvasCarga.obiscode = 'A' AND CurvasCarga.testcaco = 'R'
                  """)

      maestroContratosExpedientesMaestroAparatos.checkpoint()
      maestroContratosExpedientesMaestroAparatos.persist()
      df_01.unpersist()



      //###############################################################################################MAESTRO CONTRATOS EXPEDIENTES MAESTRO APARATOS CURVAS DE CARGA###############################################################################################


//       val maestroContratosExpedientesMaestroAparatosCurvasCargaIrregularidad =
//         maestroContratosExpedientesMaestroAparatosCurvasCarga.where("irregularidad='S' AND (flectreg BETWEEN add_months(fapexpd,-6) AND fapexpd) AND (flectreg BETWEEN fpsercon AND ffinvesu)")
//           maestroContratosExpedientesMaestroAparatosCurvasCarga.where("irregularidad='S' AND flectreg BETWEEN add_months(fapexpd,-6) AND fapexpd")

      val maestroContratosExpedientesMaestroAparatosCurvasCargaAnomalia =
//         maestroContratosExpedientesMaestroAparatosCurvasCarga.where("anomalia='S'AND (flectreg BETWEEN add_months(fapexpd,-6) AND fapexpd) AND (flectreg BETWEEN fpsercon AND ffinvesu)")
           maestroContratosExpedientesMaestroAparatosCurvasCarga.where("anomalia='S'AND flectreg BETWEEN add_months(fapexpd,-6) AND fapexpd")

      maestroContratosExpedientesMaestroAparatosCurvasCarga.unpersist()

      maestroContratosExpedientesMaestroAparatosCurvasCargaAnomalia.checkpoint()
      maestroContratosExpedientesMaestroAparatosCurvasCargaAnomalia.persist()

      //**************************************************************************IRREGULARIDAD**************************************************************************************************

//      val datasetLecturasIrregularidad_aux = maestroContratosExpedientesMaestroAparatosCurvasCargaIrregularidad.select(
//        "cupsree","cpuntmed","ccontrat","cnumscct",
//        "fpsercon", "ffinvesu", "fapexpd", "fciexped",
//        "flectreg", "testcaco", "obiscode", "vsecccar",
//        "hora_01","1q_consumo_01", "2q_consumo_01", "3q_consumo_01","4q_consumo_01","substatus_01","testmenn_01","testmecnn_01",
//        "hora_02","1q_consumo_02", "2q_consumo_02", "3q_consumo_02","4q_consumo_02","substatus_02","testmenn_02","testmecnn_02",
//        "hora_03","1q_consumo_03", "2q_consumo_03", "3q_consumo_03","4q_consumo_03","substatus_03","testmenn_03","testmecnn_03",
//        "hora_04","1q_consumo_04", "2q_consumo_04", "3q_consumo_04","4q_consumo_04","substatus_04","testmenn_04","testmecnn_04",
//        "hora_05","1q_consumo_05", "2q_consumo_05", "3q_consumo_05","4q_consumo_05","substatus_05","testmenn_05","testmecnn_05",
//        "hora_06","1q_consumo_06", "2q_consumo_06", "3q_consumo_06","4q_consumo_06","substatus_06","testmenn_06","testmecnn_06",
//        "hora_07","1q_consumo_07", "2q_consumo_07", "3q_consumo_07","4q_consumo_07","substatus_07","testmenn_07","testmecnn_07",
//        "hora_08","1q_consumo_08", "2q_consumo_08", "3q_consumo_08","4q_consumo_08","substatus_08","testmenn_08","testmecnn_08",
//        "hora_09","1q_consumo_09", "2q_consumo_09", "3q_consumo_09","4q_consumo_09","substatus_09","testmenn_09","testmecnn_09",
//        "hora_10","1q_consumo_10", "2q_consumo_10", "3q_consumo_10","4q_consumo_10","substatus_10","testmenn_10","testmecnn_10",
//        "hora_11","1q_consumo_11", "2q_consumo_11", "3q_consumo_11","4q_consumo_11","substatus_11","testmenn_11","testmecnn_11",
//        "hora_12","1q_consumo_12", "2q_consumo_12", "3q_consumo_12","4q_consumo_12","substatus_12","testmenn_12","testmecnn_12",
//        "hora_13","1q_consumo_13", "2q_consumo_13", "3q_consumo_13","4q_consumo_13","substatus_13","testmenn_13","testmecnn_13",
//        "hora_14","1q_consumo_14", "2q_consumo_14", "3q_consumo_14","4q_consumo_14","substatus_14","testmenn_14","testmecnn_14",
//        "hora_15","1q_consumo_15", "2q_consumo_15", "3q_consumo_15","4q_consumo_15","substatus_15","testmenn_15","testmecnn_15",
//        "hora_16","1q_consumo_16", "2q_consumo_16", "3q_consumo_16","4q_consumo_16","substatus_16","testmenn_16","testmecnn_16",
//        "hora_17","1q_consumo_17", "2q_consumo_17", "3q_consumo_17","4q_consumo_17","substatus_17","testmenn_17","testmecnn_17",
//        "hora_18","1q_consumo_18", "2q_consumo_18", "3q_consumo_18","4q_consumo_18","substatus_18","testmenn_18","testmecnn_18",
//        "hora_19","1q_consumo_19", "2q_consumo_19", "3q_consumo_19","4q_consumo_19","substatus_19","testmenn_19","testmecnn_19",
//        "hora_20","1q_consumo_20", "2q_consumo_20", "3q_consumo_20","4q_consumo_20","substatus_20","testmenn_20","testmecnn_20",
//        "hora_21","1q_consumo_21", "2q_consumo_21", "3q_consumo_21","4q_consumo_21","substatus_21","testmenn_21","testmecnn_21",
//        "hora_22","1q_consumo_22", "2q_consumo_22", "3q_consumo_22","4q_consumo_22","substatus_22","testmenn_22","testmecnn_22",
//        "hora_23","1q_consumo_23", "2q_consumo_23", "3q_consumo_23","4q_consumo_23","substatus_23","testmenn_23","testmecnn_23",
//        "hora_24","1q_consumo_24", "2q_consumo_24", "3q_consumo_24","4q_consumo_24","substatus_24","testmenn_24","testmecnn_24",
//        "hora_25","1q_consumo_25", "2q_consumo_25", "3q_consumo_25","4q_consumo_25","substatus_25","testmenn_25","testmecnn_25"
//      )
//
//      maestroContratosExpedientesMaestroAparatosCurvasCargaIrregularidad.unpersist()
//
//      val datasetLecturasIrregularidad = datasetLecturasIrregularidad_aux.dropDuplicates()
//
//      val iaux = datasetLecturasIrregularidad_aux.count()
//      val i = datasetLecturasIrregularidad.count()
//      println("Dataset Lecturas Irregularidad con duplicados = "+iaux)
//      println("Dataset Lecturas Irregularidad  sin duplicados = "+i)
//      println("Diferencia = "+(iaux-i))
//
//
//      println("Guardando Irrregularidad")
//      datasetLecturasIrregularidad.coalesce(1).write.option("header","true").save(TabPaths.prefix_03+"lecturasIrregularidad_08")
//      println("Parquet Irregularidad Guardada")
//      datasetLecturasIrregularidad.coalesce(1).write.option("header","true").csv(TabPaths.prefix_04+"lecturasIrregularidad_08")
//      println("Csv Irregularidad Guardada")


      //**************************************************************************ANOMALIA**************************************************************************************************


      val datasetLecturasAnomalia_aux = maestroContratosExpedientesMaestroAparatosCurvasCargaAnomalia.select(
        "cupsree","cpuntmed","ccontrat","cnumscct",
        "fpsercon", "ffinvesu", "fapexpd", "fciexped",
        "flectreg", "testcaco", "obiscode", "vsecccar",
        "hora_01","1q_consumo_01", "2q_consumo_01", "3q_consumo_01","4q_consumo_01","substatus_01","testmenn_01","testmecnn_01",
        "hora_02","1q_consumo_02", "2q_consumo_02", "3q_consumo_02","4q_consumo_02","substatus_02","testmenn_02","testmecnn_02",
        "hora_03","1q_consumo_03", "2q_consumo_03", "3q_consumo_03","4q_consumo_03","substatus_03","testmenn_03","testmecnn_03",
        "hora_04","1q_consumo_04", "2q_consumo_04", "3q_consumo_04","4q_consumo_04","substatus_04","testmenn_04","testmecnn_04",
        "hora_05","1q_consumo_05", "2q_consumo_05", "3q_consumo_05","4q_consumo_05","substatus_05","testmenn_05","testmecnn_05",
        "hora_06","1q_consumo_06", "2q_consumo_06", "3q_consumo_06","4q_consumo_06","substatus_06","testmenn_06","testmecnn_06",
        "hora_07","1q_consumo_07", "2q_consumo_07", "3q_consumo_07","4q_consumo_07","substatus_07","testmenn_07","testmecnn_07",
        "hora_08","1q_consumo_08", "2q_consumo_08", "3q_consumo_08","4q_consumo_08","substatus_08","testmenn_08","testmecnn_08",
        "hora_09","1q_consumo_09", "2q_consumo_09", "3q_consumo_09","4q_consumo_09","substatus_09","testmenn_09","testmecnn_09",
        "hora_10","1q_consumo_10", "2q_consumo_10", "3q_consumo_10","4q_consumo_10","substatus_10","testmenn_10","testmecnn_10",
        "hora_11","1q_consumo_11", "2q_consumo_11", "3q_consumo_11","4q_consumo_11","substatus_11","testmenn_11","testmecnn_11",
        "hora_12","1q_consumo_12", "2q_consumo_12", "3q_consumo_12","4q_consumo_12","substatus_12","testmenn_12","testmecnn_12",
        "hora_13","1q_consumo_13", "2q_consumo_13", "3q_consumo_13","4q_consumo_13","substatus_13","testmenn_13","testmecnn_13",
        "hora_14","1q_consumo_14", "2q_consumo_14", "3q_consumo_14","4q_consumo_14","substatus_14","testmenn_14","testmecnn_14",
        "hora_15","1q_consumo_15", "2q_consumo_15", "3q_consumo_15","4q_consumo_15","substatus_15","testmenn_15","testmecnn_15",
        "hora_16","1q_consumo_16", "2q_consumo_16", "3q_consumo_16","4q_consumo_16","substatus_16","testmenn_16","testmecnn_16",
        "hora_17","1q_consumo_17", "2q_consumo_17", "3q_consumo_17","4q_consumo_17","substatus_17","testmenn_17","testmecnn_17",
        "hora_18","1q_consumo_18", "2q_consumo_18", "3q_consumo_18","4q_consumo_18","substatus_18","testmenn_18","testmecnn_18",
        "hora_19","1q_consumo_19", "2q_consumo_19", "3q_consumo_19","4q_consumo_19","substatus_19","testmenn_19","testmecnn_19",
        "hora_20","1q_consumo_20", "2q_consumo_20", "3q_consumo_20","4q_consumo_20","substatus_20","testmenn_20","testmecnn_20",
        "hora_21","1q_consumo_21", "2q_consumo_21", "3q_consumo_21","4q_consumo_21","substatus_21","testmenn_21","testmecnn_21",
        "hora_22","1q_consumo_22", "2q_consumo_22", "3q_consumo_22","4q_consumo_22","substatus_22","testmenn_22","testmecnn_22",
        "hora_23","1q_consumo_23", "2q_consumo_23", "3q_consumo_23","4q_consumo_23","substatus_23","testmenn_23","testmecnn_23",
        "hora_24","1q_consumo_24", "2q_consumo_24", "3q_consumo_24","4q_consumo_24","substatus_24","testmenn_24","testmecnn_24",
        "hora_25","1q_consumo_25", "2q_consumo_25", "3q_consumo_25","4q_consumo_25","substatus_25","testmenn_25","testmecnn_25")

      maestroContratosExpedientesMaestroAparatosCurvasCargaAnomalia.unpersist()



      val datasetLecturasAnomalia_aux_2 = datasetLecturasAnomalia_aux.dropDuplicates()

      val datasetLecturasAnomalia = datasetLecturasAnomalia_aux_2.checkpoint()

//      val datasetLecturasAnomalia = datasetLecturasAnomalia_aux.dropDuplicates()
//
//      datasetLecturasAnomalia.persist(nivel)

      val anaux = datasetLecturasAnomalia_aux.count()
      val an = datasetLecturasAnomalia.count()
      println("Dataset Lecturas Anomalia con duplicados = "+anaux)
      println("Dataset Lecturas Anomalia sin duplicados = "+an)
      println("Diferencia = "+(anaux-an))

      println("Guardando Anomalía")
      datasetLecturasAnomalia.coalesce(1).write.option("header","true").save(TabPaths.prefix_03+"lecturasAnomalia_08")
      println("Parquet Anomalía Guardada")
      datasetLecturasAnomalia.coalesce(1).write.option("header","true").csv(TabPaths.prefix_04+"lecturasAnomalia_08")
      println("Csv Anomalía guardada")


      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }


}
