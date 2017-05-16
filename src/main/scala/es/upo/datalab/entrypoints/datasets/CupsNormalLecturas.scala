package es.upo.datalab.entrypoints.datasets

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 16/05/17.
  */
object CupsNormalLecturas {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    TimingUtils.time {

      val df_00C = LoadTableParquet.loadTable(TabPaths.TAB_00C)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

      val df_16 = LoadTableParquet.loadTable(TabPaths.TAB_16)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("Expedientes")

      val df_00E = LoadTableParquet.loadTable(TabPaths.TAB_00E)
      df_00E.persist(nivel)
      df_00E.createOrReplaceTempView("MaestroAparatos")

      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB_01)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CurvasCarga")


      val maestroContratosMaestroAparatos = sql(
        """
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
                 MaestroContratos.ccounips,MaestroContratos.cupsree2,MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
                 MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu, MaestroAparatos.csecptom, MaestroAparatos.fvigorpm,
                 MaestroAparatos.fbajapm,MaestroAparatos.caparmed
                 FROM MaestroContratos JOIN MaestroAparatos
                 ON MaestroContratos.origen = MaestroAparatos.origen AND MaestroContratos.cupsree2 = MaestroAparatos.cupsree2 AND MaestroContratos.cpuntmed = MaestroAparatos.cpuntmed
        """
      )

      df_00C.unpersist()
      df_00E.unpersist()

      maestroContratosMaestroAparatos.persist(nivel)
      maestroContratosMaestroAparatos.createOrReplaceTempView("mCmA")
      println ("Join Maestro Contratos - Maestro Aparatos = "+maestroContratosMaestroAparatos.count()+" registros")
      maestroContratosMaestroAparatos.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosMaestroAparatos")
      maestroContratosMaestroAparatos.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosMaestroAparatos")
      println("Join Maestro Contratos - Maestro Aparatos almacenado")


      val maestroContratosMaestroAparatosCurvasCarga = sql(
        """SELECT mCmA.origen, mCmA.cptocred, mCmA.cfinca, mCmA.cptoserv, mCmA.cderind, mCmA.cupsree, mCmA.ccounips,mCmA.cupsree2,mCmA.cpuntmed, mCmA.tpuntmed, mCmA.vparsist, mCmA.cemptitu,
                  mCmA.ccontrat, mCmA.cnumscct, mCmA.fpsercon, mCmA.ffinvesu, mCmA.csecptom, mCmA.fvigorpm, mCmA.fbajapm,mCmA.caparmed
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
                  FROM mCmA JOIN CurvasCarga
                  ON mCmA.origen = CurvasCarga.origen AND mCmA.cpuntmed = CurvasCarga.cpuntmed
                   """)

      df_01.unpersist()
      maestroContratosMaestroAparatos.unpersist()

      maestroContratosMaestroAparatosCurvasCarga.persist(nivel)
      maestroContratosMaestroAparatosCurvasCarga.createOrReplaceTempView("mCmAcC")
      println ("Join Maestro Contratos - Maestro Aparatos - Curvas de Carga = "+maestroContratosMaestroAparatosCurvasCarga.count()+" registros")
      maestroContratosMaestroAparatosCurvasCarga.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosMaestroAparatosCurvasCarga")
      maestroContratosMaestroAparatosCurvasCarga.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosMaestroAparatosCurvasCarga")
      println("Join Maestro Contratos - Maestro Aparatos - Curvas de Carga almacenado")


      val maestroContratosMaestroAparatosCurvasCargaExpedientesNormal = sql(
        """SELECT  mCmAcC.origen,   mCmAcC.cptocred, mCmAcC.cfinca, mCmAcC.cptoserv, mCmAcC.cderind, mCmAcC.cupsree, mCmAcC.ccounips, mCmAcC.cupsree2,mCmAcC.cpuntmed,
                   mCmAcC.tpuntmed, mCmAcC.vparsist, mCmAcC.cemptitu,mCmAcC.ccontrat, mCmAcC.cnumscct, mCmAcC.fpsercon,mCmAcC.ffinvesu, mCmAcC.ccliente, mCmAcC.fechamov,
                   mCmAcC.tindfiju, mCmAcC.cnifdnic, mCmAcC.dapersoc,mCmAcC.dnombcli, mCmAcC.csecptom, mCmAcC.fvigorpm,mCmAcC.fbajapm,mCmAcC.caparmed, mCmAcC.flectreg,
                   mCmAcC.testcaco, mCmAcC.obiscode, mCmAcC.vsecccar,
                   mCmAcC.flectreg, mCmAcC.testcaco, mCmAcC.obiscode, mCmAcC.vsecccar,
                   mCmAcC.hora_01, mCmAcC.1q_consumo_01, mCmAcC.2q_consumo_01, mCmAcC.3q_consumo_01, mCmAcC.4q_consumo_01,mCmAcC.substatus_01,mCmAcC.testmenn_01,mCmAcC.testmecnn_01,
                   mCmAcC.hora_02, mCmAcC.1q_consumo_02, mCmAcC.2q_consumo_02, mCmAcC.3q_consumo_02, mCmAcC.4q_consumo_02,mCmAcC.substatus_02,mCmAcC.testmenn_02,mCmAcC.testmecnn_02,
                   mCmAcC.hora_03, mCmAcC.1q_consumo_03, mCmAcC.2q_consumo_03, mCmAcC.3q_consumo_03, mCmAcC.4q_consumo_03,mCmAcC.substatus_03,mCmAcC.testmenn_03,mCmAcC.testmecnn_03,
                   mCmAcC.hora_04, mCmAcC.1q_consumo_04, mCmAcC.2q_consumo_04, mCmAcC.3q_consumo_04, mCmAcC.4q_consumo_04,mCmAcC.substatus_04,mCmAcC.testmenn_04,mCmAcC.testmecnn_04,
                   mCmAcC.hora_05, mCmAcC.1q_consumo_05, mCmAcC.2q_consumo_05, mCmAcC.3q_consumo_05, mCmAcC.4q_consumo_05,mCmAcC.substatus_05,mCmAcC.testmenn_05,mCmAcC.testmecnn_05,
                   mCmAcC.hora_06, mCmAcC.1q_consumo_06, mCmAcC.2q_consumo_06, mCmAcC.3q_consumo_06, mCmAcC.4q_consumo_06,mCmAcC.substatus_06,mCmAcC.testmenn_06,mCmAcC.testmecnn_06,
                   mCmAcC.hora_07, mCmAcC.1q_consumo_07, mCmAcC.2q_consumo_07, mCmAcC.3q_consumo_07, mCmAcC.4q_consumo_07,mCmAcC.substatus_07,mCmAcC.testmenn_07,mCmAcC.testmecnn_07,
                   mCmAcC.hora_08, mCmAcC.1q_consumo_08, mCmAcC.2q_consumo_08, mCmAcC.3q_consumo_08, mCmAcC.4q_consumo_08,mCmAcC.substatus_08,mCmAcC.testmenn_08,mCmAcC.testmecnn_08,
                   mCmAcC.hora_09, mCmAcC.1q_consumo_09, mCmAcC.2q_consumo_09, mCmAcC.3q_consumo_09, mCmAcC.4q_consumo_09,mCmAcC.substatus_09,mCmAcC.testmenn_09,mCmAcC.testmecnn_09,
                   mCmAcC.hora_10, mCmAcC.1q_consumo_10, mCmAcC.2q_consumo_10, mCmAcC.3q_consumo_10, mCmAcC.4q_consumo_10,mCmAcC.substatus_10,mCmAcC.testmenn_10,mCmAcC.testmecnn_10,
                   mCmAcC.hora_11, mCmAcC.1q_consumo_11, mCmAcC.2q_consumo_11, mCmAcC.3q_consumo_11, mCmAcC.4q_consumo_11,mCmAcC.substatus_11,mCmAcC.testmenn_11,mCmAcC.testmecnn_11,
                   mCmAcC.hora_12, mCmAcC.1q_consumo_12, mCmAcC.2q_consumo_12, mCmAcC.3q_consumo_12, mCmAcC.4q_consumo_12,mCmAcC.substatus_12,mCmAcC.testmenn_12,mCmAcC.testmecnn_12,
                   mCmAcC.hora_13, mCmAcC.1q_consumo_13, mCmAcC.2q_consumo_13, mCmAcC.3q_consumo_13, mCmAcC.4q_consumo_13,mCmAcC.substatus_13,mCmAcC.testmenn_13,mCmAcC.testmecnn_13,
                   mCmAcC.hora_14, mCmAcC.1q_consumo_14, mCmAcC.2q_consumo_14, mCmAcC.3q_consumo_14, mCmAcC.4q_consumo_14,mCmAcC.substatus_14,mCmAcC.testmenn_14,mCmAcC.testmecnn_14,
                   mCmAcC.hora_15, mCmAcC.1q_consumo_15, mCmAcC.2q_consumo_15, mCmAcC.3q_consumo_15, mCmAcC.4q_consumo_15, mCmAcC.substatus_15, mCmAcC.testmenn_15, mCmAcC.testmecnn_15,
                   mCmAcC.hora_16, mCmAcC.1q_consumo_16, mCmAcC.2q_consumo_16, mCmAcC.3q_consumo_16, mCmAcC.4q_consumo_16, mCmAcC.substatus_16, mCmAcC.testmenn_16, mCmAcC.testmecnn_16,
                   mCmAcC.hora_17, mCmAcC.1q_consumo_17, mCmAcC.2q_consumo_17, mCmAcC.3q_consumo_17, mCmAcC.4q_consumo_17, mCmAcC.substatus_17, mCmAcC.testmenn_17, mCmAcC.testmecnn_17,
                   mCmAcC.hora_18, mCmAcC.1q_consumo_18, mCmAcC.2q_consumo_18, mCmAcC.3q_consumo_18, mCmAcC.4q_consumo_18, mCmAcC.substatus_18, mCmAcC.testmenn_18, mCmAcC.testmecnn_18,
                   mCmAcC.hora_19, mCmAcC.1q_consumo_19, mCmAcC.2q_consumo_19, mCmAcC.3q_consumo_19, mCmAcC.4q_consumo_19, mCmAcC.substatus_19, mCmAcC.testmenn_19, mCmAcC.testmecnn_19,
                   mCmAcC.hora_20, mCmAcC.1q_consumo_20, mCmAcC.2q_consumo_20, mCmAcC.3q_consumo_20, mCmAcC.4q_consumo_20, mCmAcC.substatus_20, mCmAcC.testmenn_20, mCmAcC.testmecnn_20,
                   mCmAcC.hora_21, mCmAcC.1q_consumo_21, mCmAcC.2q_consumo_21, mCmAcC.3q_consumo_21, mCmAcC.4q_consumo_21, mCmAcC.substatus_21, mCmAcC.testmenn_21, mCmAcC.testmecnn_21,
                   mCmAcC.hora_22, mCmAcC.1q_consumo_22, mCmAcC.2q_consumo_22, mCmAcC.3q_consumo_22, mCmAcC.4q_consumo_22, mCmAcC.substatus_22, mCmAcC.testmenn_22, mCmAcC.testmecnn_22,
                   mCmAcC.hora_23, mCmAcC.1q_consumo_23, mCmAcC.2q_consumo_23, mCmAcC.3q_consumo_23, mCmAcC.4q_consumo_23, mCmAcC.substatus_23, mCmAcC.testmenn_23, mCmAcC.testmecnn_23,
                   mCmAcC.hora_24, mCmAcC.1q_consumo_24, mCmAcC.2q_consumo_24, mCmAcC.3q_consumo_24, mCmAcC.4q_consumo_24, mCmAcC.substatus_24, mCmAcC.testmenn_24, mCmAcC.testmecnn_24,
                   mCmAcC.hora_25, mCmAcC.1q_consumo_25, mCmAcC.2q_consumo_25, mCmAcC.3q_consumo_25, mCmAcC.4q_consumo_25, mCmAcC.substatus_25, mCmAcC.testmenn_25, mCmAcC.testmecnn_25,
                   Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
                   Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali,
                   Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,Expedientes.fciexped
                   FROM mCmAcC LEFT JOIN Expedientes
                   ON mCmAcC.origen=Expedientes.origen AND mCmAcC.cemptitu=Expedientes.cemptitu AND mCmAcC.cfinca=Expedientes.cfinca AND mCmAcC.cptoserv=Expedientes.cptoserv
                   WHERE Expedientes.origen IS NULL AND Expedientes.cemptitu IS NULL AND Expedientes.cfinca IS NULL AND Expedientes.cptoserv IS NULL
                   """)

      df_16.unpersist()
      maestroContratosMaestroAparatosCurvasCarga.unpersist()

      maestroContratosMaestroAparatosCurvasCargaExpedientesNormal.persist(nivel)
      println ("Join Maestro Contratos - Maestro Aparatos - Curvas de Carga - Expedientes (Normal) = "+maestroContratosMaestroAparatosCurvasCargaExpedientesNormal.count()+" registros")
      maestroContratosMaestroAparatosCurvasCargaExpedientesNormal.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosMaestroAparatosCurvasCargaSinExpedientes")
      maestroContratosMaestroAparatosCurvasCargaExpedientesNormal.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosMaestroAparatosCurvasCargaSinExpedientes")
      println("Join Maestro Contratos - Maestro Aparatos - Curvas de Carga - Expedientes (Normal) almacenado")




      val lecturasNormal_aux = maestroContratosMaestroAparatosCurvasCargaExpedientesNormal.select(
        "cupsree","ccontrat",
        "finifran", "ffinfran", "fapexpd", "fciexped",
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


      maestroContratosMaestroAparatosCurvasCargaExpedientesNormal.unpersist()

      lecturasNormal_aux.persist(nivel)
      val iaux = lecturasNormal_aux.count()

      val lecturasNormal = lecturasNormal_aux.dropDuplicates()
      lecturasNormal.persist(nivel)
      val i = lecturasNormal.count()

      println("Registros curvas normal con duplicados = "+iaux)
      println("Registros curvas normal sin duplicados = "+i)
      println("Diferencia = "+(iaux-i))

      lecturasNormal_aux.unpersist()

      println("Guardando Parquet")
      lecturasNormal.coalesce(1).write.option("header","true").save(TabPaths.prefix_03+"lecturasNormal")

      println("Guardando Csv")
      lecturasNormal.coalesce(1).write.option("header","true").csv(TabPaths.prefix_04+"lecturasNormal")

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }


}
