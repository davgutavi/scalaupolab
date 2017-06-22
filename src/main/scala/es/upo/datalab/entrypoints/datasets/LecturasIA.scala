package es.upo.datalab.entrypoints.datasets

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object LecturasIA {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    TimingUtils.time {

      val df_00C = LoadTableParquet.loadTable(TabPaths.TAB_00C)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MC")

      val df_16 = LoadTableParquet.loadTable(TabPaths.TAB_16)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("E")

      val df_00E = LoadTableParquet.loadTable(TabPaths.TAB_00E)
      df_00E.persist(nivel)
      df_00E.createOrReplaceTempView("MA")

      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB_01)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CC")


      ///******************************************************************************************PASO 1: MCE = MC U E ==> fapexpd >= fpsercon

      val mce = sql(
        """
                SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.cnumscct, MC.fpsercon, MC.ffinvesu,
                       E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe,
                       E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped
                FROM MC JOIN E
                ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon
              """)

      println("Persistiendo mce")

      mce.persist(nivel)

      df_00C.unpersist()

      df_16.unpersist()

      println("Checkpoint mce")

      mce.checkpoint()

//      mce.coalesce(1).write.option("header", "true").save(TabPaths.prefix_05 + "mce")

      mce.createOrReplaceTempView("MCE")


      ///******************************************************************************************PASO 2: MCEMA = MCE U MA

      val mcema = sql(
        """
              SELECT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cptoserv, MCE.cderind, MCE.cupsree,MCE.ccounips,MCE.cupsree2, MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu,
                     MCE.ccontrat, MCE.cnumscct, MCE.fpsercon, MCE.ffinvesu,MCE.csecexpe, MCE.fapexpd, MCE.finifran, MCE.ffinfran, MCE.anomalia, MCE.irregularidad,
                     MCE.venacord, MCE.vennofai, MCE.torigexp, MCE.texpedie,MCE.expclass, MCE.testexpe,MCE.fnormali, MCE.cplan, MCE.ccampa, MCE.cempresa, MCE.fciexped,
                     MA.csecptom, MA.fvigorpm, MA.fbajapm,MA.caparmed
              FROM MCE JOIN MA
              ON MCE.origen = MA.origen AND MCE.cupsree2 = MA.cupsree2 AND MCE.cpuntmed = MA.cpuntmed
            """)

      println("Persistiendo mcema")

      mcema.persist(nivel)

      df_00E.unpersist()

      mce.unpersist()

      println("Checkpoint mcema")

      mcema.checkpoint()

//      mcema.coalesce(1).write.option("header", "true").save(TabPaths.prefix_05 + "mcema")

      mcema.createOrReplaceTempView("MCEMA")


      ///******************************************************************************************PASO 3: MCEMACC = MCEMA U CC ==> datediff ( flectreg , fapexpd ) AS dif


      val mcemacc_diff = sql(
        """
            SELECT MCEMA.origen, MCEMA.cptocred, MCEMA.cfinca, MCEMA.cptoserv,MCEMA.cderind, MCEMA.cupsree,MCEMA.ccounips,MCEMA.cupsree2,MCEMA.cpuntmed, MCEMA.tpuntmed, MCEMA.vparsist, MCEMA.cemptitu,
                   MCEMA.ccontrat, MCEMA.cnumscct, MCEMA.fpsercon, MCEMA.ffinvesu,MCEMA.csecexpe, MCEMA.fapexpd, MCEMA.finifran, MCEMA.ffinfran,MCEMA.anomalia, MCEMA.irregularidad,MCEMA.venacord, MCEMA.vennofai,
                   MCEMA.torigexp, MCEMA.texpedie,MCEMA.expclass, MCEMA.testexpe,MCEMA.fnormali, MCEMA.cplan, MCEMA.ccampa, MCEMA.cempresa,MCEMA.fciexped,MCEMA.csecptom, MCEMA.fvigorpm, MCEMA.fbajapm,MCEMA.caparmed,
                   CC.flectreg, datediff ( flectreg , fapexpd ) AS dif,
                   CC.testcaco, CC.obiscode, CC.vsecccar,
                   CC.hora_01, CC.1q_consumo_01, CC.2q_consumo_01, CC.3q_consumo_01, CC.4q_consumo_01,CC.substatus_01,CC.testmenn_01,CC.testmecnn_01,
                   CC.hora_02, CC.1q_consumo_02, CC.2q_consumo_02, CC.3q_consumo_02, CC.4q_consumo_02,CC.substatus_02,CC.testmenn_02,CC.testmecnn_02,
                   CC.hora_03, CC.1q_consumo_03, CC.2q_consumo_03, CC.3q_consumo_03, CC.4q_consumo_03,CC.substatus_03,CC.testmenn_03,CC.testmecnn_03,
                   CC.hora_04, CC.1q_consumo_04, CC.2q_consumo_04, CC.3q_consumo_04, CC.4q_consumo_04,CC.substatus_04,CC.testmenn_04,CC.testmecnn_04,
                   CC.hora_05, CC.1q_consumo_05, CC.2q_consumo_05, CC.3q_consumo_05, CC.4q_consumo_05,CC.substatus_05,CC.testmenn_05,CC.testmecnn_05,
                   CC.hora_06, CC.1q_consumo_06, CC.2q_consumo_06, CC.3q_consumo_06, CC.4q_consumo_06,CC.substatus_06,CC.testmenn_06,CC.testmecnn_06,
                   CC.hora_07, CC.1q_consumo_07, CC.2q_consumo_07, CC.3q_consumo_07, CC.4q_consumo_07,CC.substatus_07,CC.testmenn_07,CC.testmecnn_07,
                   CC.hora_08, CC.1q_consumo_08, CC.2q_consumo_08, CC.3q_consumo_08, CC.4q_consumo_08,CC.substatus_08,CC.testmenn_08,CC.testmecnn_08,
                   CC.hora_09, CC.1q_consumo_09, CC.2q_consumo_09, CC.3q_consumo_09, CC.4q_consumo_09,CC.substatus_09,CC.testmenn_09,CC.testmecnn_09,
                   CC.hora_10, CC.1q_consumo_10, CC.2q_consumo_10, CC.3q_consumo_10, CC.4q_consumo_10,CC.substatus_10,CC.testmenn_10,CC.testmecnn_10,
                   CC.hora_11, CC.1q_consumo_11, CC.2q_consumo_11, CC.3q_consumo_11, CC.4q_consumo_11,CC.substatus_11,CC.testmenn_11,CC.testmecnn_11,
                   CC.hora_12, CC.1q_consumo_12, CC.2q_consumo_12, CC.3q_consumo_12, CC.4q_consumo_12,CC.substatus_12,CC.testmenn_12,CC.testmecnn_12,
                   CC.hora_13, CC.1q_consumo_13, CC.2q_consumo_13, CC.3q_consumo_13, CC.4q_consumo_13,CC.substatus_13,CC.testmenn_13,CC.testmecnn_13,
                   CC.hora_14, CC.1q_consumo_14, CC.2q_consumo_14, CC.3q_consumo_14, CC.4q_consumo_14,CC.substatus_14,CC.testmenn_14,CC.testmecnn_14,
                   CC.hora_15, CC.1q_consumo_15, CC.2q_consumo_15, CC.3q_consumo_15, CC.4q_consumo_15, CC.substatus_15, CC.testmenn_15, CC.testmecnn_15,
                   CC.hora_16, CC.1q_consumo_16, CC.2q_consumo_16, CC.3q_consumo_16, CC.4q_consumo_16, CC.substatus_16, CC.testmenn_16, CC.testmecnn_16,
                   CC.hora_17, CC.1q_consumo_17, CC.2q_consumo_17, CC.3q_consumo_17, CC.4q_consumo_17, CC.substatus_17, CC.testmenn_17, CC.testmecnn_17,
                   CC.hora_18, CC.1q_consumo_18, CC.2q_consumo_18, CC.3q_consumo_18, CC.4q_consumo_18, CC.substatus_18, CC.testmenn_18, CC.testmecnn_18,
                   CC.hora_19, CC.1q_consumo_19, CC.2q_consumo_19, CC.3q_consumo_19, CC.4q_consumo_19, CC.substatus_19, CC.testmenn_19, CC.testmecnn_19,
                   CC.hora_20, CC.1q_consumo_20, CC.2q_consumo_20, CC.3q_consumo_20, CC.4q_consumo_20, CC.substatus_20, CC.testmenn_20, CC.testmecnn_20,
                   CC.hora_21, CC.1q_consumo_21, CC.2q_consumo_21, CC.3q_consumo_21, CC.4q_consumo_21, CC.substatus_21, CC.testmenn_21, CC.testmecnn_21,
                   CC.hora_22, CC.1q_consumo_22, CC.2q_consumo_22, CC.3q_consumo_22, CC.4q_consumo_22, CC.substatus_22, CC.testmenn_22, CC.testmecnn_22,
                   CC.hora_23, CC.1q_consumo_23, CC.2q_consumo_23, CC.3q_consumo_23, CC.4q_consumo_23, CC.substatus_23, CC.testmenn_23, CC.testmecnn_23,
                   CC.hora_24, CC.1q_consumo_24, CC.2q_consumo_24, CC.3q_consumo_24, CC.4q_consumo_24, CC.substatus_24, CC.testmenn_24, CC.testmecnn_24,
                   CC.hora_25, CC.1q_consumo_25, CC.2q_consumo_25, CC.3q_consumo_25, CC.4q_consumo_25, CC.substatus_25, CC.testmenn_25, CC.testmecnn_25
           FROM MCEMA JOIN CC
           ON MCEMA.origen = CC.origen AND MCEMA.cpuntmed = CC.cpuntmed AND CC.obiscode = 'A' AND CC.testcaco = 'R'
           """)

      println("Persistiendo mcemacc_diff")

      mcemacc_diff.persist(nivel)

      df_01.unpersist()
      mcema.unpersist()

      mcemacc_diff.createOrReplaceTempView("MCEMACC")



      ///******************************************************************************************PASO 4: LRDATES = CUPSREE, LEFTDATE, RIGHTDATE, DIF


      val lrdates_aux = sql(
        """
           SELECT cupsree,add_months(flectreg,-6) AS leftdate, flectreg AS rightdate, dif, fapexpd FROM MCEMACC
           WHERE (cupsree,dif) IN (SELECT cupsree, max(dif) as maximo FROM MCEMACC GROUP BY cupsree)
        """)

      println("Persistiendo lrdates_aux")

      lrdates_aux.persist(nivel)

      val lrdates = lrdates_aux.dropDuplicates()

      println("Persistiendo lrdates")

      lrdates.persist(nivel)

      lrdates_aux.unpersist()

      lrdates.createOrReplaceTempView("LRDATES")

      ///******************************************************************************************PASO 5: MCEMACCLR =  MCEMACC U LRDATES ==> flectreg BETWEEN leftdate AND rightdate (JOIN: cupsree, fapexpd)


      val mcemacclr = sql(
        """
            SELECT MCEMACC.origen, MCEMACC.cptocred, MCEMACC.cfinca, MCEMACC.cptoserv,MCEMACC.cderind, MCEMACC.cupsree,MCEMACC.ccounips,MCEMACC.cupsree2,MCEMACC.cpuntmed, MCEMACC.tpuntmed, MCEMACC.vparsist, MCEMACC.cemptitu,
                   MCEMACC.ccontrat, MCEMACC.cnumscct, MCEMACC.fpsercon, MCEMACC.ffinvesu,MCEMACC.csecexpe, MCEMACC.fapexpd, MCEMACC.finifran, MCEMACC.ffinfran,MCEMACC.anomalia, MCEMACC.irregularidad,MCEMACC.venacord, MCEMACC.vennofai,
                   MCEMACC.torigexp, MCEMACC.texpedie,MCEMACC.expclass, MCEMACC.testexpe,MCEMACC.fnormali, MCEMACC.cplan, MCEMACC.ccampa, MCEMACC.cempresa,MCEMACC.fciexped,MCEMACC.csecptom, MCEMACC.fvigorpm, MCEMACC.fbajapm,MCEMACC.caparmed,
                   MCEMACC.flectreg, LRDATES.rightdate, LRDATES.leftdate,
                   MCEMACC.testcaco, MCEMACC.obiscode, MCEMACC.vsecccar,
                   MCEMACC.hora_01, MCEMACC.1q_consumo_01, MCEMACC.2q_consumo_01, MCEMACC.3q_consumo_01, MCEMACC.4q_consumo_01,MCEMACC.substatus_01,MCEMACC.testmenn_01,MCEMACC.testmecnn_01,
                   MCEMACC.hora_02, MCEMACC.1q_consumo_02, MCEMACC.2q_consumo_02, MCEMACC.3q_consumo_02, MCEMACC.4q_consumo_02,MCEMACC.substatus_02,MCEMACC.testmenn_02,MCEMACC.testmecnn_02,
                   MCEMACC.hora_03, MCEMACC.1q_consumo_03, MCEMACC.2q_consumo_03, MCEMACC.3q_consumo_03, MCEMACC.4q_consumo_03,MCEMACC.substatus_03,MCEMACC.testmenn_03,MCEMACC.testmecnn_03,
                   MCEMACC.hora_04, MCEMACC.1q_consumo_04, MCEMACC.2q_consumo_04, MCEMACC.3q_consumo_04, MCEMACC.4q_consumo_04,MCEMACC.substatus_04,MCEMACC.testmenn_04,MCEMACC.testmecnn_04,
                   MCEMACC.hora_05, MCEMACC.1q_consumo_05, MCEMACC.2q_consumo_05, MCEMACC.3q_consumo_05, MCEMACC.4q_consumo_05,MCEMACC.substatus_05,MCEMACC.testmenn_05,MCEMACC.testmecnn_05,
                   MCEMACC.hora_06, MCEMACC.1q_consumo_06, MCEMACC.2q_consumo_06, MCEMACC.3q_consumo_06, MCEMACC.4q_consumo_06,MCEMACC.substatus_06,MCEMACC.testmenn_06,MCEMACC.testmecnn_06,
                   MCEMACC.hora_07, MCEMACC.1q_consumo_07, MCEMACC.2q_consumo_07, MCEMACC.3q_consumo_07, MCEMACC.4q_consumo_07,MCEMACC.substatus_07,MCEMACC.testmenn_07,MCEMACC.testmecnn_07,
                   MCEMACC.hora_08, MCEMACC.1q_consumo_08, MCEMACC.2q_consumo_08, MCEMACC.3q_consumo_08, MCEMACC.4q_consumo_08,MCEMACC.substatus_08,MCEMACC.testmenn_08,MCEMACC.testmecnn_08,
                   MCEMACC.hora_09, MCEMACC.1q_consumo_09, MCEMACC.2q_consumo_09, MCEMACC.3q_consumo_09, MCEMACC.4q_consumo_09,MCEMACC.substatus_09,MCEMACC.testmenn_09,MCEMACC.testmecnn_09,
                   MCEMACC.hora_10, MCEMACC.1q_consumo_10, MCEMACC.2q_consumo_10, MCEMACC.3q_consumo_10, MCEMACC.4q_consumo_10,MCEMACC.substatus_10,MCEMACC.testmenn_10,MCEMACC.testmecnn_10,
                   MCEMACC.hora_11, MCEMACC.1q_consumo_11, MCEMACC.2q_consumo_11, MCEMACC.3q_consumo_11, MCEMACC.4q_consumo_11,MCEMACC.substatus_11,MCEMACC.testmenn_11,MCEMACC.testmecnn_11,
                   MCEMACC.hora_12, MCEMACC.1q_consumo_12, MCEMACC.2q_consumo_12, MCEMACC.3q_consumo_12, MCEMACC.4q_consumo_12,MCEMACC.substatus_12,MCEMACC.testmenn_12,MCEMACC.testmecnn_12,
                   MCEMACC.hora_13, MCEMACC.1q_consumo_13, MCEMACC.2q_consumo_13, MCEMACC.3q_consumo_13, MCEMACC.4q_consumo_13,MCEMACC.substatus_13,MCEMACC.testmenn_13,MCEMACC.testmecnn_13,
                   MCEMACC.hora_14, MCEMACC.1q_consumo_14, MCEMACC.2q_consumo_14, MCEMACC.3q_consumo_14, MCEMACC.4q_consumo_14,MCEMACC.substatus_14,MCEMACC.testmenn_14,MCEMACC.testmecnn_14,
                   MCEMACC.hora_15, MCEMACC.1q_consumo_15, MCEMACC.2q_consumo_15, MCEMACC.3q_consumo_15, MCEMACC.4q_consumo_15, MCEMACC.substatus_15, MCEMACC.testmenn_15, MCEMACC.testmecnn_15,
                   MCEMACC.hora_16, MCEMACC.1q_consumo_16, MCEMACC.2q_consumo_16, MCEMACC.3q_consumo_16, MCEMACC.4q_consumo_16, MCEMACC.substatus_16, MCEMACC.testmenn_16, MCEMACC.testmecnn_16,
                   MCEMACC.hora_17, MCEMACC.1q_consumo_17, MCEMACC.2q_consumo_17, MCEMACC.3q_consumo_17, MCEMACC.4q_consumo_17, MCEMACC.substatus_17, MCEMACC.testmenn_17, MCEMACC.testmecnn_17,
                   MCEMACC.hora_18, MCEMACC.1q_consumo_18, MCEMACC.2q_consumo_18, MCEMACC.3q_consumo_18, MCEMACC.4q_consumo_18, MCEMACC.substatus_18, MCEMACC.testmenn_18, MCEMACC.testmecnn_18,
                   MCEMACC.hora_19, MCEMACC.1q_consumo_19, MCEMACC.2q_consumo_19, MCEMACC.3q_consumo_19, MCEMACC.4q_consumo_19, MCEMACC.substatus_19, MCEMACC.testmenn_19, MCEMACC.testmecnn_19,
                   MCEMACC.hora_20, MCEMACC.1q_consumo_20, MCEMACC.2q_consumo_20, MCEMACC.3q_consumo_20, MCEMACC.4q_consumo_20, MCEMACC.substatus_20, MCEMACC.testmenn_20, MCEMACC.testmecnn_20,
                   MCEMACC.hora_21, MCEMACC.1q_consumo_21, MCEMACC.2q_consumo_21, MCEMACC.3q_consumo_21, MCEMACC.4q_consumo_21, MCEMACC.substatus_21, MCEMACC.testmenn_21, MCEMACC.testmecnn_21,
                   MCEMACC.hora_22, MCEMACC.1q_consumo_22, MCEMACC.2q_consumo_22, MCEMACC.3q_consumo_22, MCEMACC.4q_consumo_22, MCEMACC.substatus_22, MCEMACC.testmenn_22, MCEMACC.testmecnn_22,
                   MCEMACC.hora_23, MCEMACC.1q_consumo_23, MCEMACC.2q_consumo_23, MCEMACC.3q_consumo_23, MCEMACC.4q_consumo_23, MCEMACC.substatus_23, MCEMACC.testmenn_23, MCEMACC.testmecnn_23,
                   MCEMACC.hora_24, MCEMACC.1q_consumo_24, MCEMACC.2q_consumo_24, MCEMACC.3q_consumo_24, MCEMACC.4q_consumo_24, MCEMACC.substatus_24, MCEMACC.testmenn_24, MCEMACC.testmecnn_24,
                   MCEMACC.hora_25, MCEMACC.1q_consumo_25, MCEMACC.2q_consumo_25, MCEMACC.3q_consumo_25, MCEMACC.4q_consumo_25, MCEMACC.substatus_25, MCEMACC.testmenn_25, MCEMACC.testmecnn_25
           FROM MCEMACC JOIN LRDATES
           ON MCEMACC.cupsree = LRDATES.cupsree AND MCEMACC.fapexpd = LRDATES.fapexpd AND MCEMACC.flectreg BETWEEN LRDATES.leftdate AND LRDATES.rightdate
           """)

      println("Persistiendo mcemacclr")

      mcemacclr.persist(nivel)

      mcemacc_diff.unpersist()

      lrdates.unpersist()

      mcemacclr.createOrReplaceTempView("MCEMACCLR")



      ///******************************************************************************************PASO 6: lecturasIrregularidad =  MCEMACCLR con Irregularidad


      val li_aux = sql(
        """
            SELECT
           cupsree, ccontrat, cnumscct, fpsercon, ffinvesu, fapexpd, fciexped,
           flectreg, testcaco, obiscode, vsecccar,
           hora_01, 1q_consumo_01, 2q_consumo_01, 3q_consumo_01, 4q_consumo_01,substatus_01,testmenn_01,testmecnn_01,
           hora_02, 1q_consumo_02, 2q_consumo_02, 3q_consumo_02, 4q_consumo_02,substatus_02,testmenn_02,testmecnn_02,
           hora_03, 1q_consumo_03, 2q_consumo_03, 3q_consumo_03, 4q_consumo_03,substatus_03,testmenn_03,testmecnn_03,
           hora_04, 1q_consumo_04, 2q_consumo_04, 3q_consumo_04, 4q_consumo_04,substatus_04,testmenn_04,testmecnn_04,
           hora_05, 1q_consumo_05, 2q_consumo_05, 3q_consumo_05, 4q_consumo_05,substatus_05,testmenn_05,testmecnn_05,
           hora_06, 1q_consumo_06, 2q_consumo_06, 3q_consumo_06, 4q_consumo_06,substatus_06,testmenn_06,testmecnn_06,
           hora_07, 1q_consumo_07, 2q_consumo_07, 3q_consumo_07, 4q_consumo_07,substatus_07,testmenn_07,testmecnn_07,
           hora_08, 1q_consumo_08, 2q_consumo_08, 3q_consumo_08, 4q_consumo_08,substatus_08,testmenn_08,testmecnn_08,
           hora_09, 1q_consumo_09, 2q_consumo_09, 3q_consumo_09, 4q_consumo_09,substatus_09,testmenn_09,testmecnn_09,
           hora_10, 1q_consumo_10, 2q_consumo_10, 3q_consumo_10, 4q_consumo_10,substatus_10,testmenn_10,testmecnn_10,
           hora_11, 1q_consumo_11, 2q_consumo_11, 3q_consumo_11, 4q_consumo_11,substatus_11,testmenn_11,testmecnn_11,
           hora_12, 1q_consumo_12, 2q_consumo_12, 3q_consumo_12, 4q_consumo_12,substatus_12,testmenn_12,testmecnn_12,
           hora_13, 1q_consumo_13, 2q_consumo_13, 3q_consumo_13, 4q_consumo_13,substatus_13,testmenn_13,testmecnn_13,
           hora_14, 1q_consumo_14, 2q_consumo_14, 3q_consumo_14, 4q_consumo_14,substatus_14,testmenn_14,testmecnn_14,
           hora_15, 1q_consumo_15, 2q_consumo_15, 3q_consumo_15, 4q_consumo_15, substatus_15, testmenn_15, testmecnn_15,
           hora_16, 1q_consumo_16, 2q_consumo_16, 3q_consumo_16, 4q_consumo_16, substatus_16, testmenn_16, testmecnn_16,
           hora_17, 1q_consumo_17, 2q_consumo_17, 3q_consumo_17, 4q_consumo_17, substatus_17, testmenn_17, testmecnn_17,
           hora_18, 1q_consumo_18, 2q_consumo_18, 3q_consumo_18, 4q_consumo_18, substatus_18, testmenn_18, testmecnn_18,
           hora_19, 1q_consumo_19, 2q_consumo_19, 3q_consumo_19, 4q_consumo_19, substatus_19, testmenn_19, testmecnn_19,
           hora_20, 1q_consumo_20, 2q_consumo_20, 3q_consumo_20, 4q_consumo_20, substatus_20, testmenn_20, testmecnn_20,
           hora_21, 1q_consumo_21, 2q_consumo_21, 3q_consumo_21, 4q_consumo_21, substatus_21, testmenn_21, testmecnn_21,
           hora_22, 1q_consumo_22, 2q_consumo_22, 3q_consumo_22, 4q_consumo_22, substatus_22, testmenn_22, testmecnn_22,
           hora_23, 1q_consumo_23, 2q_consumo_23, 3q_consumo_23, 4q_consumo_23, substatus_23, testmenn_23, testmecnn_23,
           hora_24, 1q_consumo_24, 2q_consumo_24, 3q_consumo_24, 4q_consumo_24, substatus_24, testmenn_24, testmecnn_24,
           hora_25, 1q_consumo_25, 2q_consumo_25, 3q_consumo_25, 4q_consumo_25, substatus_25, testmenn_25, testmecnn_25
           FROM MCEMACCLR WHERE irregularidad='S'""")


       ///******************************************************************************************PASO 7: lecturasAnomalia =  MCEMACCLR con Anomalía


      val la_aux = sql(
        """
            SELECT
           cupsree, ccontrat, cnumscct, fpsercon, ffinvesu, fapexpd, fciexped,
           flectreg, testcaco, obiscode, vsecccar,
           hora_01, 1q_consumo_01, 2q_consumo_01, 3q_consumo_01, 4q_consumo_01,substatus_01,testmenn_01,testmecnn_01,
           hora_02, 1q_consumo_02, 2q_consumo_02, 3q_consumo_02, 4q_consumo_02,substatus_02,testmenn_02,testmecnn_02,
           hora_03, 1q_consumo_03, 2q_consumo_03, 3q_consumo_03, 4q_consumo_03,substatus_03,testmenn_03,testmecnn_03,
           hora_04, 1q_consumo_04, 2q_consumo_04, 3q_consumo_04, 4q_consumo_04,substatus_04,testmenn_04,testmecnn_04,
           hora_05, 1q_consumo_05, 2q_consumo_05, 3q_consumo_05, 4q_consumo_05,substatus_05,testmenn_05,testmecnn_05,
           hora_06, 1q_consumo_06, 2q_consumo_06, 3q_consumo_06, 4q_consumo_06,substatus_06,testmenn_06,testmecnn_06,
           hora_07, 1q_consumo_07, 2q_consumo_07, 3q_consumo_07, 4q_consumo_07,substatus_07,testmenn_07,testmecnn_07,
           hora_08, 1q_consumo_08, 2q_consumo_08, 3q_consumo_08, 4q_consumo_08,substatus_08,testmenn_08,testmecnn_08,
           hora_09, 1q_consumo_09, 2q_consumo_09, 3q_consumo_09, 4q_consumo_09,substatus_09,testmenn_09,testmecnn_09,
           hora_10, 1q_consumo_10, 2q_consumo_10, 3q_consumo_10, 4q_consumo_10,substatus_10,testmenn_10,testmecnn_10,
           hora_11, 1q_consumo_11, 2q_consumo_11, 3q_consumo_11, 4q_consumo_11,substatus_11,testmenn_11,testmecnn_11,
           hora_12, 1q_consumo_12, 2q_consumo_12, 3q_consumo_12, 4q_consumo_12,substatus_12,testmenn_12,testmecnn_12,
           hora_13, 1q_consumo_13, 2q_consumo_13, 3q_consumo_13, 4q_consumo_13,substatus_13,testmenn_13,testmecnn_13,
           hora_14, 1q_consumo_14, 2q_consumo_14, 3q_consumo_14, 4q_consumo_14,substatus_14,testmenn_14,testmecnn_14,
           hora_15, 1q_consumo_15, 2q_consumo_15, 3q_consumo_15, 4q_consumo_15, substatus_15, testmenn_15, testmecnn_15,
           hora_16, 1q_consumo_16, 2q_consumo_16, 3q_consumo_16, 4q_consumo_16, substatus_16, testmenn_16, testmecnn_16,
           hora_17, 1q_consumo_17, 2q_consumo_17, 3q_consumo_17, 4q_consumo_17, substatus_17, testmenn_17, testmecnn_17,
           hora_18, 1q_consumo_18, 2q_consumo_18, 3q_consumo_18, 4q_consumo_18, substatus_18, testmenn_18, testmecnn_18,
           hora_19, 1q_consumo_19, 2q_consumo_19, 3q_consumo_19, 4q_consumo_19, substatus_19, testmenn_19, testmecnn_19,
           hora_20, 1q_consumo_20, 2q_consumo_20, 3q_consumo_20, 4q_consumo_20, substatus_20, testmenn_20, testmecnn_20,
           hora_21, 1q_consumo_21, 2q_consumo_21, 3q_consumo_21, 4q_consumo_21, substatus_21, testmenn_21, testmecnn_21,
           hora_22, 1q_consumo_22, 2q_consumo_22, 3q_consumo_22, 4q_consumo_22, substatus_22, testmenn_22, testmecnn_22,
           hora_23, 1q_consumo_23, 2q_consumo_23, 3q_consumo_23, 4q_consumo_23, substatus_23, testmenn_23, testmecnn_23,
           hora_24, 1q_consumo_24, 2q_consumo_24, 3q_consumo_24, 4q_consumo_24, substatus_24, testmenn_24, testmecnn_24,
           hora_25, 1q_consumo_25, 2q_consumo_25, 3q_consumo_25, 4q_consumo_25, substatus_25, testmenn_25, testmecnn_25
           FROM MCEMACCLR WHERE anomalia='S'""")


      ///******************************************************************************************PASO 8: Borrar duplicados y guardar datasets


//      li_aux.persist(nivel)

//      la_aux.persist(nivel)

      println("Checkpoint li_aux")

      li_aux.checkpoint()

      println("Checkpoint la_aux")

      la_aux.checkpoint()

      mcemacclr.unpersist()

      val li = li_aux.dropDuplicates()

      val la = la_aux.dropDuplicates()

      println("Checkpoint li")

      li.checkpoint()

      println("Checkpoint la")

      la.checkpoint()

      println("Count li_aux")

      val iaux = li_aux.count()

      println("Count li")

      val i = li.count()
      println("Dataset Lecturas Irregularidad con duplicados = " + iaux)
      println("Dataset Lecturas Irregularidad                = " + i)
      println("Diferencia = " + (iaux - i))

      println("Guardando Irregularidad")
      li.coalesce(1).write.option("header", "true").save(TabPaths.prefix_03 + "lecturasIrregularidad")
      println("Parquet Irregularidad Guardada")

      println("Count la_aux")

      val aaux = la_aux.count()

      println("Count la")

      val a = la.count()
      println("Dataset Lecturas Anomalía con duplicados = " + aaux)
      println("Dataset Lecturas Anomalía                = " + a)
      println("Diferencia = " + (aaux - a))

      println("Guardando Anomalía")
      la.coalesce(1).write.option("header", "true").save(TabPaths.prefix_03 + "lecturasAnomalia")
      println("Parquet Anomalía Guardada")


      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}
