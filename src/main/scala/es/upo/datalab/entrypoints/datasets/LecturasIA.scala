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

      println("Prueba jar con spark")


      val r = SparkSessionUtils.sparkSession.read.csv("hdfs://192.168.47.247/user/gutierrez/endesa/tab05a.csv")

      r.show(20,truncate=false)


      val df_00C = LoadTableParquet.loadTable(TabPaths.TAB00C)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MC")

      val df_16 = LoadTableParquet.loadTable(TabPaths.TAB16)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("E")

      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB01)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CC")


      //**********************************PASO 1: MC U E ==>  eliminar duplicados por fapexpd >= fpsercon y fapexpd <= ffinvesu,
      //**********************************                                        por eliminación del secuencial de contrato del join y
      //**********************************                                        por fpsercon <> "0002-11-30" OR ffinvesu <>  "9999-12-31"

      val mce = sql(
        """
                SELECT DISTINCT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.fpsercon,
                                MC.ffinvesu,E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe,
                                E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped
                                FROM MC JOIN E
                                ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND
                                E.fapexpd >= MC.fpsercon AND E.fapexpd <= MC.ffinvesu

              """)
//      AND (fpsercon <> "0002-11-30" OR ffinvesu <>  "9999-12-31")

      println("Persistiendo mce")
      mce.persist(nivel)
      println("Mostrando mce")
      println("\nNúmero de registros de mce = "+mce.count())
      mce.show(5,truncate=false)
      df_00C.unpersist()
      df_16.unpersist()
      mce.createOrReplaceTempView("MCE")


      ///**********************************PASO 1.2: Eliminar duplicados por fechas infinitas

      val mce_aux = sql("""SELECT * FROM MCE WHERE fpsercon <> "0002-11-30" OR ffinvesu <> "9999-12-31" """)

//      val mce_aux = sql("""SELECT * FROM MCE WHERE fpsercon <> "0002-11-30" OR ffinvesu <>  "9999-12-31" """)

      println("Persistiendo mce_aux")
      mce_aux.persist(nivel)
      println("Mostrando mce_aux")
      println("\nNúmero de registros de mce_aux = "+mce_aux.count())
      mce_aux.show(200,truncate=false)
      mce_aux.unpersist()
      mce_aux.createOrReplaceTempView("MCE")



      val mcecc = sql(
        """
            SELECT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cptoserv,MCE.cderind, MCE.cupsree,MCE.ccounips,MCE.cupsree2,MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu,
                   MCE.ccontrat, MCE.fpsercon, MCE.ffinvesu,MCE.csecexpe, MCE.fapexpd, MCE.finifran, MCE.ffinfran,MCE.anomalia, MCE.irregularidad,MCE.venacord, MCE.vennofai,
                   MCE.torigexp, MCE.texpedie,MCE.expclass, MCE.testexpe,MCE.fnormali, MCE.cplan, MCE.ccampa, MCE.cempresa,MCE.fciexped,
                   CC.flectreg, datediff ( flectreg , fapexpd ) AS diferencia,
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
           FROM MCE JOIN CC
           ON MCE.origen = CC.origen AND MCE.cpuntmed = CC.cpuntmed AND CC.obiscode = 'A' AND CC.testcaco = 'R'
           """)

      println("Persistiendo mcecc")
      mcecc.persist(nivel)
      println("Mostrando mcecc")
      println("\nNúmero de registros de mcecc = "+mcecc.count())
      mcecc.show(5,truncate = false)
      df_01.unpersist()
      mce.unpersist()
      mcecc.createOrReplaceTempView("MCECC")

      val lrdates = sql(
        """
           SELECT DISTINCT cupsree, add_months(flectreg,-6) AS ldate, flectreg AS rdate, diferencia, fapexpd  FROM MCECC
           WHERE diferencia<=0 AND add_months(fapexpd,-3)>flectreg AND (cupsree,diferencia) IN (SELECT cupsree, max(diferencia) as maximo FROM MCECC GROUP BY cupsree)
        """)

      println("Persistiendo lrdates")
      lrdates.persist(nivel)
      println("Mostrando lrdates")
      println("\nNúmero de registros de lrdates = "+lrdates.count())
      lrdates.show(5,truncate=false)
      lrdates.createOrReplaceTempView("LRDATES")

      val mcecclr = sql(
        """
            SELECT MCECC.origen, MCECC.cptocred, MCECC.cfinca, MCECC.cptoserv,MCECC.cderind, MCECC.cupsree,MCECC.ccounips,MCECC.cupsree2,MCECC.cpuntmed, MCECC.tpuntmed, MCECC.vparsist, MCECC.cemptitu,
                   MCECC.ccontrat, MCECC.fpsercon, MCECC.ffinvesu,MCECC.csecexpe, MCECC.fapexpd, MCECC.finifran, MCECC.ffinfran,MCECC.anomalia, MCECC.irregularidad,MCECC.venacord, MCECC.vennofai,
                   MCECC.torigexp, MCECC.texpedie,MCECC.expclass, MCECC.testexpe,MCECC.fnormali, MCECC.cplan, MCECC.ccampa, MCECC.cempresa,MCECC.fciexped,
                   MCECC.flectreg, LRDATES.ldate,LRDATES.rdate, LRDATES.diferencia,
                   MCECC.testcaco, MCECC.obiscode, MCECC.vsecccar,
                   MCECC.hora_01, MCECC.1q_consumo_01, MCECC.2q_consumo_01, MCECC.3q_consumo_01, MCECC.4q_consumo_01,MCECC.substatus_01,MCECC.testmenn_01,MCECC.testmecnn_01,
                   MCECC.hora_02, MCECC.1q_consumo_02, MCECC.2q_consumo_02, MCECC.3q_consumo_02, MCECC.4q_consumo_02,MCECC.substatus_02,MCECC.testmenn_02,MCECC.testmecnn_02,
                   MCECC.hora_03, MCECC.1q_consumo_03, MCECC.2q_consumo_03, MCECC.3q_consumo_03, MCECC.4q_consumo_03,MCECC.substatus_03,MCECC.testmenn_03,MCECC.testmecnn_03,
                   MCECC.hora_04, MCECC.1q_consumo_04, MCECC.2q_consumo_04, MCECC.3q_consumo_04, MCECC.4q_consumo_04,MCECC.substatus_04,MCECC.testmenn_04,MCECC.testmecnn_04,
                   MCECC.hora_05, MCECC.1q_consumo_05, MCECC.2q_consumo_05, MCECC.3q_consumo_05, MCECC.4q_consumo_05,MCECC.substatus_05,MCECC.testmenn_05,MCECC.testmecnn_05,
                   MCECC.hora_06, MCECC.1q_consumo_06, MCECC.2q_consumo_06, MCECC.3q_consumo_06, MCECC.4q_consumo_06,MCECC.substatus_06,MCECC.testmenn_06,MCECC.testmecnn_06,
                   MCECC.hora_07, MCECC.1q_consumo_07, MCECC.2q_consumo_07, MCECC.3q_consumo_07, MCECC.4q_consumo_07,MCECC.substatus_07,MCECC.testmenn_07,MCECC.testmecnn_07,
                   MCECC.hora_08, MCECC.1q_consumo_08, MCECC.2q_consumo_08, MCECC.3q_consumo_08, MCECC.4q_consumo_08,MCECC.substatus_08,MCECC.testmenn_08,MCECC.testmecnn_08,
                   MCECC.hora_09, MCECC.1q_consumo_09, MCECC.2q_consumo_09, MCECC.3q_consumo_09, MCECC.4q_consumo_09,MCECC.substatus_09,MCECC.testmenn_09,MCECC.testmecnn_09,
                   MCECC.hora_10, MCECC.1q_consumo_10, MCECC.2q_consumo_10, MCECC.3q_consumo_10, MCECC.4q_consumo_10,MCECC.substatus_10,MCECC.testmenn_10,MCECC.testmecnn_10,
                   MCECC.hora_11, MCECC.1q_consumo_11, MCECC.2q_consumo_11, MCECC.3q_consumo_11, MCECC.4q_consumo_11,MCECC.substatus_11,MCECC.testmenn_11,MCECC.testmecnn_11,
                   MCECC.hora_12, MCECC.1q_consumo_12, MCECC.2q_consumo_12, MCECC.3q_consumo_12, MCECC.4q_consumo_12,MCECC.substatus_12,MCECC.testmenn_12,MCECC.testmecnn_12,
                   MCECC.hora_13, MCECC.1q_consumo_13, MCECC.2q_consumo_13, MCECC.3q_consumo_13, MCECC.4q_consumo_13,MCECC.substatus_13,MCECC.testmenn_13,MCECC.testmecnn_13,
                   MCECC.hora_14, MCECC.1q_consumo_14, MCECC.2q_consumo_14, MCECC.3q_consumo_14, MCECC.4q_consumo_14,MCECC.substatus_14,MCECC.testmenn_14,MCECC.testmecnn_14,
                   MCECC.hora_15, MCECC.1q_consumo_15, MCECC.2q_consumo_15, MCECC.3q_consumo_15, MCECC.4q_consumo_15, MCECC.substatus_15, MCECC.testmenn_15, MCECC.testmecnn_15,
                   MCECC.hora_16, MCECC.1q_consumo_16, MCECC.2q_consumo_16, MCECC.3q_consumo_16, MCECC.4q_consumo_16, MCECC.substatus_16, MCECC.testmenn_16, MCECC.testmecnn_16,
                   MCECC.hora_17, MCECC.1q_consumo_17, MCECC.2q_consumo_17, MCECC.3q_consumo_17, MCECC.4q_consumo_17, MCECC.substatus_17, MCECC.testmenn_17, MCECC.testmecnn_17,
                   MCECC.hora_18, MCECC.1q_consumo_18, MCECC.2q_consumo_18, MCECC.3q_consumo_18, MCECC.4q_consumo_18, MCECC.substatus_18, MCECC.testmenn_18, MCECC.testmecnn_18,
                   MCECC.hora_19, MCECC.1q_consumo_19, MCECC.2q_consumo_19, MCECC.3q_consumo_19, MCECC.4q_consumo_19, MCECC.substatus_19, MCECC.testmenn_19, MCECC.testmecnn_19,
                   MCECC.hora_20, MCECC.1q_consumo_20, MCECC.2q_consumo_20, MCECC.3q_consumo_20, MCECC.4q_consumo_20, MCECC.substatus_20, MCECC.testmenn_20, MCECC.testmecnn_20,
                   MCECC.hora_21, MCECC.1q_consumo_21, MCECC.2q_consumo_21, MCECC.3q_consumo_21, MCECC.4q_consumo_21, MCECC.substatus_21, MCECC.testmenn_21, MCECC.testmecnn_21,
                   MCECC.hora_22, MCECC.1q_consumo_22, MCECC.2q_consumo_22, MCECC.3q_consumo_22, MCECC.4q_consumo_22, MCECC.substatus_22, MCECC.testmenn_22, MCECC.testmecnn_22,
                   MCECC.hora_23, MCECC.1q_consumo_23, MCECC.2q_consumo_23, MCECC.3q_consumo_23, MCECC.4q_consumo_23, MCECC.substatus_23, MCECC.testmenn_23, MCECC.testmecnn_23,
                   MCECC.hora_24, MCECC.1q_consumo_24, MCECC.2q_consumo_24, MCECC.3q_consumo_24, MCECC.4q_consumo_24, MCECC.substatus_24, MCECC.testmenn_24, MCECC.testmecnn_24,
                   MCECC.hora_25, MCECC.1q_consumo_25, MCECC.2q_consumo_25, MCECC.3q_consumo_25, MCECC.4q_consumo_25, MCECC.substatus_25, MCECC.testmenn_25, MCECC.testmecnn_25
           FROM MCECC JOIN LRDATES
           ON MCECC.cupsree = LRDATES.cupsree AND MCECC.fapexpd = LRDATES.fapexpd AND MCECC.flectreg BETWEEN LRDATES.ldate AND LRDATES.rdate
           """)

      println("Persistiendo mcecclr")
      mcecclr.persist(nivel)
      println("Mostrando mcecclr")
      println("\nNúmero de registros de mcecclr = "+mcecclr.count())
      mcecclr.show(5,truncate = false)
      mcecc.unpersist()
      lrdates.unpersist()
      mcecclr.createOrReplaceTempView("MCECCLR")

      val li = sql(
        """
            SELECT DISTINCT
           cupsree, ccontrat, fpsercon, ffinvesu, fapexpd, fciexped,
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
           FROM MCECCLR""")

      println("Persistiendo Dataset Lecturas Irregularidad Industrial")
      li.persist(nivel)
      println("Mostrando Dataset Lecturas Irregularidad Industrial")
      println("\nNúmero de registros de Dataset Lecturas Irregularidad Industrial = "+li.count())
      li.show(5,truncate = false)

      println("\nNúmero de registros de Dataset Lecturas Irregularidad Industrial sin duplicados = "+li.dropDuplicates().count())

      mcecclr.unpersist()

      println("Guardando Dataset Lecturas Irregularidad Industrial")
      li.coalesce(1).write.option("header", "true").save(TabPaths.prefix_datasets + "lecturasIrregularidadIndustrial")
      println("Parquet Dataset Lecturas Irregularidad Industrial Guardada")

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}






