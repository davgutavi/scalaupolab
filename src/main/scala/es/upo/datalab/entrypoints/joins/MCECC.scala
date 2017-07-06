package es.upo.datalab.entrypoints.joins

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object MCECC {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

      //val df_mce = LoadTableParquet.loadTable(TabPaths.MCE)
      val df_mce = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientes)
      df_mce.persist(nivel)
      df_mce.createOrReplaceTempView("MCE")

      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB_01)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CC")

    //Join de las tablas MCE y CC, añadiendo el campo diferencia para saber cuál es la última lectura anterior a la fecha de apertura del expediente.
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
    mcecc.show(5,truncate = false)
    mcecc.persist(nivel)
    df_01.unpersist()
    df_mce.unpersist()
    mcecc.createOrReplaceTempView("MCECC")
    println("El total de curvas de carga es de: "+mcecc.count())

    println("DONE!")

    mcecc.write.parquet("hdfs://10.141.0.224:9000/database/endesa/datasets/MCECC")
    SparkSessionUtils.sc.stop()

    }








}
