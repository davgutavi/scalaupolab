package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object Test3 {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

      val df_mce = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientes)
      df_mce.persist(nivel)
      df_mce.createOrReplaceTempView("MCE")

    val mce_aux = sql(
      """
                SELECT DISTINCT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cptoserv, MCE.cupsree, MCE.ccounips, MCE.cupsree2,
                MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu, MCE.ccontrat, MCE.fpsercon, MCE.ffinvesu, MCE.csecexpe, MCE.fapexpd,
                MCE.finifran, MCE.ffinfran, MCE.anomalia, MCE.irregularidad, MCE.venacord, MCE.vennofai, MCE.torigexp,
                MCE.texpedie, MCE.expclass, MCE.testexpe, MCE.fnormali, MCE.cplan, MCE.ccampa, MCE.cempresa, MCE.fciexped
                FROM MCE WHERE MCE.fapexpd >= MCE.fpsercon AND MCE.fapexpd <= MCE.ffinvesu
              """)


    mce_aux.persist(nivel)
    mce_aux.createOrReplaceTempView("mce_aux")
    df_mce.unpersist()
    println("Registros válidos de MCE, deberían ser 130.462, y son "+mce_aux.count())

//       val mce_aux2 = sql(
//      """
//                SELECT * FROM mce_aux WHERE ffinvesu != "9999-12-31" OR fpsercon != "0002-11-30"
//      """)
//    """
//                SELECT * FROM mce_aux WHERE ffinvesu = "9999-12-31"
//      """)
//
//    val mce_aux3 = sql(
//
//      """
//                SELECT * FROM mce_aux WHERE fpsercon = "0002-11-30"
//      """)
//
//    val mce_aux4 = sql(
//
//      """
//                SELECT * FROM mce_aux WHERE fpsercon = "0002-11-30" AND ffinvesu = "9999-12-31"
//      """)
//
//
//    println("El número de registros con ffinvesu = 9999-12-31 es de "+mce_aux2.count())
//    println("El número de registros con ffinvesu = 0002-11-30 es de "+mce_aux3.count())
//    println("El número de registros totales infinitos es de "+mce_aux4.count())


    //mce_aux2.show(10,truncate=false)
    //println("Este valor era 122.312, y ahora es "+mce_aux2.count())


    mce_aux.unpersist()
    println("DONE!")


    SparkSessionUtils.sc.stop()

    }








}
