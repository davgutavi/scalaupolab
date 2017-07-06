package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object Test1 {

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

    val df_05A = LoadTableParquet.loadTable(TabPaths.TAB_05A)
    df_05A.persist(nivel)
    df_05A.createOrReplaceTempView("TAB_05A")

    val tab05a_aux = sql(
      """
                SELECT DISTINCT origen, cemptitu, ccontrat, faltacon, fbajacon, fpsercon, ffinvesu FROM TAB_05A WHERE ffinvesu > fbajacon
              """)

    tab05a_aux.persist(nivel)
    tab05a_aux.createOrReplaceTempView("tab05a_aux")

    val aux = sql(
      """
              SELECT * FROM mce_aux JOIN tab05a_aux
              ON mce_aux.origen = tab05a_aux.origen AND mce_aux.cemptitu = tab05a_aux.cemptitu AND mce_aux.ccontrat = tab05a_aux.ccontrat
              """)

//    val mce_aux2 = sql(
//      """
//                SELECT * FROM mce_aux WHERE  ffinvesu <> "9999−12−31"
//      """)
//
//    mce_aux2.show(200,truncate=false)
//    println("Este valor era 122.312, y ahora es "+mce_aux2.count())
    println("Los registros bajan de 130.462 a "+aux.count())

    val aux1 = sql("""SELECT cupsree, origen, cemptitu, faltacon, fbajacon, fpsercon, ffinvesu count(DISTINCT ccontrat) AS c1 FROM aux
       GROUP BY cupsree, c1
       HAVING ffinvesu>fbajacon """)

    println("Total de registros sin contar con los secuenciales "+aux1.count())

    mce_aux.unpersist()
    df_05A.unpersist()
    df_mce.unpersist()
    tab05a_aux.unpersist()
    println("DONE!")


    SparkSessionUtils.sc.stop()

    }








}
