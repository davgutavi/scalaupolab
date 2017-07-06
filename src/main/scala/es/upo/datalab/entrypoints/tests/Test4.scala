package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object Test4 {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

      val df_mce = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientes)
      println("Persistiendo MCE...")
      df_mce.persist(nivel)
      df_mce.createOrReplaceTempView("MCE")

      val df_05A = LoadTableParquet.loadTable(TabPaths.TAB_05A)
      println("Persistiendo Contratación")
      df_05A.persist(nivel)
      df_05A.createOrReplaceTempView("TAB_05A")

    //Join de las tablas MCE y CONTRATACIÓN
    println("Haciendo el JOIN...")
    val mce_05A = sql(
      """
                SELECT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cnumscct, MCE.cptoserv, MCE.cderind, MCE.cupsree, MCE.ccounips, MCE.cupsree2,
                MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu, MCE.ccontrat, MCE.fpsercon, MCE.ffinvesu, TAB_05A.ffinvesu AS ffinvesu2, MCE.csecexpe,
                MCE.fapexpd, MCE.finifran, MCE.ffinfran, MCE.anomalia, MCE.irregularidad, MCE.venacord, MCE.vennofai, MCE.torigexp,
                MCE.texpedie, MCE.expclass, MCE.testexpe, MCE.fnormali, MCE.cplan, MCE.ccampa, MCE.cempresa, MCE.fciexped,
                TAB_05A.tcontrat, TAB_05A.testcont, TAB_05A.ctarifa, TAB_05A.cnae, TAB_05A.fsolicon, TAB_05A.faltacon, TAB_05A.fbajacon,
                TAB_05A.fvtocon, TAB_05A.csubsect, TAB_05A.vnumcerr, TAB_05A.tubiapar, TAB_05A.csectmie, TAB_05A.vpotads1, TAB_05A.fadscri1,
                TAB_05A.fvalads1, TAB_05A.vpotads2, TAB_05A.fadscri2, TAB_05A.fvalads2, TAB_05A.vpotads3, TAB_05A.fadscri3, TAB_05A.fvalads3,
                TAB_05A.tconcort, TAB_05A.testader, TAB_05A.vpotppal, TAB_05A.potencia_1, TAB_05A.potencia_2, TAB_05A.potencia_3,
                TAB_05A.potencia_4, TAB_05A.potencia_5, TAB_05A.potencia_6, TAB_05A.empresa_instaladora, TAB_05A.empresa_instaladora,
                TAB_05A.instalador, TAB_05A.fboletin, TAB_05A.tension, TAB_05A.fases, TAB_05A.vpmaxbie, TAB_05A.emergencia,
                TAB_05A.complemento_perdida, TAB_05A.cnumscct AS cnumscct05
                FROM MCE JOIN TAB_05A
                ON MCE.origen = TAB_05A.origen AND MCE.cemptitu = TAB_05A.cemptitu AND MCE.ccontrat = TAB_05A.ccontrat
                AND MCE.cnumscct = TAB_05A.cnumscct
                WHERE (MCE.fpsercon<MCE.fapexpd AND (MCE.ffinvesu = '9999-12-31' AND MCE.fapexpd <= TAB_05A.fbajacon)
                OR (MCE.ffinvesu != '9999-12-31' AND MCE.fapexpd <= MCE.ffinvesu))
              """)

    //WHERE MCE.ffinvesu = TAB_05A.ffinvesu OR MCE.ffinvesu = TAB_05A.fbajacon
    println("Persistiendo MCE_05A...")
    mce_05A.persist(nivel)
    mce_05A.createOrReplaceTempView("MCE_05A")
    println("Registros que cumplen la guarda de fechas...", +mce_05A.count() )
    //mce_05A.show(20,truncate=false)
    df_mce.unpersist()
    df_05A.unpersist()
    //mce_05A.show(200,truncate=false)

    val mce_05A_aux = sql(
      """
                SELECT MCE_05A.cupsree, MCE_05A.ccontrat, MCE_05A.cnumscct, MCE_05A.fpsercon, MCE_05A.ffinvesu, MCE_05A.fapexpd,
                MCE_05A.fnormali, MCE_05A.ffinvesu, MCE_05A.ffinvesu2, MCE_05A.faltacon, MCE_05A.fbajacon
                FROM MCE_05A
                GROUP BY MCE_05A.cupsree, MCE_05A.ccontrat, MCE_05A.cnumscct, MCE_05A.fpsercon, MCE_05A.ffinvesu, MCE_05A.fapexpd,
                MCE_05A.fnormali, MCE_05A.faltacon, MCE_05A.fbajacon, MCE_05A.ffinvesu2
                HAVING MCE_05A.ffinvesu = MCE_05A.ffinvesu2
              """)
    //HAVING MCE_05A.ffinvesu = (SELECT MIN(MCE_05A.ffinvesu) FROM MCE_05A)
    //WHERE MCE.ffinvesu = TAB_05A.ffinvesu OR MCE.ffinvesu = TAB_05A.fbajacon

    println("Persistiendo MCE_05A_AUX...")
    mce_05A_aux.persist(nivel)
    mce_05A_aux.createOrReplaceTempView("mce_05A_AUX")
    println("Registros que cumplen la guarda de fechas eliminando duplicados...", +mce_05A_aux.count())
    mce_05A.unpersist()
    mce_05A_aux.show(20,truncate=false)
    //println("Registros : "+mce_05A.count())
    //println("Registros totales de MCE "+df_mce.count())
    //println("Registros totales de Contratación "+df_05A.count())
    df_mce.unpersist()
    df_05A.unpersist()
    //println("Registros totales del join entre MCE y Contratación "+mce_05A.count())



    mce_05A.unpersist()
    println("DONE!")


    SparkSessionUtils.sc.stop()

    }


}
