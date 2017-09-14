package es.upo.datalab.entrypoints.procesos

import es.upo.datalab.utilities._
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object Tipo123 {




  def main(args: Array[String]): Unit = {


    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql

    import sqlContext._

    val df_00C = LoadTableParquet.loadTable(TabPaths.TAB00C)
    //df_00C.persist(nivel)
    df_00C.createOrReplaceTempView("MC")

    val df_16 = LoadTableParquet.loadTable(TabPaths.TAB16)
    //df_16.persist(nivel)
    df_16.createOrReplaceTempView("E")

    //      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB01)
    //      df_01.persist(nivel)
    //      df_01.createOrReplaceTempView("CC")


    ///************PASO 1.1: MC U E ==>  eliminar duplicados por fapexpd >= fpsercon y fapexpd <= ffinvesu y por eliminación del secuencial de contrato del join

    println("join")

    val mce = sql(
      """
            SELECT DISTINCT MC.origen, MC.cptocred, MC.cfinca, MC.cnumscct, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips, MC.cupsree2,
            MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu, MC.ccontrat, MC.fpsercon, MC.ffinvesu, E.csecexpe,
            E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp,
            E.texpedie, E.expclass, E.testexpe, E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped
                FROM MC JOIN E
                ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind
                AND MC.cupsree = 'ES0031405193447001KC0F'
              """)
    //
    println("Contando registros de la tabla tras el JOIN entre MaestroContratos y Expedientes")
    //mce.persist(nivel)
    df_00C.unpersist()
    df_16.unpersist()
    mce.createOrReplaceTempView("MCE")
    //println("El número de registros es de: "+mce.count())


    val df_05A = LoadTableParquet.loadTable(TabPaths.TAB05A)
    //df_16.persist(nivel)
    df_05A.createOrReplaceTempView("TAB_05A")

    val mce_05A = sql(
      """
                      SELECT DISTINCT MCE.origen, MCE.cptocred, MCE.cfinca, MCE.cnumscct, MCE.cptoserv, MCE.cderind, MCE.cupsree, MCE.ccounips, MCE.cupsree2,
                      MCE.cpuntmed, MCE.tpuntmed, MCE.vparsist, MCE.cemptitu, MCE.ccontrat, MCE.fpsercon, MCE.ffinvesu, MCE.csecexpe,
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
                      OR (MCE.ffinvesu != '9999-12-31' AND MCE.fapexpd <= MCE.ffinvesu)) AND MCE.cnumscct!='000' AND MCE.ffinvesu == TAB_05A.ffinvesu
                      ORDER BY MCE.fapexpd
                    """)

    println("Contando registros de la tabla tras el JOIN entre MaestroContratosExpedientes y Contratación")
    //mce.persist(nivel)
    // mce_05A.createOrReplaceTempView("MCE")
    //println("El número de registros es de: "+mce_05A.count())
    mce_05A.show(20, truncate = false)

    println("DONE!")

    SparkSessionUtils.session.stop()

  }








}
