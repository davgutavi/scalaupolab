package es.upo.datalab.entrypoints.queries

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 21/04/17.
  */
object MaestroContratos {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

//      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu FROM MaestroContratos""").show(20,false)
//
//      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, ccounips, cupsree2, cpuntmed FROM MaestroContratos""").show(20,false)
//
//      sql("""SELECT ccontrat, cnumscct, cfinca, cptocred, cptoserv, cderind FROM MaestroContratos""").show(20,false)
//
//      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, ccounips, cupsree2, cpuntmed, cfinca, cptocred, cptoserv, cderind FROM MaestroContratos""").show(20,false)

//      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, ccounips, cupsree2, cpuntmed, cfinca, cptocred, cptoserv, cderind, origen, cptocred, cpuntmed, tpuntmed, vparsist, cemptitu FROM MaestroContratos""").show(20,false)


      sql(
        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
          FROM MaestroContratos
          WHERE  ccontrat = "140049199150"
          ORDER BY cnumscct
           """).show(20,false)

      sql(
        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
          FROM MaestroContratos
          WHERE  ccontrat = "140049198347"
          ORDER BY cnumscct
           """).show(20,false)

      sql(
        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
          FROM MaestroContratos
          WHERE  ccontrat = "140049199735"
          ORDER BY cnumscct
           """).show(20,false)


      sql(
        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
          FROM MaestroContratos
          WHERE  ccontrat = "140049199847"
          ORDER BY cnumscct
           """).show(20,false)

      sql(
        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
          FROM MaestroContratos
          WHERE  ccontrat = "310000000018"
          ORDER BY cnumscct
           """).show(20,false)






//      df_00C.show(100,false)
//      df_00C.coalesce(1).limit(100).write.option("delimiter",";").option("header","true").csv(TabPaths.root+"datasets/sTAB_00C")
//
//      val q1 = sql("""SELECT origen, cemptitu, ccontrat, cnumscct FROM MaestroContratos""")
//
//      df_00C.unpersist()
//
//      q1.persist(nivel)
//
//      val w1 = q1.count()
//
//      println("origen, cemptitu, ccontrat, cnumscct")
//      q1.show(5,false)
//
//      val q2 = q1.dropDuplicates()
//
//      q1.unpersist()
//
//      q2.persist(nivel)
//
//      val w2 = q2.count()
//
//      println("origen, cemptitu, ccontrat, cnumscct sin repetir")
//      q2.show(5,false)
//
//      q2.unpersist()
//
//      println("\n\nRegistros = "+w1)
//      println("Registros sin repetir = "+w2)
//      println("Diferencia = "+(w1-w2))





    }

    SparkSessionUtils.sc.stop()

  }

}
