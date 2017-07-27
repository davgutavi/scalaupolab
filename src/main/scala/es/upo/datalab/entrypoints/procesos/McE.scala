package es.upo.datalab.entrypoints.procesos

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

object McE {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql

    val sparkSession = SparkSessionUtils.session

    import sqlContext._
    import sparkSession.implicits._
    import sqlContext.implicits._


    TimingUtils.time {


      println("Leyendo TAB00C")

      val df00C = LoadTableParquet.loadTable(TabPaths.TAB00C)
      df00C.persist(nivel)
      df00C.createOrReplaceTempView("MC")

      println("Leyendo TAB16")

      val df16 = LoadTableParquet.loadTable(TabPaths.TAB16)
      df16.persist(nivel)
      df16.createOrReplaceTempView("E")


      println("Realizando Join")

      val mce = sql ("""SELECT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat,
                               MC.cnumscct, MC.fpsercon, MC.ffinvesu, MC.contrext,
                               E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe,
                               E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped, E.cexpeind
                        FROM MC JOIN E
                        ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND MC.fpsercon<E.fapexpd AND E.fapexpd <= MC.ffinvesu""")


      println("Mostrando resultados")
      mce.show(2000,truncate = false)


      println("DONE!")




    }

    SparkSessionUtils.context.stop()


  }

}
