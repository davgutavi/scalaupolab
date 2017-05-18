package es.upo.datalab.entrypoints.joins


import es.upo.datalab.utilities._
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 28/04/17.
  */
object McontratosExpedientes {

  def main(args: Array[String]): Unit = {

  val nivel = StorageLevel.MEMORY_ONLY

  val sqlContext = SparkSessionUtils.sqlContext

  import sqlContext._

  TimingUtils.time {


    val maestroContratosExpedientes = SparkSessionUtils.sparkSession.read.load(TabPaths.maestroContratosExpedientes)
    maestroContratosExpedientes.persist(nivel)
    maestroContratosExpedientes.createOrReplaceTempView("MaestroContratosExpedientes")

    println("Registros MaestroContratosExpedientes = "+maestroContratosExpedientes.count())

    sql(
      """SELECT cupsree, cpuntmed, tpuntmed, ccontrat, cnumscct, fpsercon, ffinvesu, count(DISTINCT *) AS sum
         FROM MaestroContratosExpedientes
         GROUP BY cupsree, cpuntmed, tpuntmed, ccontrat, cnumscct, fpsercon, ffinvesu
       """).show(10,truncate = false)

    sql(
      """SELECT *  FROM MaestroContratosExpedientes
        WHERE cupsree = "ES0031405440377012PF0F" AND cpuntmed = "CZZ544037701203" AND tpuntmed = 5 AND ccontrat = "130002533593" AND cnumscct = "001" AND fpsercon = "2009-07-01" AND ffinvesu = "2010-03-31"
      """).show(10,truncate = false)


  }

  SparkSessionUtils.sc.stop()



}

}

//    MaestroContratosExpedientes con enlace: .origen,cfinca,cptoserv,cderind
//    val maestroContratosExpedientes_2 = sql(//
//      """
//          SELECT  MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips,
//          MaestroContratos.cupsree2,MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct,
//          MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
//          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,Expedientes.venacord, Expedientes.vennofai,
//          Expedientes.torigexp,Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa,Expedientes.cempresa, Expedientes.fciexped
//          FROM MaestroContratos JOIN Expedientes
//          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind
//        """    )
//    maestroContratosExpedientes_2.persist(nivel)
//    maestroContratosExpedientes_2.createOrReplaceTempView("MaestroContratosExpedientes")
//    val maestroContratosExpedientes_2_s = maestroContratosExpedientes_2.dropDuplicates()
//    val c2 = maestroContratosExpedientes_2.count()
//    val c2_s = maestroContratosExpedientes_2_s.count()
//    println("Registros MaestroContratosExpedientes1 = "+c2)
//    println("Registros MaestroContratosExpedientes1s = "+c2_s)
//    println("Diferencia = "+(c2-c2_s))