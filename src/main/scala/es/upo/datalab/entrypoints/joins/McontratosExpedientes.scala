package es.upo.datalab.entrypoints.joins


import es.upo.datalab.utilities.{LoadTable, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 28/04/17.
  */
object McontratosExpedientes {

  def main(args: Array[String]): Unit = {

  val nivel = StorageLevel.MEMORY_AND_DISK

  val sqlContext = SparkSessionUtils.sqlContext

  import sqlContext._

  TimingUtils.time {


    val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
    df_00C.persist(nivel)
    df_00C.createOrReplaceTempView("MaestroContratos")

    val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
    df_16.persist(nivel)
    df_16.createOrReplaceTempView("Expedientes")


    val maestroContratosExpedientes_1 = sql(

      """
          SELECT  MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips, MaestroContratos.cupsree2,
           MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct,
           MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp,
          Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
          FROM MaestroContratos JOIN Expedientes
          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cemptitu=Expedientes.cemptitu AND MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv
        """
    )



    maestroContratosExpedientes_1.persist(nivel)
    maestroContratosExpedientes_1.createOrReplaceTempView("MaestroContratosExpedientes1")

//    val maestroContratosExpedientes_1_s = maestroContratosExpedientes_1.dropDuplicates()
    val c1 = maestroContratosExpedientes_1.count()
//    val c1_s = maestroContratosExpedientes_1_s.count()

    println("Registros MaestroContratosExpedientes1 = "+c1)
//    println("Registros MaestroContratosExpedientes1s = "+c1_s)
//    println("Diferencia = "+(c1-c1_s))

//
//    sql(
//      """SELECT ccontrat, cnumscct, cfinca, COUNT(DISTINCT csecexpe) AS SumCsecexpe, COUNT(DISTINCT fapexpd) AS SumFapexpd  FROM MaestroContratosExpedientes1
//        GROUP BY ccontrat, cnumscct, cfinca
//        HAVING (SumCsecexpe > 1 AND SumFapexpd > 1)
//        ORDER BY ccontrat DESC, cnumscct DESC, cfinca DESC
//      """).show(20,false)

    sql(
      """SELECT *  FROM MaestroContratosExpedientes1
        WHERE ccontrat = "380046773806" AND cnumscct = "011" AND cfinca ="6173636"
      """).show(20,false)




    maestroContratosExpedientes_1.unpersist()


//    val maestroContratosExpedientes_2 = sql(
//
//      """
//          SELECT  MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips, MaestroContratos.cupsree2,
//           MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct,
//           MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
//          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp,
//          Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
//          FROM MaestroContratos JOIN Expedientes
//          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind AND MaestroContratos.fpsercon=Expedientes.fapexpd
//        """
//    )
//
//    maestroContratosExpedientes_2.persist(nivel)
//    maestroContratosExpedientes_2.createOrReplaceTempView("MaestroContratosExpedientes2")
//
//    maestroContratosExpedientes_2.show(100,false)
////    val maestroContratosExpedientes_2_s = maestroContratosExpedientes_2.dropDuplicates()
//    val c2 = maestroContratosExpedientes_2.count()
////    val c2_s = maestroContratosExpedientes_2_s.count()
//
//    println("Registros MaestroContratosExpedientes2 = "+c2)
////    println("Registros MaestroContratosExpedientes2s = "+c2_s)
////    println("Diferencia = "+(c2-c2_s))
//
//    sql(
//      """SELECT ccontrat, cnumscct, cfinca, COUNT(DISTINCT csecexpe) AS SumCsecexpe, COUNT(DISTINCT fapexpd) AS SumFapexpd  FROM MaestroContratosExpedientes2
//                 GROUP BY ccontrat, cnumscct, cfinca
//                 HAVING (SumCsecexpe > 1 AND SumFapexpd > 1)
//                 ORDER BY ccontrat DESC, cnumscct DESC, cfinca DESC
//      """).show(20,false)
//
//    maestroContratosExpedientes_2.unpersist()
//
//
//    val maestroContratosExpedientes_3 = sql(
//
//      """
//          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips, MaestroContratos.cupsree2,
//          MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct,
//          MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
//          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp,
//          Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
//          FROM MaestroContratos JOIN Expedientes
//          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind AND MaestroContratos.ffinvesu=Expedientes.fapexpd
//        """
//    )
//
//    maestroContratosExpedientes_3.persist(nivel)
//    maestroContratosExpedientes_3.createOrReplaceTempView("MaestroContratosExpedientes3")
//    maestroContratosExpedientes_3.show(100,false)
//
////    val maestroContratosExpedientes_3_s = maestroContratosExpedientes_3.dropDuplicates()
//    val c3 = maestroContratosExpedientes_3.count()
////    val c3_s = maestroContratosExpedientes_3_s.count()
//
//    println("Registros MaestroContratosExpedientes3 = "+c3)
////    println("Registros MaestroContratosExpedientes3s = "+c3_s)
////    println("Diferencia = "+(c3-c3_s))
//
//
//    sql(
//      """SELECT ccontrat, cnumscct, cfinca, COUNT(DISTINCT csecexpe) AS SumCsecexpe, COUNT(DISTINCT fapexpd) AS SumFapexpd  FROM MaestroContratosExpedientes3
//                 GROUP BY ccontrat, cnumscct, cfinca
//                 HAVING (SumCsecexpe > 1 AND SumFapexpd > 1)
//                 ORDER BY ccontrat DESC, cnumscct DESC, cfinca DESC
//      """).show(20,false)
//
//    maestroContratosExpedientes_3.unpersist()


//    val maestroContratosExpedientes_4 = sql(
//
//      """
//          SELECT  MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips, MaestroContratos.cupsree2,
//           MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct,
//           MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
//          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp,
//          Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
//          FROM MaestroContratos JOIN Expedientes
//          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv AND MaestroContratos.cderind=Expedientes.cderind
//        """
//    )
//
//    maestroContratosExpedientes_4.persist(nivel)
//    maestroContratosExpedientes_4.createOrReplaceTempView("MaestroContratosExpedientes4")
//
//
////    val maestroContratosExpedientes_4_s = maestroContratosExpedientes_4.dropDuplicates()
//    val c4 = maestroContratosExpedientes_4.count()
////    val c4_s = maestroContratosExpedientes_4_s.count()
//
//    println("Registros MaestroContratosExpedientes4 = "+c4)
////    println("Registros MaestroContratosExpedientes4s = "+c4_s)
////    println("Diferencia = "+(c4-c4_s))
//
//    sql(
//      """SELECT ccontrat, cnumscct, cfinca, COUNT(DISTINCT csecexpe) AS SumCsecexpe, COUNT(DISTINCT fapexpd) AS SumFapexpd  FROM MaestroContratosExpedientes4
//         GROUP BY ccontrat, cnumscct, cfinca
//         HAVING (SumCsecexpe > 1 AND SumFapexpd > 1)
//         ORDER BY ccontrat DESC, cnumscct DESC, cfinca DESC
//
//      """).show(20,false)
//
//    maestroContratosExpedientes_4.unpersist()


  }

  SparkSessionUtils.sc.stop()



}

}
