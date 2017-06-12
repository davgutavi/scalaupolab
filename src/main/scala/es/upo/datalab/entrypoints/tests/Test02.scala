package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 12/06/17.
  */
object Test02 {


  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val n = 20

      val lecturasIrregularidad = LoadTableParquet.loadTable(TabPaths.maestroContratosClientes)

      lecturasIrregularidad.createOrReplaceTempView("MaestroContratosClientes")

            val df_16 = LoadTableParquet.loadTable(TabPaths.TAB_16)
            df_16.persist(nivel)
            df_16.createOrReplaceTempView("Expedientes")


     val mcc = sql("""SELECT MaestroContratosClientes.origen, MaestroContratosClientes.cptocred, MaestroContratosClientes.cfinca, MaestroContratosClientes.cptoserv, MaestroContratosClientes.cderind, MaestroContratosClientes.cupsree, MaestroContratosClientes.ccounips, MaestroContratosClientes.cupsree2,
            MaestroContratosClientes.cpuntmed, MaestroContratosClientes.tpuntmed, MaestroContratosClientes.vparsist, MaestroContratosClientes.cemptitu, MaestroContratosClientes.ccontrat, MaestroContratosClientes.cnumscct, MaestroContratosClientes.fpsercon, MaestroContratosClientes.ffinvesu,
            MaestroContratosClientes.ccliente, MaestroContratosClientes.fechamov, MaestroContratosClientes.tindfiju, MaestroContratosClientes.cnifdnic, MaestroContratosClientes.dapersoc, MaestroContratosClientes.dnombcli,
      Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
      Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie,Expedientes.expclass, Expedientes.testexpe,
      Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
      FROM MaestroContratosClientes JOIN Expedientes
        ON MaestroContratosClientes.origen=Expedientes.origen AND MaestroContratosClientes.cfinca=Expedientes.cfinca AND
        MaestroContratosClientes.cptoserv=Expedientes.cptoserv AND MaestroContratosClientes.cderind=Expedientes.cderind AND fpsercon<=fapexpd""")

      mcc.createOrReplaceTempView("MaestroContratosClientesExpedientes")

//            val aux1 = sql("""SELECT cupsree, count(DISTINCT ccontrat) AS c1 , count(DISTINCT cnifdnic) AS c2 FROM MaestroContratosClientesExpedientes
//
//        GROUP BY cupsree
//
//        HAVING c1>2 AND c2>2 """)
//
//            aux1.show(100,truncate = false)

//      val aux1 = sql("""SELECT DISTINCT ccontrat,cnumscct,cupsree, fpsercon,ffinvesu,fapexpd,fciexped    FROM MaestroContratosClientesExpedientes
//
//        WHERE  cnifdnic= "B61502035" ORDER BY ccontrat,cnumscct   """)
//
//      println("Registros = "+ aux1.count())
//
      //      aux1.show(100,truncate = false)
//
//      val aux2 = sql("""SELECT DISTINCT cupsree, ccontrat, cnumscct, fapexpd, fciexped    FROM MaestroContratosClientesExpedientes
//
//        WHERE  cnifdnic= "B61502035" ORDER BY  cupsree, ccontrat, cnumscct  """)
//
//      println("Registros = "+ aux2.count())
//
//      aux2.show(100,truncate = false)


      val aux3 = sql("""SELECT DISTINCT cnifdnic, cupsree , ccontrat, cnumscct, fpsercon, ffinvesu,fapexpd, fciexped  FROM MaestroContratosClientesExpedientes
                  WHERE cupsree="ES0031406188447002FB0F"

                  ORDER BY cnifdnic, ccontrat, cnumscct


                      """)

      println("Registros = "+ aux3.count())

      aux3.show(100,truncate = false)


      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }


}
