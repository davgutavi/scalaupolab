package es.upo.datalab.entrypoints.joins

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 25/04/17.
  */
object PersonaClienteContratoSecuencialExpediente {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, true)
      df_05C.persist(nivel)
      df_05C.createOrReplaceTempView("Clientes")

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")
//
//      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
//      df_16.persist(nivel)
//      df_16.createOrReplaceTempView("Expedientes")


//      sql("""SELECT cnifdnic, count(ccliente) as sum FROM Clientes GROUP BY cnifdnic HAVING sum > 1 ORDER BY sum DESC """).show(20,truncate = false)
//      sql("""SELECT ccliente, ccontrat, cnumscct, fechamov FROM Clientes WHERE cnifdnic = "P0801900B" ORDER BY ccontrat, cnumscct, ccliente,fechamov""").show(1000,truncate = false)


      val maestroContratosClientes = sql(

        """
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed,
          MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu, Clientes.ccliente, Clientes.fechamov, Clientes.tindfiju,
          Clientes.cnifdnic, Clientes.dapersoc, Clientes.dnombcli
          FROM Clientes JOIN MaestroContratos
          ON Clientes.origen=MaestroContratos.origen AND Clientes.cemptitu=MaestroContratos.cemptitu AND Clientes.ccontrat=MaestroContratos.ccontrat AND Clientes.cnumscct=MaestroContratos.cnumscct

        """
      )

      maestroContratosClientes.persist(nivel)
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")


//      sql("""SELECT cnifdnic, count(cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY cnifdnic HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """).show(20,truncate = false)
//      sql("""SELECT cnifdnic, cfinca FROM MaestroContratosClientes WHERE cnifdnic = "P0801900B" ORDER BY cfinca""").show(1000,truncate = false)

//      sql("""SELECT ccliente, count(cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY ccliente HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """).show(20,truncate = false)
//      sql("""SELECT cnifdnic, ccliente, cfinca FROM MaestroContratosClientes WHERE ccliente = "100403491" ORDER BY cfinca""").show(1000,truncate = false)

            sql("""SELECT ccontrat, cnumscct, count(cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY ccontrat, cnumscct HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """).show(20,truncate = false)
//            sql("""SELECT ccontrat, cnumscct, cfinca FROM MaestroContratosClientes WHERE ccontrat = "170053113318" AND cnumscct = "002" ORDER BY cfinca""").show(1000,truncate = false)

//
//      df_05C.unpersist()
//      df_00C.unpersist()
//
//

//      val maestroContratosClientesExpedientes = sql(
//
//        """
//          SELECT MaestroContratosClientess.origen, MaestroContratosClientess.cptocred, MaestroContratosClientess.cfinca, MaestroContratosClientess.cptoserv, MaestroContratosClientess.cderind, MaestroContratosClientess.cupsree,
//          MaestroContratosClientess.cpuntmed, MaestroContratosClientess.tpuntmed, MaestroContratosClientess.vparsist, MaestroContratosClientess.cemptitu, MaestroContratosClientess.ccontrat, MaestroContratosClientess.cnumscct,
//          MaestroContratosClientess.fpsercon, MaestroContratosClientess.ffinvesu, MaestroContratosClientess.ccliente, MaestroContratosClientess.fechamov, MaestroContratosClientess.tindfiju, MaestroContratosClientess.cnifdnic,
//          MaestroContratosClientess.dapersoc, MaestroContratosClientess.dnombcli, Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
//          Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,
//          Expedientes.fciexped
//          FROM MaestroContratosClientess JOIN Expedientes
//          ON MaestroContratosClientess.origen=Expedientes.origen AND MaestroContratosClientess.cemptitu=Expedientes.cemptitu AND MaestroContratosClientess.cfinca=Expedientes.cfinca AND MaestroContratosClientess.cptoserv=Expedientes.cptoserv
//        """
//      )
//
//     df_16.unpersist()
//
//      maestroContratosClientesExpedientes.persist(nivel)


    }

    SparkSessionUtils.sc.stop()

  }

}
