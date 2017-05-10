package es.upo.datalab.entrypoints.joins


import es.upo.datalab.utilities.{LoadTable, SparkSessionUtils, TabPaths, TimingUtils}
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

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("Expedientes")


      val q1 = sql("""SELECT cnifdnic, count(DISTINCT ccliente) as sumCliente FROM Clientes GROUP BY cnifdnic HAVING sumCliente > 1 ORDER BY sumCliente DESC """)
      println("Número de ccliente por cada cnifdnif = "+q1.count()+" registros")
      q1.show(11,false)
      val q2 = sql("""SELECT DISTINCT cnifdnic, ccliente FROM Clientes WHERE cnifdnic = "A28354520" ORDER BY ccliente""")
      println("ccliente para cnifdnic = A28354520  = "+q2.count()+" registros")
      q2.show(10,false)

      val maestroContratosClientes = sql(

        """
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips, MaestroContratos.cupsree2,
            MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct,
            MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
            Clientes.ccliente, Clientes.fechamov,Clientes.tindfiju, Clientes.cnifdnic, Clientes.dapersoc, Clientes.dnombcli
          FROM Clientes JOIN MaestroContratos
          ON Clientes.origen=MaestroContratos.origen AND Clientes.cemptitu=MaestroContratos.cemptitu AND Clientes.ccontrat=MaestroContratos.ccontrat AND Clientes.cnumscct=MaestroContratos.cnumscct

        """
      )

      maestroContratosClientes.persist(nivel)
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")


      val q3 = sql("""SELECT cnifdnic, count(DISTINCT cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY cnifdnic HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """)
      println("Número de cfinca por cada cnifdnic = "+q3.count()+" registros")
      q3.show(10,false)

      val q4 = sql("""SELECT DISTINCT cnifdnic, cfinca FROM MaestroContratosClientes WHERE cnifdnic = "P0801900B" ORDER BY cfinca""")
      println("cfinca para cnifdnic = P0801900B  = "+q4.count()+" registros")
      q4.show(10,false)


      val q5 = sql("""SELECT ccliente, count(DISTINCT cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY ccliente HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """)
      println("cfinca por cada ccliente = "+q5.count()+" registros")
      q5.show(10,false)

      val q6 = sql("""SELECT DISTINCT ccliente, cfinca FROM MaestroContratosClientes WHERE ccliente = "100403491" ORDER BY cfinca""")
      println("cfinca para ccliente = 100403491 = "+q6.count()+" registros")
      q6.show(10,false)

      val q7 = sql("""SELECT ccontrat, cnumscct, count(DISTINCT cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY ccontrat, cnumscct HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """)
      println("cfinca por cada ccontrat y cnumscct = "+q7.count()+" registros")
      q7.show(10,false)

      val q8 = sql("""SELECT DISTINCT ccontrat, cnumscct, cfinca FROM MaestroContratosClientes WHERE ccontrat = "140050102868" AND cnumscct = "001" ORDER BY cfinca""")
      println("cfinca para ccontrat = 140050102868 y cnumscct = 001 = "+q8.count()+" registros")
      q8.show(10,false)

      df_05C.unpersist()
      df_00C.unpersist()


      val maestroContratosClientesExpedientes = sql(

        """
          SELECT MaestroContratosClientes.origen, MaestroContratosClientes.cptocred, MaestroContratosClientes.cfinca, MaestroContratosClientes.cptoserv, MaestroContratosClientes.cderind, MaestroContratosClientes.cupsree, MaestroContratosClientes.ccounips, MaestroContratosClientes.cupsree2,
            MaestroContratosClientes.cpuntmed, MaestroContratosClientes.tpuntmed, MaestroContratosClientes.vparsist, MaestroContratosClientes.cemptitu, MaestroContratosClientes.ccontrat, MaestroContratosClientes.cnumscct,
            MaestroContratosClientes.fpsercon, MaestroContratosClientes.ffinvesu,
            MaestroContratosClientes.ccliente, MaestroContratosClientes.fechamov, MaestroContratosClientes.tindfiju, MaestroContratosClientes.cnifdnic,
          MaestroContratosClientes.dapersoc, MaestroContratosClientes.dnombcli, Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
          Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,
          Expedientes.fciexped
          FROM MaestroContratosClientes JOIN Expedientes
          ON MaestroContratosClientes.origen=Expedientes.origen AND MaestroContratosClientes.cemptitu=Expedientes.cemptitu AND MaestroContratosClientes.cfinca=Expedientes.cfinca AND MaestroContratosClientes.cptoserv=Expedientes.cptoserv
        """
      )

      df_16.unpersist()

      maestroContratosClientesExpedientes.persist(nivel)
      maestroContratosClientesExpedientes.createOrReplaceTempView("MaestroContratosClientesExpedientes")

      val q9 = sql(
        """SELECT ccontrat, cnumscct, COUNT(DISTINCT csecexpe) AS SumCsecexpe, COUNT(DISTINCT finifran) AS SumFinifran, COUNT(DISTINCT ffinfran) AS SumFfinfran, COUNT(DISTINCT fapexpd) AS SumFapexpd,
           COUNT(DISTINCT fciexped) AS SumFciexped
            FROM MaestroContratosClientesExpedientes
             GROUP BY ccontrat, cnumscct
             HAVING (SumCsecexpe > 1 AND SumFinifran > 1 AND SumFfinfran > 1 AND SumFapexpd > 1 AND SumFciexped > 1 )
            ORDER BY ccontrat DESC, cnumscct DESC
              """)

      println("csecexpe, finifran, ffinfran, fapexpd por cada ccontrat y cnumscct = "+q9.count()+" registros")
      q9.show(10,false)

      val q10 = sql(
        """SELECT ccontrat, cnumscct, csecexpe, finifran, ffinfran, fapexpd, fciexped  FROM MaestroContratosClientesExpedientes WHERE ccontrat = "380046773806" AND cnumscct = "011" AND
          cfinca ="6173636"""")
      println("csecexpe, finifran, ffinfran, fapexpd para ccontrat = 380046773806 y cnumscct = 011 = "+q10.count()+" registros")
      q10.show(10,false)

    }

    SparkSessionUtils.sc.stop()

  }

}
