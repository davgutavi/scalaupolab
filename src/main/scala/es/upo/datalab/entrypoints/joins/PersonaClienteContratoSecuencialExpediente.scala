package es.upo.datalab.entrypoints.joins


import es.upo.datalab.utilities._
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

//      val df_05C = LoadTableParquet.loadTable(TabPaths.TAB_05C)
//      df_05C.persist(nivel)
//      df_05C.createOrReplaceTempView("Clientes")
//
//      val df_00C = LoadTableParquet.loadTable(TabPaths.TAB_00C)
//      df_00C.persist(nivel)
//      df_00C.createOrReplaceTempView("MaestroContratos")
//
//      val df_16 =  LoadTableParquet.loadTable(TabPaths.TAB_16)
//      df_16.persist(nivel)
//      df_16.createOrReplaceTempView("Expedientes")


//      val q1 = sql("""SELECT cnifdnic, count(DISTINCT ccliente) as sumCliente FROM Clientes GROUP BY cnifdnic HAVING sumCliente > 1 ORDER BY sumCliente DESC """)
//      println("Número de ccliente por cada cnifdnif = "+q1.count()+" registros")
//      q1.show(11,truncate = false)
//      val q2 = sql("""SELECT DISTINCT cnifdnic, ccliente FROM Clientes WHERE cnifdnic = "A28354520" ORDER BY ccliente""")
//      println("ccliente para cnifdnic = A28354520  = "+q2.count()+" registros")
//      q2.show(10,truncate = false)

      val maestroContratosClientes = SparkSessionUtils.sparkSession.read.load(TabPaths.maestroContratosClientes)

      maestroContratosClientes.persist(nivel)
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")


//      val q3 = sql("""SELECT cnifdnic, count(DISTINCT cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY cnifdnic HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """)
//      println("Número de cfinca por cada cnifdnic = "+q3.count()+" registros")
//      q3.show(10,truncate = false)
//
//      val q4 = sql("""SELECT DISTINCT cnifdnic, cfinca FROM MaestroContratosClientes WHERE cnifdnic = "P0801900B" ORDER BY cfinca""")
//      println("cfinca para cnifdnic = P0801900B  = "+q4.count()+" registros")
//      q4.show(10,truncate = false)
//
//
//      val q5 = sql("""SELECT ccliente, count(DISTINCT cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY ccliente HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """)
//      println("cfinca por cada ccliente = "+q5.count()+" registros")
//      q5.show(10,truncate = false)
//
//      val q6 = sql("""SELECT DISTINCT ccliente, cfinca FROM MaestroContratosClientes WHERE ccliente = "100403491" ORDER BY cfinca""")
//      println("cfinca para ccliente = 100403491 = "+q6.count()+" registros")
//      q6.show(10,truncate = false)
//
//      val q7 = sql("""SELECT ccontrat, cnumscct, count(DISTINCT cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY ccontrat, cnumscct HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """)
//      println("cfinca por cada ccontrat y cnumscct = "+q7.count()+" registros")
//      q7.show(10,truncate = false)
//
//      val q8 = sql("""SELECT DISTINCT ccontrat, cnumscct, cfinca FROM MaestroContratosClientes WHERE ccontrat = "140050102868" AND cnumscct = "001" ORDER BY cfinca""")
//      println("cfinca para ccontrat = 140050102868 y cnumscct = 001 = "+q8.count()+" registros")
//      q8.show(10,truncate = false)



      val q11 = sql("""SELECT cnifdnic, count(DISTINCT cupsree) as SumCupsree FROM MaestroContratosClientes GROUP BY cnifdnic HAVING SumCupsree > 1 ORDER BY SumCupsree DESC """)
      println("Número de cupsree's por cada cnifdnic = "+q11.count()+" registros")
      q11.show(10,truncate = false)

     sql("""SELECT DISTINCT cnifdnic, cupsree FROM MaestroContratosClientes WHERE cnifdnic = "P0801900B" """).show(10,truncate = false)

      val q12 = sql("""SELECT ccliente, count(DISTINCT cupsree) as SumCupsree FROM MaestroContratosClientes GROUP BY ccliente HAVING SumCupsree > 1 ORDER BY SumCupsree DESC """)
      println("Número de cupsree's por cada ccliente = "+q12.count()+" registros")
      q12.show(10,truncate = false)

      sql("""SELECT DISTINCT ccliente, cupsree FROM MaestroContratosClientes WHERE ccliente = "100403491" """).show(10,truncate = false)

      val q13 = sql("""SELECT ccontrat, cnumscct, count(DISTINCT cupsree) as SumCupsree FROM MaestroContratosClientes GROUP BY ccontrat, cnumscct HAVING SumCupsree > 1 ORDER BY SumCupsree DESC """)
      println("Número de cupsree's por cada ccliente = "+q13.count()+" registros")
      q13.show(10,truncate = false)

      sql("""SELECT DISTINCT ccontrat, cnumscct, cupsree FROM MaestroContratosClientes WHERE ccontrat = "180049501520" AND cnumscct="001" """).show(10,truncate = false)

      val q15 = sql("""SELECT DISTINCT cfinca, count(DISTINCT cupsree) as SumCupsree FROM MaestroContratosClientes GROUP BY cfinca HAVING SumCupsree > 1 ORDER BY SumCupsree DESC """)
      println("Número de cupsree's por cada cfinca = "+q15.count()+" registros")
      q15.show(10,truncate = false)

      sql("""SELECT DISTINCT cfinca, cupsree FROM MaestroContratosClientes WHERE cfinca = "5693907" """).show(10,truncate = false)

      val q16 = sql("""SELECT cupsree, count(DISTINCT cfinca) as SumCfinca FROM MaestroContratosClientes GROUP BY cupsree HAVING SumCfinca > 1 ORDER BY SumCfinca DESC """)
      println("Número de cfinca's por cada cupsree = "+q16.count()+" registros")
      q16.show(10,truncate = false)

//      sql("""SELECT DISTINCT cupsree, cfinca FROM MaestroContratosClientes WHERE cupsree = "5693907" """).show(10,truncate = false)


//      df_05C.unpersist()
//      df_00C.unpersist()


      val maestroContratosClientesExpedientes = SparkSessionUtils.sparkSession.read.load(TabPaths.maestroContratosClientesExpedientes)

//      df_16.unpersist()

      maestroContratosClientesExpedientes.persist(nivel)
      maestroContratosClientesExpedientes.createOrReplaceTempView("MaestroContratosClientesExpedientes")

      val q14 = sql(
              """SELECT cupsree, COUNT(DISTINCT csecexpe) AS SumCsecexpe, COUNT(DISTINCT finifran) AS SumFinifran, COUNT(DISTINCT ffinfran) AS SumFfinfran, COUNT(DISTINCT fapexpd) AS SumFapexpd, COUNT(DISTINCT fciexped) AS SumFciexped
                  FROM MaestroContratosClientesExpedientes
                   GROUP BY cupsree
                   HAVING (SumCsecexpe > 1 AND SumFinifran > 1 AND SumFfinfran > 1 AND SumFapexpd > 1 AND SumFciexped > 1 )
                  ORDER BY cupsree DESC
                    """)
      q14.show(10,truncate = false)

      sql(
        """SELECT DISTINCT cupsree, csecexpe, finifran, ffinfran, fapexpd, fciexped FROM MaestroContratosClientesExpedientes
          WHERE cupsree = "ES0031448531618001LM0F" """).show(10,truncate = false)

//      val q9 = sql(
//        """SELECT ccontrat, cnumscct, COUNT(DISTINCT csecexpe) AS SumCsecexpe, COUNT(DISTINCT finifran) AS SumFinifran, COUNT(DISTINCT ffinfran) AS SumFfinfran, COUNT(DISTINCT fapexpd) AS SumFapexpd,
//           COUNT(DISTINCT fciexped) AS SumFciexped
//            FROM MaestroContratosClientesExpedientes
//             GROUP BY ccontrat, cnumscct
//             HAVING (SumCsecexpe > 1 AND SumFinifran > 1 AND SumFfinfran > 1 AND SumFapexpd > 1 AND SumFciexped > 1 )
//            ORDER BY ccontrat DESC, cnumscct DESC
//              """)
//
//      println("csecexpe, finifran, ffinfran, fapexpd por cada ccontrat y cnumscct = "+q9.count()+" registros")
//      q9.show(10,truncate = false)
//
//      val q10 = sql(
//        """SELECT ccontrat, cnumscct, csecexpe, finifran, ffinfran, fapexpd, fciexped  FROM MaestroContratosClientesExpedientes WHERE ccontrat = "380046773806" AND cnumscct = "011" AND
//          cfinca ="6173636"""")
//      println("csecexpe, finifran, ffinfran, fapexpd para ccontrat = 380046773806 y cnumscct = 011 = "+q10.count()+" registros")
//      q10.show(10,truncate = false)

    }

    SparkSessionUtils.sc.stop()

  }

}
