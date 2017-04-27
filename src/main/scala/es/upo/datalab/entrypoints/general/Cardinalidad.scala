package es.upo.datalab.entrypoints.general

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 27/04/17.
  */
object Cardinalidad {

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

      val maestroContratosClientes = sql(

        """
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed,
          MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu, Clientes.ccliente, Clientes.fechamov, Clientes.tindfiju,
          Clientes.cnifdnic, Clientes.dapersoc, Clientes.dnombcli
          FROM Clientes JOIN MaestroContratos
          ON Clientes.origen=MaestroContratos.origen AND Clientes.cemptitu=MaestroContratos.cemptitu AND Clientes.ccontrat=MaestroContratos.ccontrat AND Clientes.cnumscct=MaestroContratos.cnumscct

        """
      )

      df_05C.unpersist()
      df_00C.unpersist()

      maestroContratosClientes.persist(nivel)
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")


      val maestroContratosClientesExpedientes = sql(

        """
          SELECT MaestroContratosClientes.origen, MaestroContratosClientes.cptocred, MaestroContratosClientes.cfinca, MaestroContratosClientes.cptoserv, MaestroContratosClientes.cderind, MaestroContratosClientes.cupsree,
          MaestroContratosClientes.cpuntmed, MaestroContratosClientes.tpuntmed, MaestroContratosClientes.vparsist, MaestroContratosClientes.cemptitu, MaestroContratosClientes.ccontrat, MaestroContratosClientes.cnumscct,
          MaestroContratosClientes.fpsercon, MaestroContratosClientes.ffinvesu, MaestroContratosClientes.ccliente, MaestroContratosClientes.fechamov, MaestroContratosClientes.tindfiju, MaestroContratosClientes.cnifdnic,
          MaestroContratosClientes.dapersoc, MaestroContratosClientes.dnombcli, Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
          Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,
          Expedientes.fciexped
          FROM MaestroContratosClientes JOIN Expedientes
          ON MaestroContratosClientes.origen=Expedientes.origen AND MaestroContratosClientes.cemptitu=Expedientes.cemptitu AND MaestroContratosClientes.cfinca=Expedientes.cfinca AND MaestroContratosClientes.cptoserv=Expedientes.cptoserv
        """
      )

      maestroContratosClientes.unpersist()

      maestroContratosClientesExpedientes.persist(nivel)
      maestroContratosClientesExpedientes.createOrReplaceTempView("MaestroContratosClientesExpedientes")

      val q1 = sql("""SELECT cnifdnic, COUNT(DISTINCT ccliente) AS sumCliente FROM MaestroContratosClientesExpedientes GROUP BY cnifdnic HAVING sumCliente > 1 ORDER BY sumCliente DESC""")
      q1.show(10,false)
      sql("""SELECT cnifdnic, ccliente FROM MaestroContratosClientesExpedientes WHERE cnifdnic = "A28354520" """).show(20,false)

      val q2 = sql(
        """SELECT ccliente, COUNT(DISTINCT ccontrat) AS sumCcontrat, COUNT(DISTINCT cnumscct) AS sumCnumscct FROM MaestroContratosClientesExpedientes
          GROUP BY ccliente HAVING (sumCcontrat > 1 OR sumCnumscct > 1) ORDER BY sumCcontrat DESC, sumCnumscct DESC """)
      q2.show(10,false)
      sql("""SELECT ccliente, ccontrat, cnumscct FROM MaestroContratosClientesExpedientes WHERE ccliente = "100403491" """).show(20,false)

      val q3 = sql("""SELECT ccontrat, COUNT(DISTINCT cfinca) AS sumCfinca FROM MaestroContratosClientesExpedientes GROUP BY ccontrat HAVING sumCfinca > 1 ORDER BY sumCfinca DESC""")
      q3.show(10,false)
      sql("""SELECT ccontrat, cfinca FROM MaestroContratosClientesExpedientes WHERE ccontrat = "140051783546" """).show(20,false)

      val q4 = sql(
        """SELECT ccontrat, COUNT(DISTINCT csecexpe) AS sumCsecexpe, COUNT(DISTINCT finifran) AS sumFinifran, COUNT(DISTINCT ffinfran) AS sumFfinfran, COUNT(DISTINCT fapexpd) AS sumFapexpd,
           COUNT(DISTINCT fciexped) AS sumFciexped
           FROM MaestroContratosClientesExpedientes
          GROUP BY ccontrat
          HAVING (sumCsecexpe > 1 OR sumFinifran > 1 OR   sumFfinfran > 1 OR sumFapexpd > 1  OR sumFciexped >1  )
        ORDER BY sumCsecexpe DESC, sumFinifran DESC, sumFfinfran DESC, sumFapexpd DESC, sumFciexped DESC""")
      q4.show(10,false)
      sql("""SELECT ccontrat, csecexpe,finifran,ffinfran,fapexpd, fciexped FROM MaestroContratosClientesExpedientes WHERE ccontrat = "210018123693" """).show(20,false)





      print("Cardinalidad Persona - Cliente = "+q1.count()+" registros")
      print("Cardinalidad Cliente - Contrato = "+q2.count()+" registros")
      print("Cardinalidad Contrato - Finca = "+q3.count()+" registros")
      print("Cardinalidad Contrato - Expediente = "+q4.count()+" registros")
    }

    SparkSessionUtils.sc.stop()

  }

}
