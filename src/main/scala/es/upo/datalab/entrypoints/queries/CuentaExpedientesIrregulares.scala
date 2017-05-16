package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 20/04/17.
  */
object CuentaExpedientesIrregulares {
  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_00C = LoadTableCsv.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

      val df_05C = LoadTableCsv.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, dropDuplicates = true)
      df_05C.persist(nivel)
      df_05C.createOrReplaceTempView("Clientes")


      val df_16 = LoadTableCsv.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("Expedientes")

      val t = df_16.count()
      val i = df_16.where("irregularidad = 'S'").count()
      val a = df_16.where("anomalia = 'S'").count()

      println("Expedientes irregularidad = "+i+" registros")
      println("Expedientes anomalia = "+a+" registros")
      println("Suma irregularidad más anomalía = "+(i+a)+" registros")
      println("Registros totales en Expedientes = "+t)
      println("Diferencia Totales - (irregulares + anómalos) = "+(t-(i+a)))


      val maestroContratosClientes = sql(

        """
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree,
          MaestroContratos.ccounips,MaestroContratos.cupsree2, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu,
          MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
          Clientes.ccliente, Clientes.fechamov, Clientes.tindfiju, Clientes.cnifdnic, Clientes.dapersoc, Clientes.dnombcli
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
          MaestroContratosClientes.ccounips,MaestroContratosClientes.cupsree2,MaestroContratosClientes.cpuntmed, MaestroContratosClientes.tpuntmed, MaestroContratosClientes.vparsist, MaestroContratosClientes.cemptitu,
          MaestroContratosClientes.ccontrat,MaestroContratosClientes.cnumscct, MaestroContratosClientes.fpsercon, MaestroContratosClientes.ffinvesu,MaestroContratosClientes.ccliente, MaestroContratosClientes.fechamov,
          MaestroContratosClientes.tindfiju, MaestroContratosClientes.cnifdnic, MaestroContratosClientes.dapersoc, MaestroContratosClientes.dnombcli,
          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie,
          Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
          FROM MaestroContratosClientes JOIN Expedientes
          ON MaestroContratosClientes.origen=Expedientes.origen AND MaestroContratosClientes.cemptitu=Expedientes.cemptitu AND MaestroContratosClientes.cfinca=Expedientes.cfinca AND MaestroContratosClientes.cptoserv=Expedientes.cptoserv
        """
      )

      maestroContratosClientes.unpersist()

      maestroContratosClientesExpedientes.persist(nivel)


      val maestroContratosClientesSinExpedientes = sql(
        """
          SELECT MaestroContratosClientes.origen, MaestroContratosClientes.cptocred, MaestroContratosClientes.cfinca, MaestroContratosClientes.cptoserv, MaestroContratosClientes.cderind, MaestroContratosClientes.cupsree,
          MaestroContratosClientes.ccounips,MaestroContratosClientes.cupsree2,MaestroContratosClientes.cpuntmed, MaestroContratosClientes.tpuntmed, MaestroContratosClientes.vparsist, MaestroContratosClientes.cemptitu,
          MaestroContratosClientes.ccontrat,MaestroContratosClientes.cnumscct, MaestroContratosClientes.fpsercon, MaestroContratosClientes.ffinvesu,MaestroContratosClientes.ccliente, MaestroContratosClientes.fechamov,
          MaestroContratosClientes.tindfiju, MaestroContratosClientes.cnifdnic, MaestroContratosClientes.dapersoc, MaestroContratosClientes.dnombcli,
          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie,
          Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
          FROM MaestroContratosClientes LEFT JOIN Expedientes
          ON MaestroContratosClientes.origen=Expedientes.origen AND MaestroContratosClientes.cemptitu=Expedientes.cemptitu AND MaestroContratosClientes.cfinca=Expedientes.cfinca AND MaestroContratosClientes.cptoserv=Expedientes.cptoserv
          WHERE Expedientes.origen IS NULL AND Expedientes.cemptitu IS NULL AND Expedientes.cfinca IS NULL AND Expedientes.cptoserv IS NULL
        """
      )
      maestroContratosClientesSinExpedientes.persist()

      maestroContratosClientesSinExpedientes.show(10,truncate=false)

      val c1_0 = maestroContratosClientesExpedientes.where("irregularidad = 'N'").count()
      val c2_0 = maestroContratosClientesExpedientes.where("anomalia = 'N'").count()
      val c1_1 = maestroContratosClientesExpedientes.where("irregularidad = 'S'").count()
      val c2_1 = maestroContratosClientesExpedientes.where("anomalia = 'S'").count()
      val c00 = maestroContratosClientesExpedientes.where("anomalia = 'N' AND irregularidad= 'N' ").count()
      val c01 = maestroContratosClientesExpedientes.where("anomalia = 'N' AND irregularidad= 'S' ").count()
      val c10 = maestroContratosClientesExpedientes.where("anomalia = 'S' AND irregularidad= 'N' ").count()
      val c11 = maestroContratosClientesExpedientes.where("anomalia = 'S' AND irregularidad= 'S' ").count()
      val s = maestroContratosClientesSinExpedientes.count()


      println("irregularidad = 'N' => "+c1_0+" registros")
      println("anomalia = 'N' => "+c2_0+" registros")
      println("irregularidad = 'S' => "+c1_1+" registros")
      println("anomalia = 'S' => "+c2_1+" registros")
      println("irregularidad = 'N' => "+c1_0+" registros")

      println("anomalia = 'N' AND irregularidad= 'N' => "+c00+" registros")
      println("anomalia = 'N' AND irregularidad= 'S' => "+c01+" registros")
      println("anomalia = 'S' AND irregularidad= 'N' => "+c10+" registros")
      println("anomalia = 'S' AND irregularidad= 'S' => "+c11+" registros")

      println("clientes sin expedientes = "+s+" registros")




    }


    SparkSessionUtils.sc.stop()


  }
}