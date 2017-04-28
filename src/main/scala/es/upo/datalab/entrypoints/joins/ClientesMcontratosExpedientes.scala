package es.upo.datalab.entrypoints.joins

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 31/03/17.
  */
object ClientesMcontratosExpedientes {

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
      val maestroContratosClientess = maestroContratosClientes.dropDuplicates()
      maestroContratosClientess.persist(nivel)

      val mcc = maestroContratosClientes.count()
      val mccs = maestroContratosClientess.count()

      println("Maestro Contratos - Clientes = " +mcc + " registros")
      println("Maestro Contratos - Clientes sin repetición = " + mccs + " registros")
      println("Diferencia = " + (mcc-mccs))

      maestroContratosClientes.unpersist()
      maestroContratosClientess.createOrReplaceTempView("MaestroContratosClientess")


      val maestroContratosClientesExpedientes = sql(

        """
          SELECT MaestroContratosClientess.origen, MaestroContratosClientess.cptocred, MaestroContratosClientess.cfinca, MaestroContratosClientess.cptoserv, MaestroContratosClientess.cderind, MaestroContratosClientess.cupsree,
          MaestroContratos.ccounips,MaestroContratos.cupsree2,MaestroContratosClientess.cpuntmed, MaestroContratosClientess.tpuntmed, MaestroContratosClientess.vparsist, MaestroContratosClientess.cemptitu, MaestroContratosClientess.ccontrat,
          MaestroContratosClientess.cnumscct, MaestroContratosClientess.fpsercon, MaestroContratosClientess.ffinvesu,
          MaestroContratosClientess.ccliente, MaestroContratosClientess.fechamov, MaestroContratosClientess.tindfiju, MaestroContratosClientess.cnifdnic, MaestroContratosClientess.dapersoc, MaestroContratosClientess.dnombcli,
          Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad, Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie,
          Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa, Expedientes.fciexped
          FROM MaestroContratosClientess JOIN Expedientes
          ON MaestroContratosClientess.origen=Expedientes.origen AND MaestroContratosClientess.cemptitu=Expedientes.cemptitu AND MaestroContratosClientess.cfinca=Expedientes.cfinca AND MaestroContratosClientess.cptoserv=Expedientes.cptoserv
        """
      )

      maestroContratosClientess.unpersist()

      maestroContratosClientesExpedientes.persist(nivel)
      val maestroContratosClientesExpedientess = maestroContratosClientesExpedientes.dropDuplicates()
      maestroContratosClientesExpedientess.persist(nivel)

      val mcce = maestroContratosClientesExpedientes.count()
      val mcces = maestroContratosClientesExpedientess.count()

      println("Maestro Contratos - Clientes - Expedientes = " +mcce + " registros")
      println("Maestro Contratos - Clientes - Expedientes sin repetición = " + mcces + " registros")
      println("Diferencia = " + (mcce-mcces))

      maestroContratosClientesExpedientes.unpersist()
      maestroContratosClientesExpedientess.createOrReplaceTempView("MaestroContratosClientesExpedientess")


      val maestroContratosClientesExpedientessi = maestroContratosClientesExpedientess.where("irregularidad='S'")
      println("Maestro Contratos - Clientes - Expedientes sin repetición con irregularidad = " + maestroContratosClientesExpedientessi.count() + " registros")
      maestroContratosClientesExpedientessi.show(20,truncate = false)

    }

    SparkSessionUtils.sc.stop()

  }

}
