package es.upo.datalab.entrypoints.joins

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 23/03/17.
  */
object McontratosClientesMaparatos {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time{

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C,TabPaths.TAB_05C_headers)
      df_05C.persist(nivel)
      df_05C.createOrReplaceTempView("Clientes")

      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)
      df_00E.persist(nivel)
      df_00E.createOrReplaceTempView("MaestroAparatos")

      val maestroContratosClientes = sql(
        """SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.ccounips, MaestroContratos.cupsree2,
           MaestroContratos.cpuntmed, MaestroContratos.tpuntmed, MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
           Clientes.ccliente, Clientes.fechamov, Clientes.tindfiju, Clientes.cnifdnic, Clientes.dapersoc, Clientes.dnombcli
           FROM MaestroContratos JOIN Clientes
           ON MaestroContratos.origen = Clientes.origen AND MaestroContratos.cemptitu = Clientes.cemptitu AND MaestroContratos.ccontrat = Clientes.ccontrat AND MaestroContratos.cnumscct = Clientes.cnumscct
        """)

      val maestroContratosClientesS = maestroContratosClientes.dropDuplicates()

      df_00C.unpersist()
      df_05C.unpersist()
      maestroContratosClientes.persist(nivel)
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")

      println("MaestroContratosClientes ("+maestroContratosClientes.count()+" registros)\n")
      println("MaestroContratosClientes ("+maestroContratosClientesS.count()+" registros sin repeticion)\n")
      maestroContratosClientes.show(5,false)

      val maestroContratosClientesMaestroAparatos = sql(
        """SELECT MaestroContratosClientes.origen, MaestroContratosClientes.cptocred, MaestroContratosClientes.cfinca, MaestroContratosClientes.cptoserv, MaestroContratosClientes.cderind, MaestroContratosClientes.cupsree,
          MaestroContratosClientes.ccounips, MaestroContratosClientes.cupsree2,MaestroContratosClientes.cpuntmed, MaestroContratosClientes.tpuntmed, MaestroContratosClientes.vparsist, MaestroContratosClientes.cemptitu,
          MaestroContratosClientes.ccontrat, MaestroContratosClientes.cnumscct, MaestroContratosClientes.fpsercon, MaestroContratosClientes.ffinvesu, MaestroContratosClientes.ccliente, MaestroContratosClientes.fechamov,
          MaestroContratosClientes.tindfiju, MaestroContratosClientes.cnifdnic, MaestroContratosClientes.dapersoc, MaestroContratosClientes.dnombcli, MaestroAparatos.csecptom, MaestroAparatos.fvigorpm,
          MaestroAparatos.fbajapm,MaestroAparatos.caparmed
          FROM MaestroContratosClientes JOIN MaestroAparatos
         ON MaestroContratosClientes.origen = MaestroAparatos.origen AND MaestroContratosClientes.cupsree2 = MaestroAparatos.cupsree2 AND MaestroContratosClientes.cpuntmed = MaestroAparatos.cpuntmed
      """)

      val maestroContratosClientesMaestroAparatosS = maestroContratosClientesMaestroAparatos.dropDuplicates()

      df_00E.unpersist()
      maestroContratosClientes.unpersist()

      maestroContratosClientesMaestroAparatos.persist(nivel)
      maestroContratosClientesMaestroAparatos.createOrReplaceTempView("MaestroContratosClientesAparatos")
      println("\nMaestroContratosClientesMaestroAparatos ("+maestroContratosClientesMaestroAparatos.count()+" registros)\n")
      println("MaestroContratosClientesMaestroAparatos ("+maestroContratosClientesMaestroAparatosS.count()+" registros sin repeticion)\n")
      maestroContratosClientesMaestroAparatos.show(5,false)

      val sumadorCupsree = sql("""SELECT origen, cupsree2, cpuntmed, ccliente, count(cupsree) AS SumCupsree FROM MaestroContratosClientesAparatos GROUP BY origen, cupsree2, cpuntmed, ccliente""")

      maestroContratosClientesMaestroAparatos.unpersist()

      sumadorCupsree.persist(nivel)
      sumadorCupsree.createOrReplaceTempView("SumadorCupsree")

      println("\nSumadorCupsree ("+sumadorCupsree.count()+" registros)\n")
      sumadorCupsree.show(5,false)


      sql ("""SELECT ccliente, SumCupsree FROM SumadorCupsree WHERE SumCupsree > 1""").show(10,false)

      }


    SparkSessionUtils.sc.stop()


  }

}
