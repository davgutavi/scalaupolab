package es.upo.datalab.entrypoints.joins


import es.upo.datalab.utilities.{LoadTable, SparkSessionUtils, TabPaths, TimingUtils}
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

      //val maestroContratosRepetidos = maestroContratosClientesS.except(maestroContratosClientes)
      val maestroContratosRepetidos = maestroContratosClientes.except(maestroContratosClientesS)

      println("MaestroContratosClientes ("+maestroContratosRepetidos.count()+" registros que están repetidos)\n")
      maestroContratosRepetidos.show(20,false)


      df_00C.unpersist()
      df_05C.unpersist()
      maestroContratosClientes.persist(nivel)
      maestroContratosClientes.createOrReplaceTempView("MaestroContratosClientes")


      val mcc = maestroContratosClientes.count()
      val mccs = maestroContratosClientesS.count()
      println("MaestroContratosClientes ("+mcc+" registros)\n")
      println("MaestroContratosClientes ("+mccs+" registros sin repeticion)\n")
      println("Diferencia = "+(mcc-mccs))


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
      val maestroContratosClientesMaestroAparatosRepetidos = maestroContratosClientesMaestroAparatosS.except(maestroContratosClientesMaestroAparatos)
      println("MaestroContratosClientesMaestroAparatos ("+maestroContratosClientesMaestroAparatosRepetidos.count()+" registros que están repetidos)\n")
      maestroContratosClientesMaestroAparatosRepetidos.show(20,false)

      df_00E.unpersist()
      maestroContratosClientes.unpersist()

      maestroContratosClientesMaestroAparatos.persist(nivel)
      maestroContratosClientesMaestroAparatos.createOrReplaceTempView("MaestroContratosClientesAparatos")


      val mccma = maestroContratosClientesMaestroAparatos.count()
      val mccmas = maestroContratosClientesMaestroAparatosS.count()


      println("\nMaestroContratosClientesMaestroAparatos ("+mccma+" registros)\n")
      println("MaestroContratosClientesMaestroAparatos ("+mccmas+" registros sin repeticion)\n")
      println("Diferencia = "+(mccma-mccmas))

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
