package es.upo.datalab.entrypoints.joins

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 15/03/17.
  */
object ClientesMcontratosMaparatosCurvaCargaV1 {


  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._



    TimingUtils.time{

    val df_00C = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)

      //println("Persistiendo Contratos\n")
      //df_00C.persist(nivel)
      println("Registrando Contratos")
      df_00C.createOrReplaceTempView("contratos")

    val df_05C = LoadTable.loadTable(TabPaths.TAB_05C,TabPaths.TAB_05C_headers)

      //println("Persistiendo Clientes\n")
      //df_05C.persist(nivel)
      println("Registrando Clientes")
      df_05C.createOrReplaceTempView("clientes")

    val df_00E = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)

      //println("Persistiendo Aparatos\n")
      //df_00E.persist(nivel)
      println("Registrando Aparatos")
      df_00E.createOrReplaceTempView("aparatos")

    val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10,TabPaths.TAB_01_headers)

      //println("Persistiendo Cargas\n")
      //df_01_10.persist(nivel)
      println("Registrando Cargas")
      df_01_10.createOrReplaceTempView("cargas")

      println("Construyendo J1\n")

      val j1 = sql(
        """SELECT contratos.origen, contratos.cemptitu, contratos.ccontrat, contratos.cnumscct, contratos.cupsree2, contratos.cpuntmed, clientes.ccliente, clientes.dapersoc, clientes.dnombcli
            FROM contratos JOIN clientes
            ON contratos.origen = clientes.origen AND contratos.cemptitu = clientes.cemptitu AND contratos.ccontrat = clientes.ccontrat AND contratos.cnumscct = clientes.cnumscct
        """)

//      println("Persistiendo J1\n")
//      j1.persist(nivel)
      println("Registrando Tabla J1\n")
      j1.createOrReplaceTempView("con_cli")

      println("\nJoin contratos-clientes ("+j1.count()+" registros)\n")
      j1.show(5)



      println("Borrando Contratos, Clientes\n")
      df_00C.unpersist()
      df_05C.unpersist()

      println("Construyendo J2\n")

      val j2 = sql(
      """SELECT aparatos.origen, aparatos.cpuntmed, con_cli.ccliente, con_cli.dapersoc, con_cli.dnombcli
         FROM con_cli JOIN aparatos
         ON con_cli.origen = aparatos.origen AND con_cli.cupsree2 = aparatos.cupsree2 AND con_cli.cpuntmed = aparatos.cpuntmed
      """)

//      println("Persistiendo J2\n")
//      j2.persist(nivel)

      println("Registrando Tabla J2\n")
      j2.createOrReplaceTempView("con_cli_apa")

      println("\nJoin contratos-clientes-aparatos ("+j2.count()+" registros)\n")
      j2.show(5)

      println("Borrando Aparatos, J1\n")
      df_00E.unpersist()
      j1.unpersist()

      println("Construyendo J3\n")

      val j3 = sql(
      """SELECT cargas.origen, cargas.cpuntmed, con_cli_apa.ccliente, con_cli_apa.dapersoc, con_cli_apa.dnombcli,
         cargas.hora_01, cargas.1q_consumo_01, cargas.2q_consumo_01, cargas.3q_consumo_01, cargas.3q_consumo_01
         FROM con_cli_apa JOIN cargas
         ON con_cli_apa.origen = cargas.origen AND con_cli_apa.cpuntmed = cargas.cpuntmed
      """)

//      println("Persistiendo J3\n")
//      j3.persist(nivel)

      println("\nJoin contratos-clientes-aparatos-curvas ("+j3.count()+" registros)\n")
      j3.show(5)

    }

    SparkSessionUtils.sc.stop()

  }

}