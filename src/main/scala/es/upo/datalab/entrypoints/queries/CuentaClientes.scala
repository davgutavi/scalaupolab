package es.upo.datalab.entrypoints.queries

import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 21/03/17.
  */
object CuentaClientes {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {


          val df_05C = LoadTableCsv.loadTable(TabPaths.TAB05C, TabPaths.TAB05C_headers)
          df_05C.persist(nivel)
          df_05C.createOrReplaceTempView("Clientes")

          val contadorClientes = sql(""" SELECT origen, cemptitu, ccontrat, cnumscct, count(ccliente) as sumatorio FROM Clientes GROUP BY origen, cemptitu, ccontrat, cnumscct""")
          contadorClientes.persist(nivel)
          contadorClientes.createOrReplaceTempView("contadorClientes")

          sql("""SELECT * FROM ContadorClientes  WHERE sumatorio > 1""").show(8,truncate = false)

          df_05C.unpersist()


    }




    SparkSessionUtils.sc.stop()

  }

}