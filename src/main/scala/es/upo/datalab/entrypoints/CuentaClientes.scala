package es.upo.datalab.entrypoints


import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.SparkSessionUtils

/**
  * Created by davgutavi on 21/03/17.
  */
object CuentaClientes {

  def main( args:Array[String] ):Unit = {

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    val df_05C = LoadTable.loadTable(TabPaths.TAB_05C,TabPaths.TAB_05C_headers)
    df_05C.cache()
    df_05C.createOrReplaceTempView("clientes")

    println("Contador clientes\n\n")

    val c_2 = sql(
      """SELECT origen, cemptitu, ccontrat, cnumscct, count(ccliente) as sumatorio
               FROM clientes
               GROUP BY origen, cemptitu, ccontrat, cnumscct
            """)

    c_2.cache()
    c_2.createOrReplaceTempView("contclientes")

    println("\n Tamaño = "+c_2.count())


    val c_2_p = sql( """SELECT *
               FROM contclientes
               WHERE sumatorio > 1
               ORDER BY sumatorio
            """)

    c_2_p.cache()

    c_2_p.show(200)

    println("\n Tamaño = "+c_2_p.count())




    SparkSessionUtils.sc.stop()

  }

}