package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.SparkSessionUtils

/**
  * Created by davgutavi on 21/03/17.
  */
object CuentaClientes {

  def main( args:Array[String] ):Unit = {

    import SparkSessionUtils.sqlContext.sql

    val df_05C = LoadTable.loadTable(TabPaths.TAB_05C,TabPaths.TAB_05C_headers)
    df_05C.cache()
    df_05C.registerTempTable("clientes")

    println("Contador clientes\n\n")

    val c_2 = sql(
      """SELECT origen, cemptitu, ccontrat, cnumscct, count(ccliente) as sumatorio
               FROM clientes
               GROUP BY origen, cemptitu, ccontrat, cnumscct
            """)

    c_2.cache()
    c_2.registerTempTable("contclientes")

    println("\n Tamaño = "+c_2.count())


    val c_2_p = sql( """SELECT *
               FROM contclientes
               WHERE sumatorio > 1
               ORDER BY sumatorio
            """)

    c_2_p.cache()

    c_2_p.show(200)

    println("\n Tamaño = "+c_2_p.count())

    //    val c_1 = sql(
    //      """SELECT origen, cemptitu, ccontrat, cnumscct, count(ccliente) as sumatorio
    //         FROM con_cli
    //         GROUP BY origen, cemptitu, ccontrat, cnumscct
    //      """)
    //
    //    c_1.cache()
    //    c_1.registerTempTable("contclientes")
    //    println("Contador contratos clientes\n\n")
    //    c_1.show(30)
    //    println("\n Tamaño = "+c_1.count())
    //
    //    val c_1_p = sql(
    //            """SELECT *
    //               FROM contclientes
    //               WHERE ccontrat = '130055576341'
    //            """)
    //    c_1_p.show(30)
    //    println("\n Tamaño = "+c_1_p.count())



    SparkSessionUtils.sc.stop()

  }

}