package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 31/03/17.
  */
object ClientesContratos {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
      df_05C.persist(nivel)
      println("\n Número de registros en Clientes = " + df_05C.count() + "\n")
      df_05C.createOrReplaceTempView("clientes")

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("contratos")
      println("\n Número de registros en Maestro Contratos = " + df_00C.count() + "\n")

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      println("\n Número de registros en Expedientes = " + df_16.count() + "\n")
      df_16.createOrReplaceTempView("expedientes")


      val t1 = sql(
        """
        SELECT cnifdnic,dnombcli,dapersoc, count(ccliente) AS contador_ccliente FROM clientes GROUP BY cnifdnic,dnombcli,dapersoc
      """)
      t1.persist(nivel)
      t1.createOrReplaceTempView("t1")
      println("\n Número de ccliente's asociado a cada cliente = " + t1.count() + "\n")
//      t1.show(10)

      val t1_1 = sql(
        """
        SELECT cnifdnic,dnombcli,dapersoc, contador_ccliente FROM t1 WHERE contador_ccliente > 1
      """)
      println("\n Clientes con más de un ccliente = " + t1_1.count() + "\n")
//      t1_1.show(20)

      val t2 = sql(
        """
        SELECT cnifdnic,dnombcli,dapersoc, count(ccontrat) AS contador_ccontrat FROM clientes GROUP BY cnifdnic,dnombcli,dapersoc
      """)
      t2.persist(nivel)
      t2.createOrReplaceTempView("t2")
      println("\n Número de ccontrat's asociado a cada cliente = " + t2.count() + "\n")
//      t2.show(10)

      val t2_1 = sql(
        """
        SELECT cnifdnic,dnombcli,dapersoc, contador_ccontrat FROM t2 WHERE contador_ccontrat > 1
      """)
      println("\n Clientes con más de un ccontrat = " + t2_1.count() + "\n")
//      t2_1.show(20)

      val t2_2 = sql(
        """
        SELECT cnifdnic,dnombcli,dapersoc, contador_ccontrat FROM t2 WHERE contador_ccontrat = 1
      """)
      println("\n Clientes con un ccontrat = " + t2_2.count() + "\n")


      val cli_con_r = sql(
        """
          SELECT clientes.cnifdnic,clientes.dapersoc,clientes.dnombcli,contratos.origen,contratos.cfinca,contratos.cptoserv,contratos.cderind
          FROM clientes JOIN contratos
          ON clientes.origen=contratos.origen AND clientes.cemptitu=contratos.cemptitu AND clientes.ccontrat=contratos.ccontrat AND clientes.cnumscct=contratos.cnumscct
        """)
      cli_con_r.persist(nivel)
      cli_con_r.createOrReplaceTempView("cliconr")
      println("\n Join Clientes Contratos con repeticiones = " + cli_con_r.count() + "\n")
//      cli_con_r.show(10)

      val cli_con = cli_con_r.dropDuplicates()
      cli_con.persist(nivel)
      cli_con.createOrReplaceTempView("clicon")
      println("\n Join Clientes Contratos sin repeticiones = " + cli_con.count() + "\n")
//      cli_con.show(20)









      //      val cli_con_1 = sql(
//        """
//        SELECT origen, cfinca, cptoserv, cderind, count(cnifdnic) as contador_nif
//        FROM clicon
//        GROUP BY origen, cfinca, cptoserv, cderind
//      """)
//      cli_con_1.persist(nivel)
//      cli_con_1.createOrReplaceTempView("clicon1")
//      println("\n Número de clientes por cada tupla {origen, cfinca, cptoserv, cderind} = " + cli_con_1.count() + "\n")
//      cli_con_1.show(20)
//
//
//      val cli_con_2 = sql(
//        """
//        SELECT origen, cfinca, cptoserv, cderind, contador_nif
//        FROM clicon1 WHERE contador_nif > 1
//      """)
//      cli_con_2.persist(nivel)
//      cli_con_2.createOrReplaceTempView("clicon2")
//      println("\n Número de clientes mayores que 1 por cada tupla {origen, cfinca, cptoserv, cderind} = " + cli_con_2.count() + "\n")
//      cli_con_2.show(20)
//
//      val cli_con_3 = sql(
//        """
//        SELECT origen, cfinca, cptoserv, cderind,contador_nif
//        FROM clicon1 WHERE contador_nif = 1
//      """)
//      cli_con_3.persist(nivel)
//      cli_con_3.createOrReplaceTempView("clicon3")
//      println("\n Número de clientes igual a 1 por cada tupla {origen, cfinca, cptoserv, cderind} = " + cli_con_3.count() + "\n")
//      cli_con_3.show(20)
//
//      val cli_con_4 = sql(
//        """
//        SELECT origen, cfinca, cptoserv, cderind,contador_nif
//        FROM clicon1 WHERE contador_nif = 0
//      """)
//      cli_con_4.persist(nivel)
//      cli_con_4.createOrReplaceTempView("clicon4")
//      println("\n Número de clientes igual a 0 por cada tupla {origen, cfinca, cptoserv, cderind} = " + cli_con_4.count() + "\n")
//      cli_con_4.show(20)
//
//      val cli_con_5 = sql(
//        """
//        SELECT origen, cfinca, cptoserv, cderind,cnifdnic
//        FROM clicon WHERE cnifdnic="B60844206"
//      """)
//      cli_con_5.persist(nivel)
//      cli_con_5.createOrReplaceTempView("clicon5")
//      println("\n Tuplas {origen, cfinca, cptoserv, cderind} asociadas al cnifdnic = B60844206 = " + cli_con_5.count() + "\n")
//      cli_con_5.show(20)
//
//      val cli_con_7 = sql(
//        """
//        SELECT origen, cfinca, cptoserv, cderind, cnifdnic
//        FROM clicon WHERE cnifdnic="P0801900B"
//      """)
//      cli_con_7.persist(nivel)
//      cli_con_7.createOrReplaceTempView("clicon7")
//      println("\n Tuplas {origen, cfinca, cptoserv, cderind} asociadas al cnifdnic = P0801900B  = " + cli_con_7.count() + "\n")
//      cli_con_7.show(20)
//
//      val cli_con_8 = sql(
//        """
//        SELECT origen, cfinca, cptoserv, cderind, cnifdnic
//        FROM clicon WHERE origen = 'F' AND cfinca = 6002147 AND cptoserv = "007" AND cderind = "001"
//      """)
//      cli_con_8.persist(nivel)
//      cli_con_8.createOrReplaceTempView("clicon8")
//      println("\n  origen = 'F' AND cfinca = 6002147 AND cptoserv = 007 AND cderind = 001 =" + cli_con_8.count() + "\n")
//      cli_con_8.show(20)


      val cli_con_exp_r = sql(
        """
          SELECT clicon.cnifdnic, clicon.dapersoc, clicon.dnombcli, expedientes.origen,expedientes.cfinca ,expedientes.cptoserv,  expedientes.cderind,expedientes.irregularidad, expedientes.fapexpd, expedientes.finifran,expedientes.ffinfran
          FROM clicon JOIN expedientes
          ON clicon.origen = expedientes.origen AND clicon.cfinca = expedientes.cfinca AND clicon.cptoserv = expedientes.cptoserv AND clicon.cderind = expedientes.cderind
        """
      )
      cli_con_exp_r.persist(nivel)
      cli_con_exp_r.createOrReplaceTempView("cliconexpr")
      println("\n Join Clientes Contratos Expedientes con repeticiones = " + cli_con_exp_r.count() + "\n")
//      cli_con_exp_r.show(40)


      val cli_con_exp = cli_con_exp_r.dropDuplicates()
      cli_con_exp.persist(nivel)
      cli_con_exp.createOrReplaceTempView("cliconexp")
      println("\n Join Clientes Contratos Expedientes sin repeticiones = " + cli_con_exp.count() + "\n")
//      cli_con_exp.show(40)

      val w1 = sql(
        """SELECT irregularidad,fapexpd,finifran,ffinfran, count(cnifdnic)
           FROM cliconexp
           GROUP BY irregularidad, fapexpd , finifran,  ffinfran           """)

      println("\n Contador de cnifdnic por registro de expedientes = " + w1.count() + "\n")
      w1.show(40)

      val w2 = sql(
        """SELECT origen,cfinca,cptoserv,cderind, count(cnifdnic)
           FROM cliconexp
           GROUP BY origen,cfinca,cptoserv,cderind           """)

      println("\n Contador de cnifdnic por tuplas de enlace= " + w2.count() + "\n")
      w2.show(40)








    }


  }

}
