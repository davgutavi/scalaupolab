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
      println("Número de registros en Clientes = " + df_05C.count())
      df_05C.createOrReplaceTempView("clientes")
      val df_05Cs = df_05C.dropDuplicates()
      println("Número de registros en Clientes sin Repetición = " + df_05Cs.count())
      df_05Cs.createOrReplaceTempView("clientess")

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      println("Número de registros en Maestro Contratos = " + df_00C.count())
      df_00C.createOrReplaceTempView("contratos")
//

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      println("Número de registros en Expedientes = " + df_16.count())
      df_16.createOrReplaceTempView("expedientes")
//      val df_16s = df_16.dropDuplicates()
//      println("Número de registros en Expedientes sin Repetición = " + df_16s.count()+"\n\n")



      val jcc = sql(

        """
          SELECT clientess.cnifdnic, clientess.dnombcli, clientess.dapersoc, clientess.ccliente, contratos.ccontrat, contratos.cnumscct, clientess.fechamov, contratos.fpsercon, contratos.ffinvesu,
          contratos.origen, contratos.cemptitu, contratos.cfinca, contratos.cptoserv
          FROM clientess JOIN contratos
          ON clientess.origen=contratos.origen AND clientess.cemptitu=contratos.cemptitu AND clientess.ccontrat=contratos.ccontrat AND clientess.cnumscct=contratos.cnumscct

        """
      )

      jcc.persist(nivel)
      jcc.createOrReplaceTempView("jcc")
      println("Join clientes-contratos = " + jcc.count() + " registros")
//    jcc.show(20)
      val jccs = jcc.dropDuplicates()
      jccs.createOrReplaceTempView("jccs")
      println("Join clientes-contratos sin repetición = " + jccs.count() + " registros")
//      jccs.show(20)


      val jccse = sql(

        """
          SELECT jccs.cnifdnic, jccs.dnombcli, jccs.dapersoc, jccs.ccliente, jccs.ccontrat, jccs.cnumscct, jccs.fechamov, jccs.fpsercon, jccs.ffinvesu,
          jccs.cfinca, expedientes.fapexpd, expedientes.finifran, expedientes.ffinfran, expedientes.fnormali, expedientes.fciexped, expedientes.irregularidad
          FROM jccs JOIN expedientes
          ON jccs.origen=expedientes.origen AND jccs.cemptitu=expedientes.cemptitu AND jccs.cfinca=expedientes.cfinca AND jccs.cptoserv=expedientes.cptoserv

        """
      )
      jccse.persist(nivel)
      jccse.createOrReplaceTempView("jccse")
      println("Join clientes-contratos-expedientes = " + jccse.count() + " registros")
//      jccse.show(40,false)
      val jccses = jccse.dropDuplicates()
      jccses.createOrReplaceTempView("jccs")
      println("Join clientes-contratos-expedientes sin repetición = " + jccses.count() + " registros")
//      jccses.show(40,false)

      val jccsesi = jccses.where("irregularidad='S'")
      println("Join clientes-contratos-expedientes con irregularidad = " + jccsesi.count() + " registros")
      jccsesi.show(100,false)


      val w1 = jccsesi.drop("ccliente","ccontrat","cnumscct","fechamov","fpsercon","ffinvesu")

      val w2 = w1.dropDuplicates()

      println("Join clientes-contratos-expedientes con irregularidad sin duplicados = " + w2.count() + " registros")
      w2.show(100,false)


//      w2.write.format("com.databricks.spark.csv").option("header","true").save(TabPaths.root+"datasets/clientes_irregularidad_sin_repeticiones")

//      val jccsesis = jccsesi.dropDuplicates()
//      println("Join clientes-contratos-expedientes con irregularidad sin repetición = " + jccsesis.count() + " registros")
//      jccsesis.show(40,false)




//      jccsesi.write.format("com.databricks.spark.csv").option("header","true").save(TabPaths.root+"datasets/clientes_irregularidad.csv")













//      cnifdnic, dnombcli, dapersoc, ccliente, ccontrat, origen, cemptitu, cnumscct, fechamov,tindfiju
//      val l1 = sql(
//        """
//          SELECT cnifdnic, dnombcli, dapersoc, ccliente, ccontrat, cnumscct, fechamov,origen, cemptitu, tindfiju
//          FROM clientess
//          WHERE cnifdnic="36947468S"
//          ORDER BY ccliente
//
//        """)
//      println("Número de registros l1 = " + l1.count())
//      l1.show(100,false)
//
//      val l2 = sql(
//        """
//          SELECT cnifdnic, count(ccliente) as contador_ccliente
//          FROM clientess
//          GROUP BY cnifdnic
//
//        """)
//      println("Número de registros l2 = " + l2.count())
//      l2.show(100,false)
//
//      val l3 = sql(
//        """
//          SELECT cnifdnic, count(ccontrat) as contador_ccontrat
//          FROM clientess
//          GROUP BY cnifdnic
//
//        """)
//      println("Número de registros l3 = " + l3.count())
//      l3.show(100,false)


//      val u1 = sql(
//        """
//          SELECT * FROM contratos JOIN expedientes
//          ON contratos.origen = expedientes.origen AND contratos.cemptitu = expedientes.cemptitu AND contratos.cfinca = expedientes.cfinca AND contratos.cptoserv = expedientes.cptoserv
//
//        """)
//      u1.persist(nivel)
//      u1.createOrReplaceTempView("u1")
//      println("Contratos-Expedientes {origen, cemptitu, cfinca, cptoserv} = " + u1.count() + " registros")
//      u1.show(20)
//      val u1s = u1.dropDuplicates()
//      println("Contratos-Expedientes {origen, cemptitu, cfinca, cptoserv} sin repeticiones = " + u1s.count() + " registros")
//      u1.show(20)
//
//      val u2 = sql(
//        """
//          SELECT * FROM contratos JOIN expedientes
//          ON contratos.origen = expedientes.origen AND contratos.cfinca = expedientes.cfinca AND contratos.cptoserv = expedientes.cptoserv AND contratos.cderind = expedientes.cderind
//
//        """)
//      u2.persist(nivel)
//      u2.createOrReplaceTempView("u2")
//      println("Contratos-Expedientes {origen, cfinca, cptoserv, cderind} = " + u2.count() + " registros")
//      u2.show(10)
//      val u2s = u2.dropDuplicates()
//      println("Contratos-Expedientes {origen, cfinca, cptoserv, cderind} sin repeticiones = " + u2s.count() + " registros")
//      u2s.show(20)



















//     val t1 = sql(
//        """
//        SELECT cnifdnic,dnombcli,dapersoc, count(ccliente) AS contador_ccliente FROM clientes GROUP BY cnifdnic,dnombcli,dapersoc
//      """)
//      t1.persist(nivel)
//      t1.createOrReplaceTempView("t1")
//      println("\n Número de ccliente's asociado a cada cliente = " + t1.count() + "\n")
////      t1.show(10)
//
//      val t1_1 = sql(
//        """
//        SELECT cnifdnic,dnombcli,dapersoc, contador_ccliente FROM t1 WHERE contador_ccliente > 1
//      """)
//      println("\n Clientes con más de un ccliente = " + t1_1.count() + "\n")
////      t1_1.show(20)
//
//      val t2 = sql(
//        """
//        SELECT cnifdnic,dnombcli,dapersoc, count(ccontrat) AS contador_ccontrat FROM clientes GROUP BY cnifdnic,dnombcli,dapersoc
//      """)
//      t2.persist(nivel)
//      t2.createOrReplaceTempView("t2")
//      println("\n Número de ccontrat's asociado a cada cliente = " + t2.count() + "\n")
////      t2.show(10)
//
//      val t2_1 = sql(
//        """
//        SELECT cnifdnic,dnombcli,dapersoc, contador_ccontrat FROM t2 WHERE contador_ccontrat > 1
//      """)
//      println("\n Clientes con más de un ccontrat = " + t2_1.count() + "\n")
////      t2_1.show(20)
//
//      val t2_2 = sql(
//        """
//        SELECT cnifdnic,dnombcli,dapersoc, contador_ccontrat FROM t2 WHERE contador_ccontrat = 1
//      """)
//      println("\n Clientes con un ccontrat = " + t2_2.count() + "\n")
//
//
//      val cli_con_r = sql(
//        """
//          SELECT clientes.cnifdnic,clientes.dapersoc,clientes.dnombcli,contratos.origen,contratos.cfinca,contratos.cptoserv,contratos.cderind
//          FROM clientes JOIN contratos
//          ON clientes.origen=contratos.origen AND clientes.cemptitu=contratos.cemptitu AND clientes.ccontrat=contratos.ccontrat AND clientes.cnumscct=contratos.cnumscct
//        """)
//      cli_con_r.persist(nivel)
//      cli_con_r.createOrReplaceTempView("cliconr")
//      println("\n Join Clientes Contratos con repeticiones = " + cli_con_r.count() + "\n")
////      cli_con_r.show(10)
//
//      val cli_con = cli_con_r.dropDuplicates()
//      cli_con.persist(nivel)
//      cli_con.createOrReplaceTempView("clicon")
//      println("\n Join Clientes Contratos sin repeticiones = " + cli_con.count() + "\n")
////      cli_con.show(20)









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


//      val cli_con_exp_r = sql(
//        """
//          SELECT clicon.cnifdnic, clicon.dapersoc, clicon.dnombcli, expedientes.origen,expedientes.cfinca ,expedientes.cptoserv,  expedientes.cderind,expedientes.irregularidad, expedientes.fapexpd, expedientes.finifran,expedientes.ffinfran
//          FROM clicon JOIN expedientes
//          ON clicon.origen = expedientes.origen AND clicon.cfinca = expedientes.cfinca AND clicon.cptoserv = expedientes.cptoserv AND clicon.cderind = expedientes.cderind
//        """
//      )
//      cli_con_exp_r.persist(nivel)
//      cli_con_exp_r.createOrReplaceTempView("cliconexpr")
//      println("\n Join Clientes Contratos Expedientes con repeticiones = " + cli_con_exp_r.count() + "\n")
////      cli_con_exp_r.show(40)
//
//
//      val cli_con_exp = cli_con_exp_r.dropDuplicates()
//      cli_con_exp.persist(nivel)
//      cli_con_exp.createOrReplaceTempView("cliconexp")
//      println("\n Join Clientes Contratos Expedientes sin repeticiones = " + cli_con_exp.count() + "\n")
////      cli_con_exp.show(40)
//
//      val w1 = sql(
//        """SELECT irregularidad,fapexpd,finifran,ffinfran, count(cnifdnic)
//           FROM cliconexp
//           GROUP BY irregularidad, fapexpd , finifran,  ffinfran           """)
//
//      println("\n Contador de cnifdnic por registro de expedientes = " + w1.count() + "\n")
//      w1.show(40)
//
//      val w2 = sql(
//        """SELECT origen,cfinca,cptoserv,cderind, count(cnifdnic)
//           FROM cliconexp
//           GROUP BY origen,cfinca,cptoserv,cderind           """)
//
//      println("\n Contador de cnifdnic por tuplas de enlace= " + w2.count() + "\n")
//      w2.show(40)








    }


  }

}
