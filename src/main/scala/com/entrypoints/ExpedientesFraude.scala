package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 23/03/17.
  */
object ExpedientesFraude {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    import SparkSessionUtils.sqlContext.sql

    TimingUtils.time {

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      println("\n Número de registros en Maestro Contratos = " + df_00C.count() + "\n")
      df_00C.createOrReplaceTempView("contratos")

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
      df_05C.persist(nivel)
      println("\n Número de registros en Clientes = " + df_05C.count() + "\n")
      df_05C.createOrReplaceTempView("clientes")

      val df_05D = LoadTable.loadTable(TabPaths.TAB_05D, TabPaths.TAB_05D_headers)
      df_05D.persist(nivel)
      println("\n Número de registros en Clientes PTOSE = " + df_05D.count() + "\n")
      df_05D.createOrReplaceTempView("clientesptose")

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      println("\n Número de registros en Expedientes = " + df_16.count() + "\n")
      df_16.createOrReplaceTempView("expedientes")


      val con_cli = sql(
        """

                SELECT contratos.origen, contratos.cfinca, contratos.cptoserv, contratos.cderind ,clientes.ccliente, clientes.cnifdnic, clientes.dapersoc, clientes.dnombcli
                FROM contratos JOIN clientes
                ON contratos.origen=clientes.origen AND contratos.cemptitu=clientes.cemptitu AND contratos.ccontrat=clientes.ccontrat AND contratos.cnumscct=clientes.cnumscct

              """)
      println("\n Contratos-Clientes (" + con_cli.count() + " registros)\n")
      con_cli.createOrReplaceTempView("concli")
      con_cli.show(20)

      val concli_exp = sql(
        """

                SELECT concli.ccliente, concli.cnifdnic, concli.dapersoc, concli.dnombcli, expedientes.irregularidad, expedientes.anomalia, expedientes.finifran, expedientes.ffinfran, expedientes.fciexped
                FROM concli JOIN expedientes
                ON concli.origen=expedientes.origen AND concli.cfinca=expedientes.cfinca AND concli.cptoserv=expedientes.cptoserv AND concli.cderind=expedientes.cderind
                 """)
      println("\n Contratos-Clientes-Expedientes (" + concli_exp.count() + " registros)\n")
      concli_exp.createOrReplaceTempView("concliexp")
      concli_exp.show(20)

      val concli_exp_irregular = sql(
        """

                SELECT *
                FROM concliexp
                WHERE irregularidad = 'S'
                 """)
      println("\n Contratos-Clientes-Expedientes con Irregularidad (" + concli_exp_irregular.count() + " registros)\n")
      concli_exp_irregular.createOrReplaceTempView("concliexp")
      concli_exp_irregular.show(500)







      //      val exp1 = sql(
      //        """
      //            SELECT * FROM contratos WHERE origen = 'S'
      //
      //          """)
      //      println("\n Contratos S ("+exp1.count()+" registros)\n")
      //      exp1.show(5)
      //
      //      val exp2 = sql(
      //        """
      //            SELECT * FROM contratos WHERE origen = 'F'
      //
      //          """)
      //      println("\n Contratos origen F ("+exp2.count()+" registros)\n")
      //      exp2.show(5)
      //
      //      val exp3 = sql(
      //        """
      //            SELECT * FROM contratos WHERE origen = 'G'
      //
      //          """)
      //      println("\n Contratos origen G ("+exp3.count()+" registros)\n")
      //      exp3.show(5)
      //
      //      val exp4 = sql(
      //        """
      //            SELECT * FROM contratos WHERE origen = 'U'
      //
      //          """)
      //      println("\n Contratos origen U ("+exp4.count()+" registros)\n")
      //      exp4.show(5)
      //
      //      val exp5 = sql(
      //        """
      //            SELECT * FROM contratos WHERE origen = 'Z'
      //
      //          """)
      //      println("\n Contratos origen Z ("+exp5.count()+" registros)\n")
      //      exp5.show(5)

      //      val exp1 = sql(
      //        """
      //            SELECT * FROM clientes WHERE origen = 'S'
      //
      //          """)
      //      println("\n Clientes S ("+exp1.count()+" registros)\n")
      //      exp1.show(5)
      //
      //      val exp2 = sql(
      //        """
      //            SELECT * FROM clientes WHERE origen = 'F'
      //
      //          """)
      //      println("\n Clientes origen F ("+exp2.count()+" registros)\n")
      //      exp2.show(5)
      //
      //      val exp3 = sql(
      //        """
      //            SELECT * FROM clientes WHERE origen = 'G'
      //
      //          """)
      //      println("\n Clientes origen G ("+exp3.count()+" registros)\n")
      //      exp3.show(5)
      //
      //      val exp4 = sql(
      //        """
      //            SELECT * FROM clientes WHERE origen = 'U'
      //
      //          """)
      //      println("\n Clientes origen U ("+exp4.count()+" registros)\n")
      //      exp4.show(5)
      //
      //      val exp5 = sql(
      //        """
      //            SELECT * FROM clientes WHERE origen = 'Z'
      //
      //          """)
      //      println("\n Clientes origen Z ("+exp5.count()+" registros)\n")
      //      exp5.show(5)


      //      val exp1 = sql(
      //        """
      //            SELECT * FROM clientesptose WHERE origen = 'S'
      //
      //          """)
      //      println("\n Clientes PTOSE origen S ("+exp1.count()+" registros)\n")
      //      exp1.show(5)
      //
      //      val exp2 = sql(
      //        """
      //            SELECT * FROM clientesptose WHERE origen = 'F'
      //
      //          """)
      //      println("\n Clientes PTOSE origen F ("+exp2.count()+" registros)\n")
      //      exp2.show(5)
      //
      //      val exp3 = sql(
      //        """
      //            SELECT * FROM clientesptose WHERE origen = 'G'
      //
      //          """)
      //      println("\n Clientes PTOSE origen G ("+exp3.count()+" registros)\n")
      //      exp3.show(5)
      //
      //      val exp4 = sql(
      //        """
      //            SELECT * FROM clientesptose WHERE origen = 'U'
      //
      //          """)
      //      println("\n Clientes PTOSE origen U ("+exp4.count()+" registros)\n")
      //      exp4.show(5)
      //
      //      val exp5 = sql(
      //        """
      //            SELECT * FROM clientesptose WHERE origen = 'Z'
      //
      //          """)
      //      println("\n Clientes PTOSE origen Z ("+exp5.count()+" registros)\n")
      //      exp5.show(5)

      //        val exp1 = sql(
      //          """
      //            SELECT * FROM expedientes WHERE origen = 'S'
      //
      //          """)
      //            println("\n Expedientes origen S ("+exp1.count()+" registros)\n")
      //      exp1.show(5)
      //
      //      val exp2 = sql(
      //        """
      //            SELECT * FROM expedientes WHERE origen = 'F'
      //
      //          """)
      //      println("\n Expedientes origen F ("+exp2.count()+" registros)\n")
      //      exp2.show(5)
      //
      //      val exp3 = sql(
      //        """
      //            SELECT * FROM expedientes WHERE origen = 'G'
      //
      //          """)
      //      println("\n Expedientes origen G ("+exp3.count()+" registros)\n")
      //      exp3.show(5)
      //
      //      val exp4 = sql(
      //        """
      //            SELECT * FROM expedientes WHERE origen = 'U'
      //
      //          """)
      //      println("\n Expedientes origen U ("+exp4.count()+" registros)\n")
      //      exp4.show(5)
      //
      //      val exp5 = sql(
      //        """
      //            SELECT * FROM expedientes WHERE origen = 'Z'
      //
      //          """)
      //      println("\n Expedientes origen Z ("+exp5.count()+" registros)\n")
      //      exp5.show(5)


      //  val exp_irregulares = sql(
      //    """
      //      SELECT * FROM expedientes WHERE irregularidad = 'S'
      //
      //    """)
      //      println("\n Expedientes con irregularidad ("+exp_irregulares.count()+" registros)\n")
      //      exp_irregulares.show(50)


      //      val test8 = sql(
      //        """
      //          SELECT origen, ccounips, ccliente, cnifdnic, dnombcli, dapersoc
      //             FROM clientesptose
      //             ORDER BY origen DESC, ccounips DESC
      //              """)
      //      println("\n Sumador origen  ("+test8.count()+" registros)\n")
      //      test8.show(50)

      //      val test2 = sql(
      //        """
      //          SELECT *
      //             FROM test
      //             WHERE numccounips > 1
      //             """)
      //      println("\n Clientes PTOSE con más de un ccounips ("+test2.count()+" registros)\n")
      //       test2.show(50)
      //
      //
      //      val con_cli = sql("""
      //
      //          SELECT contratos.cfinca, contratos.cptoserv, contratos.cderind, clientesptose.ccliente, clientesptose.cnifdnic,clientesptose.dnombcli, clientesptose.dapersoc,contratos.origen, clientesptose.ccounips
      //          FROM contratos JOIN clientesptose
      //          ON contratos.ccounips=clientesptose.ccounips
      //
      //        """)
      //      println("\n Contratos-Clientes PTOSE ("+con_cli.count()+" registros)\n")
      //      con_cli.createOrReplaceTempView("concli")
      //      con_cli.show(20)
      //
      //
      //      val test3 = sql(
      //        """
      //          SELECT cfinca, cptoserv, cderind,ccliente, cnifdnic, dnombcli, dapersoc, origen, count(ccounips) AS numccounips
      //             FROM concli
      //             GROUP BY cfinca, cptoserv, cderind,ccliente, cnifdnic, dnombcli, dapersoc, origen
      //             """)
      //      println("\n Sumador ccounips  ("+test3.count()+" registros)\n")
      //      test3.show(50)
      //
      //
      //


      //      val concli_exp = sql("""
      //
      //          SELECT concli.ccliente, concli.dapersoc, concli.dnombcli, concli.cnifdnic, concli.ccounips, expedientes.origen, expedientes.cemptitu, expedientes.cfinca, expedientes.cptoserv,
      //           expedientes.cderind, expedientes.csecexpe, expedientes.fapexpd, expedientes.finifran, expedientes.ffinfran, expedientes.anomalia, expedientes.irregularidad,
      //           expedientes.venacord,expedientes.vennofai, expedientes.torigexp, expedientes.texpedie, expedientes.expclass, expedientes.testexpe,  expedientes.tpuntmed,
      //           expedientes.fnormali, expedientes.cplan, expedientes.ccampa, expedientes.cempresa, expedientes.fciexped
      //          FROM concli JOIN expedientes
      //          ON concli.origen=expedientes.origen AND concli.cfinca=expedientes.cfinca AND concli.cptoserv=expedientes.cptoserv AND concli.cderind=expedientes.cderind
      //          WHERE concli.ccounips = "CZZ8141947006"
      //
      //        """)
      //      println("\n Contratos-Clientes PTOSE-Expedientes con Irregularidad ("+concli_exp.count()+" registros)\n")
      //      concli_exp.show(30)


    }
  }

}
