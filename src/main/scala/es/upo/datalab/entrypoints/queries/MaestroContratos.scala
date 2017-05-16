package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 21/04/17.
  */
object MaestroContratos {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_00C = LoadTableCsv.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MaestroContratos")

      //      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu FROM MaestroContratos""").show(20,false)
      //
      //      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, ccounips, cupsree2, cpuntmed FROM MaestroContratos""").show(20,false)
      //
      //      sql("""SELECT ccontrat, cnumscct, cfinca, cptocred, cptoserv, cderind FROM MaestroContratos""").show(20,false)
      //
      //      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, ccounips, cupsree2, cpuntmed, cfinca, cptocred, cptoserv, cderind FROM MaestroContratos""").show(20,false)

      //      sql("""SELECT ccontrat, cnumscct, fpsercon, ffinvesu, cupsree, ccounips, cupsree2, cpuntmed, cfinca, cptocred, cptoserv, cderind, origen, cptocred, cpuntmed, tpuntmed, vparsist, cemptitu FROM MaestroContratos""").show(20,false)


      //      val q1 = sql(
      //        """SELECT ccontrat, cnumscct
      //          FROM MaestroContratos
      //          ORDER BY ccontrat, cnumscct
      //           """)
      //
      //      q1.show(20,false)
      //      val q1c = +q1.count()
      //
      //
      //      val q1s = q1.dropDuplicates()
      //
      //      q1s.show(20,false)
      //      val q1sc = +q1s.count()
      //
      //      println("q1 = "+q1c+" registros")
      //      println("q1s = "+q1sc+" registros")
      //      println("Diferencia = "+(q1c-q1sc))


      val q7 = sql(
        """SELECT ccontrat, cnumscct,fpsercon,ffinvesu, cupsree, cpuntmed, tpuntmed
                            FROM MaestroContratos
                            ORDER BY ccontrat, cnumscct
                             """)

      q7.show(20, truncate = false)
      val q7c = q7.count()

      val q7s = q7.dropDuplicates()

      q7s.show(20, truncate = false)
      val q7sc = q7s.count()

      println("q7 = " + q7c + " registros")
      println("q7s = " + q7sc + " registros")
      println("Diferencia = " + (q7c - q7sc))


//      val q8 = sql(
//        """SELECT ccontrat, cnumscct, ffinvesu, cupsree, cpuntmed, tpuntmed, count(*) as sum
//             FROM MaestroContratos
//             GROUP BY ccontrat, cnumscct, ffinvesu, cupsree, cpuntmed, tpuntmed
//              HAVING sum > 1
//         """)
//
//      q8.show(30, false)

//            sql(
//              """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
//                                  FROM MaestroContratos
//                                  WHERE  ccontrat = "130004120433"
//                                  ORDER BY cnumscct
//                                   """).show(20, false)
//
//      sql(
//        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
//                                  FROM MaestroContratos
//                                  WHERE  ccontrat = "170059111410"
//                                  ORDER BY cnumscct
//                                   """).show(20, false)
//
//      sql(
//        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
//                                  FROM MaestroContratos
//                                  WHERE  ccontrat = "170002125311"
//                                  ORDER BY cnumscct
//                                   """).show(20, false)


      //                  sql(
      //                    """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //                      FROM MaestroContratos
      //                      WHERE  ccontrat = "310013893684"
      //                      ORDER BY cnumscct
      //                       """).show(20,false)
      //
      ////
      //            sql(
      //              """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //                FROM MaestroContratos
      //                WHERE  ccontrat = "110000000041"
      //                ORDER BY cnumscct
      //                 """).show(20,false)
      //
      //
      //      sql(
      //        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //                FROM MaestroContratos
      //                WHERE  ccontrat = "110000000018"
      //                ORDER BY cnumscct
      //                 """).show(20,false)


      //      val q2 = sql(
      //        """SELECT ccontrat, cnumscct, cupsree
      //          FROM MaestroContratos
      //          ORDER BY ccontrat, cnumscct
      //           """)

      //q2.show(100,false)
      //val q2c = +q2.count()

      //      val q2s = q2.dropDuplicates()

      //      q2s.show(20,false)
      //      val q2sc = +q2s.count()
      //
      //      println("q2 = "+q2c+" registros")
      //      println("q2s = "+q2sc+" registros")
      //      println("Diferencia = "+(q2c-q2sc))


      //            val q5 = sql(
      //              """SELECT ccontrat, cnumscct, cupsree
      //                FROM MaestroContratos
      //                WHERE cnumscct = "001"
      //                ORDER BY cupsree, ccontrat, cnumscct
      //                 """)

      //      q.show(200,false)


      //      val q3 = sql(
      //        """SELECT ccontrat, cnumscct, cupsree
      //          FROM MaestroContratos
      //          WHERE cnumscct = "001"
      //          ORDER BY cupsree, ccontrat, cnumscct
      //           """)
      //
      //      q3.show(200,false)


      //            val q4 = sql(
      //              """SELECT ccontrat, cnumscct, cupsree, tpuntmed
      //                FROM MaestroContratos
      //                ORDER BY ccontrat, cnumscct
      //                 """)
      //
      //      q4.show(100,false)
      //      val q4c = q4.count()
      //
      //            val q4s = q4.dropDuplicates()
      //
      //            q4s.show(20,false)
      //            val q4sc = +q4s.count()
      //
      //            println("q4 = "+q4c+" registros")
      //            println("q4s = "+q4sc+" registros")
      //            println("Diferencia = "+(q4c-q4sc))


      //            val q6 = sql(
      //              """SELECT tpuntmed, cupsree, ccontrat, cnumscct
      //                FROM MaestroContratos
      //                WHERE cnumscct = "001"
      //                ORDER BY tpuntmed, cupsree, ccontrat, cnumscct
      //                 """)
      //
      //            q6.show(200,false)

      //                  val q7 = sql(
      //                    """SELECT ccontrat, cnumscct, cupsree, cpuntmed, tpuntmed
      //                      FROM MaestroContratos
      //                      ORDER BY ccontrat, cnumscct
      //                       """)
      //
      //      q7.show(100,false)
      //            val q7c = q7.count()
      //
      //                  val q7s = q7.dropDuplicates()
      //
      //                  q7s.show(20,false)
      //                  val q7sc = q7s.count()
      //
      //                  println("q7 = "+q7c+" registros")
      //                  println("q7s = "+q7sc+" registros")
      //                  println("Diferencia = "+(q7c-q7sc))


      //                  val q8 = sql(
      //                    """SELECT ccontrat, count(*)
      //                      FROM MaestroContratos
      //                      GROUP BY ccontrat
      //                      HAVING count(*)>1
      //                       """)
      //
      //                  q8.show(200,false)


      //      sql(
      //        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //          FROM MaestroContratos
      //          WHERE  ccontrat = "140049199150"
      //          ORDER BY cnumscct
      //           """).show(20,false)
      //
      //      sql(
      //        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //          FROM MaestroContratos
      //          WHERE  ccontrat = "140049198347"
      //          ORDER BY cnumscct
      //           """).show(20,false)
      //
      //      sql(
      //        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //          FROM MaestroContratos
      //          WHERE  ccontrat = "140049199735"
      //          ORDER BY cnumscct
      //           """).show(20,false)
      //
      //
      //      sql(
      //        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //          FROM MaestroContratos
      //          WHERE  ccontrat = "140049199847"
      //          ORDER BY cnumscct
      //           """).show(20,false)
      //
      //      sql(
      //        """SELECT ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, cupsree, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, cemptitu
      //          FROM MaestroContratos
      //          WHERE  ccontrat = "310000000018"
      //          ORDER BY cnumscct
      //           """).show(20,false)


      //      df_00C.show(100,false)
      //      df_00C.coalesce(1).limit(100).write.option("delimiter",";").option("header","true").csv(TabPaths.root+"datasets/sTAB_00C")
      //
      //      val q1 = sql("""SELECT origen, cemptitu, ccontrat, cnumscct FROM MaestroContratos""")
      //
      //      df_00C.unpersist()
      //
      //      q1.persist(nivel)
      //
      //      val w1 = q1.count()
      //
      //      println("origen, cemptitu, ccontrat, cnumscct")
      //      q1.show(5,false)
      //
      //      val q2 = q1.dropDuplicates()
      //
      //      q1.unpersist()
      //
      //      q2.persist(nivel)
      //
      //      val w2 = q2.count()
      //
      //      println("origen, cemptitu, ccontrat, cnumscct sin repetir")
      //      q2.show(5,false)
      //
      //      q2.unpersist()
      //
      //      println("\n\nRegistros = "+w1)
      //      println("Registros sin repetir = "+w2)
      //      println("Diferencia = "+(w1-w2))


    }

    SparkSessionUtils.sc.stop()

  }

}
