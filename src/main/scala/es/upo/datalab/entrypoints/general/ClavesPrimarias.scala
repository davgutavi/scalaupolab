package es.upo.datalab.entrypoints.general

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 26/04/17.
  */
object ClavesPrimarias {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      //*************************************************************************************************************************TAB_00C Maestro Contratos

//            val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
//            df_00C.persist(nivel)
//            df_00C.createOrReplaceTempView("MaestroContratos")
//
//            val endesa_00C = sql(
//              """SELECT origen, cfinca, cptoserv, cderind, cupsree, cemptitu, ccontrat, cnumscct, count(DISTINCT *) as sum
//                 FROM MaestroContratos
//                 GROUP BY origen, cfinca, cptoserv, cderind, cupsree, cemptitu, ccontrat, cnumscct
//                 HAVING sum > 1
//                           """)
//            println("Clave primaria de Endesa:")
//            endesa_00C.show(10,false)

//            sql("""SELECT origen, cfinca, cptoserv, cderind, cupsree, cemptitu, ccontrat, cnumscct, cptocred, ccounips, cupsree2, cpuntmed, tpuntmed, vparsist, fpsercon, ffinvesu FROM MaestroContratos
//               WHERE cupsree = "ES0031405656038001WR0F" """).show(100, false)

//            val upo_00C = sql(
//              """SELECT cupsree, cpuntmed, tpuntmed, ccontrat, cnumscct, fpsercon, ffinvesu, count(DISTINCT *) as sum
//                 FROM MaestroContratos
//                 GROUP BY cupsree, cpuntmed, tpuntmed, ccontrat, cnumscct, fpsercon, ffinvesu
//                 HAVING sum > 1
//                           """)
//
//
//            sql("""SELECT cupsree, cpuntmed, tpuntmed, ccontrat, cnumscct, fpsercon, ffinvesu, origen, cptocred, cfinca, cptoserv, cderind, ccounips, cupsree2, vparsist FROM MaestroContratos
//             WHERE cupsree = "ES0031405656038001WR0F" """).show(100, false)
//
//            println("Clave primaria de UPO:")
//            upo_00C.show(30,false)

      //********************************************************************************************************************************TAB_05C Clientes

//            val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, true)
//            df_05C.persist(nivel)
//            df_05C.createOrReplaceTempView("Clientes")
//
//            println("Clave primaria de Endesa:")
//
//            val endesa_05C = sql(
//              """SELECT origen, cemptitu, ccontrat, cnumscct, cnifdnic, dnombcli, count(DISTINCT *) as sum
//                 FROM Clientes
//                 GROUP BY origen, cemptitu, ccontrat, cnumscct, cnifdnic, dnombcli
//                 HAVING sum > 1
//                        """)
//
//            endesa_05C.show(30,false)

//            sql("""SELECT origen, cemptitu, ccontrat, cnumscct, cnifdnic, dnombcli, ccliente, fechamov, tindfiju, dapersoc FROM Clientes WHERE cnifdnic = "39114590P" """).show(100, false)
//
//            println("Clave primaria de UPO:")
//
//            val upo_05C = sql(
//              """SELECT cemptitu, ccontrat, cnumscct, cnifdnic, ccliente, fechamov, count(DISTINCT *) as sum
//                 FROM Clientes
//                 GROUP BY cemptitu, ccontrat, cnumscct, cnifdnic, ccliente, fechamov
//                 HAVING sum > 1
//                           """)
//            upo_05C.show(30,false)
      //
      //      sql("""SELECT cemptitu, ccontrat, cnumscct, cnifdnic, ccliente, fechamov, origen, tindfiju, dapersoc, dnombcli  FROM Clientes WHERE cnifdnic = "39114590P" """).show(100, false)


      //*************************************************************************************************************************TAB_16 Expedientes


//            val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
//            df_16.persist(nivel)
//            df_16.createOrReplaceTempView("Expedientes")
//      //
//      //      //      PK Endesa: origen, cfinca, cptoserv, cderind, csecexpe
//      //      //      PK UPO:    cfinca, cptoserv, csecexpe, fapexpd, fciexped
//      //
//      //
//
//      println("Clave primaria de Endesa:")
//            val endesa_16 = sql(
//              """SELECT origen, cfinca, cptoserv, cderind, csecexpe, count(DISTINCT *) as sum
//                       FROM Expedientes
//                       GROUP BY origen, cfinca, cptoserv, cderind, csecexpe
//                       HAVING sum > 1
//                                 """)
//
//            endesa_16.show(30, false)
//
//      println("Clave primaria de UPO:")
//            val upo_16 = sql(
//              """SELECT cfinca, cptoserv, csecexpe, fapexpd, fciexped, count(DISTINCT *) as sum
//                       FROM Expedientes
//                       GROUP BY cfinca, cptoserv, csecexpe, fapexpd, fciexped
//                       HAVING sum > 1
//                                 """)
//
//
//            upo_16.show(30, false)

      //*********************************************************************************************************************************************TAB_03 Consumos Tipo I-IV

//            val df_03 = LoadTable.loadTable(TabPaths.TAB_03, TabPaths.TAB_03_headers)
//            df_03.persist(nivel)
//            df_03.createOrReplaceTempView("Consumos14")
      //
      //      //      PK Endesa: origen, cemptitu, ccontrat, vnumscpc, vnumscfa
      //      //      PK UPO: ccontrat, vnumscpc, vnumscfa, vsecresu, cpuntmed, ffinratr
      //
      //
//            println("Clave primaria de Endesa:")
//
//            val endesa_03 = sql(
//              """SELECT origen, cemptitu, ccontrat, vnumscpc, vnumscfa, count(DISTINCT *) as sum
//                             FROM Consumos14
//                             GROUP BY origen, cemptitu, ccontrat, vnumscpc, vnumscfa
//                             HAVING sum > 1
//                                       """)
//
//            endesa_03.show(30, false)
      //
      //      sql(
      //        """SELECT *  FROM Consumos14
      //                     WHERE ccontrat = "130008063145" AND vnumscpc = "004" AND vnumscfa = "001" AND cpuntmed = "CZZ542899901101" """).show(100, false)


//            println("Clave primaria de UPO:")
//
//            val upo_03 = sql(
//              """SELECT ccontrat, vnumscpc, vnumscfa, vsecresu, cpuntmed, ffinratr, count(DISTINCT *) as sum
//                             FROM Consumos14
//                             GROUP BY ccontrat, vnumscpc, vnumscfa, vsecresu, cpuntmed, ffinratr
//                             HAVING sum > 1                                 """)
//
//            upo_03.show(30, false)

      //                  sql("""SELECT * FROM Consumos14
      //                         WHERE ccontrat = "130057767707" AND vnumscpc = "008" AND vnumscfa = "001" AND vsecresu = "01" AND cpuntmed = "CZZ544523601101"""").show(100, false)


      //*********************************************************************************************************************************************TAB_04 Consumos Tipo V

//                  val df_04 = LoadTable.loadTable(TabPaths.TAB_04_14, TabPaths.TAB_04_headers)
//            df_04.persist(nivel)
//            df_04.createOrReplaceTempView("Consumos5")
      //
      //            //      PK Endesa: origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr
      //            //      PK UPO:    ccontrat, vnumscpc, vnumscfa, vnumscca, tintegr


//                  println("Clave primaria de Endesa:")
//
//                  val endesa_03 = sql(
//                    """SELECT origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr, count(DISTINCT *) as sum
//                                   FROM Consumos5
//                                   GROUP BY origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr
//                                   HAVING sum > 1
//                                   ORDER BY sum DESC
//                                             """)
//      //
//                  endesa_03.show(30, false)
//
//      //            sql(
//      //              """SELECT *  FROM Consumos14
//      //                           WHERE ccontrat = "130008063145" AND vnumscpc = "004" AND vnumscfa = "001" AND cpuntmed = "CZZ542899901101" """).show(100, false)
//
//
//                  println("Clave primaria de UPO:")
//
//                  val upo_03 = sql(
//                    """SELECT ccontrat, vnumscpc, vnumscfa, vnumscca, tintegr, count(DISTINCT *) as sum
//                                   FROM Consumos5
//                                   GROUP BY ccontrat, vnumscpc, vnumscfa, vnumscca, tintegr
//                                   HAVING sum > 1
//                                   ORDER BY sum DESC""")
//      //
//                  upo_03.show(30, false)

      //                        sql("""SELECT * FROM Consumos14
      //                               WHERE ccontrat = "130057767707" AND vnumscpc = "008" AND vnumscfa = "001" AND vsecresu = "01" AND cpuntmed = "CZZ544523601101"""").show(100, false)


      //*************************************************************************************************************************TAB_5D Clientes PTOSE

//                  val df_05D = LoadTable.loadTable(TabPaths.TAB_05D, TabPaths.TAB_05D_headers)
//            df_05D.persist(nivel)
//            df_05D.createOrReplaceTempView("ClientesPTOSE")

      //      PK Endesa:
      //      PK UPO:    ccliente, ccounips


      //                  println("Clave primaria de Endesa:")
      //
      //                  val endesa_03 = sql(
      //                    """SELECT origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr, count(DISTINCT *) as sum
      //                                   FROM ClientesPTOSE
      //                                   GROUP BY origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr
      //                                   HAVING sum > 1
      //                                   ORDER BY sum DESC
      //                                             """)
      //
      //                  endesa_03.show(30, false)
      //
      //                  sql(
      //                    """SELECT *  FROM ClientesPTOSE
      //                                 WHERE ccontrat = "130008063145" AND vnumscpc = "004" AND vnumscfa = "001" AND cpuntmed = "CZZ542899901101" """).show(100, false)


//                        println("Clave primaria de UPO:")
//
//                        val upo_03 = sql(
//                          """SELECT ccliente, ccounips, count(DISTINCT *) as sum
//                                         FROM ClientesPTOSE
//                                         GROUP BY ccliente, ccounips
//                                         HAVING sum > 1
//                                         ORDER BY sum DESC""")
//      //
//                        upo_03.show(30, false)
      //
      //                              sql("""SELECT * FROM ClientesPTOSE
      //                                     WHERE ccliente = "115417922" AND cnifdnic = "36328248R" """).show(100, false)


      //*************************************************************************************************************************TAB_5B Geolocalización


//                        val df_05B = LoadTable.loadTable(TabPaths.TAB_05B, TabPaths.TAB_05B_headers)
//            df_05B.persist(nivel)
//            df_05B.createOrReplaceTempView("Geolocalizacion")

      // PK Endesa:
      // PK UPO:    cfinca


      //                        println("Clave primaria de Endesa:")
      //
      //                        val endesa_03 = sql(
      //                          """SELECT origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr, count(DISTINCT *) as sum
      //                                         FROM ClientesPTOSE
      //                                         GROUP BY origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr
      //                                         HAVING sum > 1
      //                                         ORDER BY sum DESC
      //                                                   """)
      //
      //                        endesa_03.show(30, false)
      //
      //                        sql(
      //                          """SELECT *  FROM ClientesPTOSE
      //                                       WHERE ccontrat = "130008063145" AND vnumscpc = "004" AND vnumscfa = "001" AND cpuntmed = "CZZ542899901101" """).show(100, false)

      //
      //                        println("Clave primaria de UPO:")
      //
//                              val upo_03 = sql(
//                                """SELECT cfinca, count(DISTINCT *) as sum
//                                               FROM Geolocalizacion
//                                               GROUP BY cfinca
//                                               HAVING sum > 1
//                                               ORDER BY sum DESC""")
      //
//                              upo_03.show(30, false)

      //                                    sql("""SELECT * FROM Geolocalizacion
      //                                           WHERE ccliente = "115417922" AND cnifdnic = "36328248R" """).show(100, false)


      //*************************************************************************************************************************TAB_15A TDC


      val df_15A = LoadTable.loadTable(TabPaths.TAB_15A, TabPaths.TAB_15A_headers)
      df_15A.persist(nivel)
      df_15A.createOrReplaceTempView("TDC")

      //       PK Endesa:
      //       PK UPO:    cemptitu, ccontrat, csecutdc


      //                              println("Clave primaria de Endesa:")
      //
//                                    val endesa_03 = sql(
//                                      """SELECT origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr, count(DISTINCT *) as sum
//                                                     FROM ClientesPTOSE
//                                                     GROUP BY origen, cemptitu, ccontrat, vnumscpc, vnumscca, tintegr
//                                                     HAVING sum > 1
//                                                     ORDER BY sum DESC
//                                                               """)
//
//                                    endesa_03.show(30, false)
      //
      //                              sql(
      //                                """SELECT *  FROM ClientesPTOSE
      //                                             WHERE ccontrat = "130008063145" AND vnumscpc = "004" AND vnumscfa = "001" AND cpuntmed = "CZZ542899901101" """).show(100, false)
      //

      println("Clave primaria de UPO:")

      val upo_03 = sql(
        """SELECT cemptitu, ccontrat, csecutdc, count(DISTINCT *) as sum
                                               FROM TDC
                                               GROUP BY cemptitu,ccontrat, csecutdc
                                               HAVING sum > 1
                                               ORDER BY sum DESC""")

      upo_03.show(30, false)

      //                                          sql("""SELECT * FROM TDC
      //                                                 WHERE ccontrat = "140049193125" AND csecutdc = "001" """).show(100, false)


      //*************************************************************************************************************************TAB_00E Maestro Aparatos

      //            val df_00E = LoadTable.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers)
      //      df_00E.persist(nivel)
      //      df_00E.createOrReplaceTempView("MaestroAparatos")
      //
      ////      PK Endesa: cpuntmed, csecptom
      ////      PK UPO: cupsree, cpuntmed, csecptom, fbajapm
      //
      //
      //            val endesa_00E = sql(
      //              """SELECT cpuntmed, csecptom, count(*) as sum
      //                 FROM MaestroAparatos
      //                 GROUP BY cpuntmed, csecptom
      //                 HAVING sum > 1
      //                           """)
      //            println("Clave primaria de Endesa:")
      //            endesa_00E.show(30,false)
      //
      //          sql("""SELECT cpuntmed, csecptom, origen, cupsree, cupsree2, fvigorpm, fbajapm, caparmed  FROM MaestroAparatos
      //               WHERE cpuntmed = "CZZ612826300100" AND csecptom = 13 """).show(100, false)
      //
      //            sql("""SELECT cpuntmed, csecptom, origen, cupsree, cupsree2, fvigorpm, fbajapm, caparmed  FROM MaestroAparatos
      //               WHERE cpuntmed = "CZZ609196600601" AND csecptom = 2 """).show(100, false)
      //
      //            val upo_00E = sql(
      //              """SELECT cpuntmed, csecptom, fbajapm, count(*) as sum
      //                 FROM MaestroAparatos
      //                 GROUP BY cpuntmed, csecptom, fbajapm
      //                 HAVING sum > 1
      //                           """)
      //      println("Clave primaria de UPO:")
      //      upo_00E.show(30,false)
      //
      //      sql("""SELECT cpuntmed, csecptom, fbajapm, origen, cupsree, cupsree2, fvigorpm, caparmed FROM MaestroAparatos
      //             WHERE cpuntmed = "CZZ609196600601" AND csecptom = 2 """).show(100, false)
      //
      //            sql("""SELECT cpuntmed, csecptom, fbajapm, origen, cupsree, cupsree2, fvigorpm, caparmed FROM MaestroAparatos
      //             WHERE cpuntmed = "CZZ612826300100" AND csecptom = 13 """).show(100, false)


      //*********************************************************************************************************************************************TAB_00E Maestro Aparatos

      //                  val df_01 = LoadTable.loadTable(TabPaths.TAB_01_10, TabPaths.TAB_01_headers)
      //      df_01.persist(nivel)
      //      df_01.createOrReplaceTempView("CurvasCarga")

      //      PK Endesa: origen, cpuntmed, flectreg, obiscode, vsecccar
      //      PK UPO: cpuntmed, flectreg, testcaco, obiscode, vsecccar


      //
      //      println("Clave primaria de Endesa:")
      //                  val endesa_01 = sql(
      //                    """SELECT origen, cpuntmed, flectreg, obiscode, vsecccar, count(DISTINCT *) as sum
      //                       FROM CurvasCarga
      //                       GROUP BY origen, cpuntmed, flectreg, obiscode, vsecccar
      //                       HAVING sum > 1
      //                                 """)
      //
      //                  endesa_01.show(30,false)

      //                sql("""SELECT *  FROM CurvasCarga
      //                     WHERE cpuntmed = "CZZ629707600100" """).show(100, false)
      //
      //                  sql("""SELECT *  FROM CurvasCarga
      //                     WHERE cpuntmed = "CZZ512293800100" """).show(100, false)

      //                  val upo_01 = sql(
      //                    """SELECT cpuntmed, flectreg, testcaco, obiscode, vsecccar, count(DISTINCT *) as sum
      //                       FROM CurvasCarga
      //                       GROUP BY cpuntmed, flectreg, testcaco, obiscode, vsecccar
      //                       HAVING sum > 1
      //                                 """)
      //            println("Clave primaria de UPO:")
      //            upo_01.show(30,false)

      //            sql("""SELECT * FROM CurvasCarga
      //                   WHERE cpuntmed = "CZZ629707600100" """).show(100, false)
      //
      //                  sql("""SELECT * FROM CurvasCarga
      //                   WHERE cpuntmed = "CZZ512293800100"  """).show(100, false)


      //*********************************************************************************************************************************************TAB_02 Bits de Calidad

      //           val df_02 = LoadTable.loadTable(TabPaths.TAB_02, TabPaths.TAB_02_headers)
      //            df_02.persist(nivel)
      //            df_02.createOrReplaceTempView("BitsCalidad")
      //
      ////      df_02.show(200,false)
      //
      //                  //      PK Endesa:
      //                  //      PK UPO:     cpuntmed, flectreg, vsecccar, hora
      //
      //
      //                  println("Clave primaria de UPO:")
      //
      //                  val upo_02 = sql(
      //                    """SELECT cpuntmed, flectreg, vsecccar, hora, count(DISTINCT *) as sum
      //                                   FROM BitsCalidad
      //                                   GROUP BY cpuntmed, flectreg, vsecccar, hora
      //                                   HAVING sum > 1
      //                                   ORDER BY sum DESC
      //                                             """)
      //
      //                  upo_02.show(30, false)
      //
      //                  sql(
      //                    """SELECT *  FROM BitsCalidad
      //                                 WHERE cpuntmed = "CZZ544523300201" AND flectreg = "2012-10-28" AND vsecccar = "01" AND hora = "000003" """).show(100, false)


    }

    SparkSessionUtils.sc.stop()
  }

}
