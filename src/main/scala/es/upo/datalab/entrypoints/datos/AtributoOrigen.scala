package es.upo.datalab.entrypoints.datos


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/04/17.
  */
object AtributoOrigen {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql

    import sqlContext._

    TimingUtils.time {

//      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
//      df_00C.persist(nivel)
//      df_00C.createOrReplaceTempView("maestrocontratos")
//
//      val s_00C = sql("""SELECT * FROM maestrocontratos WHERE origen = 'S'""")
//      val f_00C = sql("""SELECT * FROM maestrocontratos WHERE origen = 'F'""")
//      val g_00C = sql("""SELECT * FROM maestrocontratos WHERE origen = 'G'""")
//      val u_00C = sql("""SELECT * FROM maestrocontratos WHERE origen = 'U'""")
//      val z_00C = sql("""SELECT * FROM maestrocontratos WHERE origen = 'Z'""")
//
//      println("Maestro Contratos: Origen S = " + s_00C.count() + " registros")
//      println("Maestro Contratos: Origen F = " + f_00C.count() + " registros")
//      println("Maestro Contratos: Origen G = " + g_00C.count() + " registros")
//      println("Maestro Contratos: Origen U = " + u_00C.count() + " registros")
//      println("Maestro Contratos: Origen Z = " + z_00C.count() + " registros\n")
//
//      df_00C.unpersist()
//
//      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers)
//      df_00E.persist(nivel)
//      df_00E.createOrReplaceTempView("maestroaparatos")
//
//      val s_00E = sql("""SELECT * FROM maestroaparatos WHERE origen = 'S'""")
//      val f_00E = sql("""SELECT * FROM maestroaparatos WHERE origen = 'F'""")
//      val g_00E = sql("""SELECT * FROM maestroaparatos WHERE origen = 'G'""")
//      val u_00E = sql("""SELECT * FROM maestroaparatos WHERE origen = 'U'""")
//      val z_00E = sql("""SELECT * FROM maestroaparatos WHERE origen = 'Z'""")
//
//      println("Maestro Aparatos: Origen S = " + s_00E.count() + " registros")
//      println("Maestro Aparatos: Origen F = " + f_00E.count() + " registros")
//      println("Maestro Aparatos: Origen G = " + g_00E.count() + " registros")
//      println("Maestro Aparatos: Origen U = " + u_00E.count() + " registros")
//      println("Maestro Aparatos: Origen Z = " + z_00E.count() + " registros\n")
//
//      df_00E.unpersist()
//
//      val df_05Cs = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, false)
//      df_05Cs.persist(nivel)
//      df_05Cs.createOrReplaceTempView("clientes")
//
//      val s_05Cs = sql("""SELECT * FROM clientes WHERE origen = 'S'""")
//      val f_05Cs = sql("""SELECT * FROM clientes WHERE origen = 'F'""")
//      val g_05Cs = sql("""SELECT * FROM clientes WHERE origen = 'G'""")
//      val u_05Cs = sql("""SELECT * FROM clientes WHERE origen = 'U'""")
//      val z_05Cs = sql("""SELECT * FROM clientes WHERE origen = 'Z'""")
//
//      println("Clientes: Origen S = " + s_05Cs.count() + " registros")
//      println("Clientes: Origen F = " + f_05Cs.count() + " registros")
//      println("Clientes: Origen G = " + g_05Cs.count() + " registros")
//      println("Clientes: Origen U = " + u_05Cs.count() + " registros")
//      println("Clientes: Origen Z = " + z_05Cs.count() + " registros\n")
//
//      df_05Cs.unpersist()
//
//      val df_05D = LoadTable.loadTable(TabPaths.TAB_05D, TabPaths.TAB_05D_headers)
//      df_05D.persist(nivel)
//      df_05D.createOrReplaceTempView("clientesptose")
//
//      val s_05D = sql("""SELECT * FROM clientesptose WHERE origen = 'S'""")
//      val f_05D = sql("""SELECT * FROM clientesptose WHERE origen = 'F'""")
//      val g_05D = sql("""SELECT * FROM clientesptose WHERE origen = 'G'""")
//      val u_05D = sql("""SELECT * FROM clientesptose WHERE origen = 'U'""")
//      val z_05D = sql("""SELECT * FROM clientesptose WHERE origen = 'Z'""")
//
//      println("Clientes PTOSE: Origen S = " + s_05D.count() + " registros")
//      println("Clientes PTOSE: Origen F = " + f_05D.count() + " registros")
//      println("Clientes PTOSE: Origen G = " + g_05D.count() + " registros")
//      println("Clientes PTOSE: Origen U = " + u_05D.count() + " registros")
//      println("Clientes PTOSE: Origen Z = " + z_05D.count() + " registros\n")
//
//      df_05D.unpersist()
//
//      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
//      df_16.persist(nivel)
//      df_16.createOrReplaceTempView("expedientes")
//
//      val s_16 = sql("""SELECT * FROM expedientes WHERE origen = 'S'""")
//      val f_16 = sql("""SELECT * FROM expedientes WHERE origen = 'F'""")
//      val g_16 = sql("""SELECT * FROM expedientes WHERE origen = 'G'""")
//      val u_16 = sql("""SELECT * FROM expedientes WHERE origen = 'U'""")
//      val z_16 = sql("""SELECT * FROM expedientes WHERE origen = 'Z'""")
//
//      println("Expedientes: Origen S = " + s_16.count() + " registros")
//      println("Expedientes: Origen F = " + f_16.count() + " registros")
//      println("Expedientes: Origen G = " + g_16.count() + " registros")
//      println("Expedientes: Origen U = " + u_16.count() + " registros")
//      println("Expedientes: Origen Z = " + z_16.count() + " registros\n")
//
//      df_16.unpersist()

//********************************************************TAB_01, Curvas de Carga

//      val s_01 = s_01_10.count() + s_01_11.count() + s_01_12.count() + s_01_13.count() + s_01_14.count() + s_01_15.count() + s_01_16.count()
//      val f_01 = f_01_10.count() + f_01_11.count() + f_01_12.count() + f_01_13.count() + f_01_14.count() + f_01_15.count() + f_01_16.count()
//      val g_01 = g_01_10.count() + g_01_11.count() + g_01_12.count() + g_01_13.count() + g_01_14.count() + g_01_15.count() + g_01_16.count()
//      val u_01 = u_01_10.count() + u_01_11.count() + u_01_12.count() + u_01_13.count() + u_01_14.count() + u_01_15.count() + u_01_16.count()
//      val z_01 = z_01_10.count() + z_01_11.count() + z_01_12.count() + z_01_13.count() + z_01_14.count() + z_01_15.count() + z_01_16.count()

//      var s_01 = 0l
//      var f_01 = 0l
//      var g_01 = 0l
//      var u_01 = 0l
//      var z_01 = 0l
//
//      val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10, TabPaths.TAB_01_headers)
//      df_01_10.persist(nivel)
//      df_01_10.createOrReplaceTempView("curvas10")
//
//      val s_01_10 = sql("""SELECT * FROM curvas10 WHERE origen = 'S'""")
//      val f_01_10 = sql("""SELECT * FROM curvas10 WHERE origen = 'F'""")
//      val g_01_10 = sql("""SELECT * FROM curvas10 WHERE origen = 'G'""")
//      val u_01_10 = sql("""SELECT * FROM curvas10 WHERE origen = 'U'""")
//      val z_01_10 = sql("""SELECT * FROM curvas10 WHERE origen = 'Z'""")
//
//      s_01 = s_01 + s_01_10.count()
//      f_01 = f_01 + f_01_10.count()
//      g_01 = g_01 + g_01_10.count()
//      u_01 = u_01 + u_01_10.count()
//      z_01 = z_01 + z_01_10.count()
//
//      println("Curvas de Carga 10: Origen S = " + s_01_10.count() + " registros")
//      println("Curvas de Carga 10: Origen F = " + f_01_10.count() + " registros")
//      println("Curvas de Carga 10: Origen G = " + g_01_10.count() + " registros")
//      println("Curvas de Carga 10: Origen U = " + u_01_10.count() + " registros")
//      println("Curvas de Carga 10: Origen Z = " + z_01_10.count() + " registros\n")
//
//      df_01_10.unpersist()
//
//      val df_01_11 = LoadTable.loadTable(TabPaths.TAB_01_11, TabPaths.TAB_01_headers)
//      df_01_11.persist(nivel)
//      df_01_11.createOrReplaceTempView("curvas11")
//
//      val s_01_11 = sql("""SELECT * FROM curvas11 WHERE origen = 'S'""")
//      val f_01_11 = sql("""SELECT * FROM curvas11 WHERE origen = 'F'""")
//      val g_01_11 = sql("""SELECT * FROM curvas11 WHERE origen = 'G'""")
//      val u_01_11 = sql("""SELECT * FROM curvas11 WHERE origen = 'U'""")
//      val z_01_11 = sql("""SELECT * FROM curvas11 WHERE origen = 'Z'""")
//
//      s_01 = s_01 + s_01_11.count()
//      f_01 = f_01 + f_01_11.count()
//      g_01 = g_01 + g_01_11.count()
//      u_01 = u_01 + u_01_11.count()
//      z_01 = z_01 + z_01_11.count()
//
//      println("Curvas de Carga 11: Origen S = " + s_01_11.count() + " registros")
//      println("Curvas de Carga 11: Origen F = " + f_01_11.count() + " registros")
//      println("Curvas de Carga 11: Origen G = " + g_01_11.count() + " registros")
//      println("Curvas de Carga 11: Origen U = " + u_01_11.count() + " registros")
//      println("Curvas de Carga 11: Origen Z = " + z_01_11.count() + " registros\n")
//
//      df_01_11.unpersist()
//
//      val df_01_12 = LoadTable.loadTable(TabPaths.TAB_01_12, TabPaths.TAB_01_headers)
//      df_01_12.persist(nivel)
//      df_01_12.createOrReplaceTempView("curvas12")
//
//      val s_01_12 = sql("""SELECT * FROM curvas12 WHERE origen = 'S'""")
//      val f_01_12 = sql("""SELECT * FROM curvas12 WHERE origen = 'F'""")
//      val g_01_12 = sql("""SELECT * FROM curvas12 WHERE origen = 'G'""")
//      val u_01_12 = sql("""SELECT * FROM curvas12 WHERE origen = 'U'""")
//      val z_01_12 = sql("""SELECT * FROM curvas12 WHERE origen = 'Z'""")
//
//      s_01 = s_01 + s_01_12.count()
//      f_01 = f_01 + f_01_12.count()
//      g_01 = g_01 + g_01_12.count()
//      u_01 = u_01 + u_01_12.count()
//      z_01 = z_01 + z_01_12.count()
//
//
//      println("Curvas de Carga 12: Origen S = " + s_01_12.count() + " registros")
//      println("Curvas de Carga 12: Origen F = " + f_01_12.count() + " registros")
//      println("Curvas de Carga 12: Origen G = " + g_01_12.count() + " registros")
//      println("Curvas de Carga 12: Origen U = " + u_01_12.count() + " registros")
//      println("Curvas de Carga 12: Origen Z = " + z_01_12.count() + " registros\n")
//
//      df_01_12.unpersist()
//
//      val df_01_13 = LoadTable.loadTable(TabPaths.TAB_01_13, TabPaths.TAB_01_headers)
//      df_01_13.persist(nivel)
//      df_01_13.createOrReplaceTempView("curvas13")
//
//      val s_01_13 = sql("""SELECT * FROM curvas13 WHERE origen = 'S'""")
//      val f_01_13 = sql("""SELECT * FROM curvas13 WHERE origen = 'F'""")
//      val g_01_13 = sql("""SELECT * FROM curvas13 WHERE origen = 'G'""")
//      val u_01_13 = sql("""SELECT * FROM curvas13 WHERE origen = 'U'""")
//      val z_01_13 = sql("""SELECT * FROM curvas13 WHERE origen = 'Z'""")
//
//      s_01 = s_01 + s_01_13.count()
//      f_01 = f_01 + f_01_13.count()
//      g_01 = g_01 + g_01_13.count()
//      u_01 = u_01 + u_01_13.count()
//      z_01 = z_01 + z_01_13.count()
//
//
//      println("Curvas de Carga 13: Origen S = " + s_01_13.count() + " registros")
//      println("Curvas de Carga 13: Origen F = " + f_01_13.count() + " registros")
//      println("Curvas de Carga 13: Origen G = " + g_01_13.count() + " registros")
//      println("Curvas de Carga 13: Origen U = " + u_01_13.count() + " registros")
//      println("Curvas de Carga 13: Origen Z = " + z_01_13.count() + " registros\n")
//
//      df_01_13.unpersist()
//
//      val df_01_14 = LoadTable.loadTable(TabPaths.TAB_01_14, TabPaths.TAB_01_headers)
//      df_01_14.persist(nivel)
//      df_01_14.createOrReplaceTempView("curvas14")
//
//      val s_01_14 = sql("""SELECT * FROM curvas14 WHERE origen = 'S'""")
//      val f_01_14 = sql("""SELECT * FROM curvas14 WHERE origen = 'F'""")
//      val g_01_14 = sql("""SELECT * FROM curvas14 WHERE origen = 'G'""")
//      val u_01_14 = sql("""SELECT * FROM curvas14 WHERE origen = 'U'""")
//      val z_01_14 = sql("""SELECT * FROM curvas14 WHERE origen = 'Z'""")
//
//      s_01 = s_01 + s_01_14.count()
//      f_01 = f_01 + f_01_14.count()
//      g_01 = g_01 + g_01_14.count()
//      u_01 = u_01 + u_01_14.count()
//      z_01 = z_01 + z_01_14.count()
//
//
//      println("Curvas de Carga 14: Origen S = " + s_01_14.count() + " registros")
//      println("Curvas de Carga 14: Origen F = " + f_01_14.count() + " registros")
//      println("Curvas de Carga 14: Origen G = " + g_01_14.count() + " registros")
//      println("Curvas de Carga 14: Origen U = " + u_01_14.count() + " registros")
//      println("Curvas de Carga 14: Origen Z = " + z_01_14.count() + " registros\n")
//
//      df_01_14.unpersist()
//
//
//      val df_01_15 = LoadTable.loadTable(TabPaths.TAB_01_15, TabPaths.TAB_01_headers)
//      df_01_15.persist(nivel)
//      df_01_15.createOrReplaceTempView("curvas15")
//
//      val s_01_15 = sql("""SELECT * FROM curvas15 WHERE origen = 'S'""")
//      val f_01_15 = sql("""SELECT * FROM curvas15 WHERE origen = 'F'""")
//      val g_01_15 = sql("""SELECT * FROM curvas15 WHERE origen = 'G'""")
//      val u_01_15 = sql("""SELECT * FROM curvas15 WHERE origen = 'U'""")
//      val z_01_15 = sql("""SELECT * FROM curvas15 WHERE origen = 'Z'""")
//
//      s_01 = s_01 + s_01_15.count()
//      f_01 = f_01 + f_01_15.count()
//      g_01 = g_01 + g_01_15.count()
//      u_01 = u_01 + u_01_15.count()
//      z_01 = z_01 + z_01_15.count()
//
//
//
//      println("Curvas de Carga 15: Origen S = " + s_01_15.count() + " registros")
//      println("Curvas de Carga 15: Origen F = " + f_01_15.count() + " registros")
//      println("Curvas de Carga 15: Origen G = " + g_01_15.count() + " registros")
//      println("Curvas de Carga 15: Origen U = " + u_01_15.count() + " registros")
//      println("Curvas de Carga 15: Origen Z = " + z_01_15.count() + " registros\n")
//
//      df_01_15.unpersist()
//
//      val df_01_16 = LoadTable.loadTable(TabPaths.TAB_01_16, TabPaths.TAB_01_headers)
//      df_01_16.persist(nivel)
//      df_01_16.createOrReplaceTempView("curvas16")
//
//      val s_01_16 = sql("""SELECT * FROM curvas16 WHERE origen = 'S'""")
//      val f_01_16 = sql("""SELECT * FROM curvas16 WHERE origen = 'F'""")
//      val g_01_16 = sql("""SELECT * FROM curvas16 WHERE origen = 'G'""")
//      val u_01_16 = sql("""SELECT * FROM curvas16 WHERE origen = 'U'""")
//      val z_01_16 = sql("""SELECT * FROM curvas16 WHERE origen = 'Z'""")
//
//      s_01 = s_01 + s_01_16.count()
//      f_01 = f_01 + f_01_16.count()
//      g_01 = g_01 + g_01_16.count()
//      u_01 = u_01 + u_01_16.count()
//      z_01 = z_01 + z_01_16.count()
//
//
//      println("Curvas de Carga 16: Origen S = " + s_01_16.count() + " registros")
//      println("Curvas de Carga 16: Origen F = " + f_01_16.count() + " registros")
//      println("Curvas de Carga 16: Origen G = " + g_01_16.count() + " registros")
//      println("Curvas de Carga 16: Origen U = " + u_01_16.count() + " registros")
//      println("Curvas de Carga 16: Origen Z = " + z_01_16.count() + " registros\n")
//
//      df_01_16.unpersist()
//
//
//      println("Curvas de Carga Total: Origen S = " + s_01 + " registros")
//      println("Curvas de Carga Total: Origen F = " + f_01 + " registros")
//      println("Curvas de Carga Total: Origen G = " + g_01 + " registros")
//      println("Curvas de Carga Total: Origen U = " + u_01 + " registros")
//      println("Curvas de Carga Total: Origen Z = " + z_01 + " registros\n")



//      val df_03 = LoadTable.loadTable(TabPaths.TAB_03, TabPaths.TAB_03_headers)
//      df_03.persist(nivel)
//      df_03.createOrReplaceTempView("consumos14")
//
//      val s_03 = sql("""SELECT * FROM consumos14 WHERE origen = 'S'""")
//      val f_03 = sql("""SELECT * FROM consumos14 WHERE origen = 'F'""")
//      val g_03 = sql("""SELECT * FROM consumos14 WHERE origen = 'G'""")
//      val u_03 = sql("""SELECT * FROM consumos14 WHERE origen = 'U'""")
//      val z_03 = sql("""SELECT * FROM consumos14 WHERE origen = 'Z'""")

//      val s_03 = df_03.where("origen = 'S'")
//      val f_03 = df_03.where("origen = 'F'")
//      val g_03 = df_03.where("origen = 'G'")
//      val u_03 = df_03.where("origen = 'U'")
//      val z_03 = df_03.where("origen = 'Z'")

//      println("Consumos Tipo I - IV: Origen S = " + s_03.count() + " registros")
//      println("Consumos Tipo I - IV: Origen F = " + f_03.count() + " registros")
//      println("Consumos Tipo I - IV: Origen G = " + g_03.count() + " registros")
//      println("Consumos Tipo I - IV: Origen U = " + u_03.count() + " registros")
//      println("Consumos Tipo I - IV: Origen Z = " + z_03.count() + " registros\n")
//
//      df_03.unpersist()

//      val df_04 = LoadTable.loadTable(TabPaths.TAB_04, TabPaths.TAB_04_headers)
//      df_04.persist(nivel)
//      df_04.createOrReplaceTempView("consumos5")
//
//      val s_04 = sql("""SELECT * FROM consumos5 WHERE origen = 'S'""")
//      val f_04 = sql("""SELECT * FROM consumos5 WHERE origen = 'F'""")
//      val g_04 = sql("""SELECT * FROM consumos5 WHERE origen = 'G'""")
//      val u_04 = sql("""SELECT * FROM consumos5 WHERE origen = 'U'""")
//      val z_04 = sql("""SELECT * FROM consumos5 WHERE origen = 'Z'""")
//
//      println("Consumos Tipo V: Origen S = " + s_04.count() + " registros")
//      println("Consumos Tipo V: Origen F = " + f_04.count() + " registros")
//      println("Consumos Tipo V: Origen G = " + g_04.count() + " registros")
//      println("Consumos Tipo V: Origen U = " + u_04.count() + " registros")
//      println("Consumos Tipo V: Origen Z = " + z_04.count() + " registros\n")
//
//      df_04.unpersist()

//      val df_05B = LoadTable.loadTable(TabPaths.TAB_05B, TabPaths.TAB_05B_headers)
//      df_05B.persist(nivel)
//      df_05B.createOrReplaceTempView("geolocalizacion")
//
//      val s_05B = sql("""SELECT * FROM geolocalizacion WHERE origen = 'S'""")
//      val f_05B = sql("""SELECT * FROM geolocalizacion WHERE origen = 'F'""")
//      val g_05B = sql("""SELECT * FROM geolocalizacion WHERE origen = 'G'""")
//      val u_05B = sql("""SELECT * FROM geolocalizacion WHERE origen = 'U'""")
//      val z_05B = sql("""SELECT * FROM geolocalizacion WHERE origen = 'Z'""")
//
//      println("Geolocalización: Origen S = " + s_05B.count() + " registros")
//      println("Geolocalización: Origen F = " + f_05B.count() + " registros")
//      println("Geolocalización: Origen G = " + g_05B.count() + " registros")
//      println("Geolocalización: Origen U = " + u_05B.count() + " registros")
//      println("Geolocalización: Origen Z = " + z_05B.count() + " registros\n")
//
//      df_05B.unpersist()
//
//      val df_15A = LoadTable.loadTable(TabPaths.TAB_15A, TabPaths.TAB_15A_headers)
//      df_15A.persist(nivel)
//      df_15A.createOrReplaceTempView("tdc")
//
//      val s_15A = sql("""SELECT * FROM tdc WHERE origen = 'S'""")
//      val f_15A = sql("""SELECT * FROM tdc WHERE origen = 'F'""")
//      val g_15A = sql("""SELECT * FROM tdc WHERE origen = 'G'""")
//      val u_15A = sql("""SELECT * FROM tdc WHERE origen = 'U'""")
//      val z_15A = sql("""SELECT * FROM tdc WHERE origen = 'Z'""")
//
//      println("TDC: Origen S = " + s_15A.count() + " registros")
//      println("TDC: Origen F = " + f_15A.count() + " registros")
//      println("TDC: Origen G = " + g_15A.count() + " registros")
//      println("TDC: Origen U = " + u_15A.count() + " registros")
//      println("TDC: Origen Z = " + z_15A.count() + " registros\n")
//
//      df_15A.unpersist()

//            val df_02 = LoadTableCsv.loadTable(TabPaths.TAB02, TabPaths.TAB02_headers)
//      df_02.persist(nivel)
//      df_02.createOrReplaceTempView("BitsCalidad")
//
//            val s_15A = sql("""SELECT * FROM BitsCalidad WHERE origen = 'S'""")
//            val f_15A = sql("""SELECT * FROM BitsCalidad WHERE origen = 'F'""")
//            val g_15A = sql("""SELECT * FROM BitsCalidad WHERE origen = 'G'""")
//            val u_15A = sql("""SELECT * FROM BitsCalidad WHERE origen = 'U'""")
//            val z_15A = sql("""SELECT * FROM BitsCalidad WHERE origen = 'Z'""")
//
//            println("BitsCalidad: Origen S = " + s_15A.count() + " registros")
//            println("BitsCalidad: Origen F = " + f_15A.count() + " registros")
//            println("BitsCalidad: Origen G = " + g_15A.count() + " registros")
//            println("BitsCalidad: Origen U = " + u_15A.count() + " registros")
//            println("BitsCalidad: Origen Z = " + z_15A.count() + " registros\n")
//
//      df_02.unpersist()

    }

    SparkSessionUtils.session.stop()
  }
}
