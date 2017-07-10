package es.upo.datalab.entrypoints.general


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/04/17.
  */
object ContadorRegistros {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    TimingUtils.time {

//      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
//      val df_00Cs = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers, false)
//      val c0C = df_00C.count()
//      val c0Cs = df_00Cs.count()
//      println("Número de registros en Maestro Contratos = " + c0C)
//      println("Número de registros en Maestro Contratos sin Repetición = " + c0Cs)
//      println("Diferencia = " + (c0C - c0Cs) + "\n")
//
//      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers)
//      val df_00Es = LoadTable.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers, false)
//      val c0E = df_00E.count()
//      val c0Es = df_00Es.count()
//      println("Número de registros en Maestro Aparatos = " + c0E)
//      println("Número de registros en Maestro Aparatos sin Repetición = " + c0Es)
//      println("Diferencia = " + (c0E - c0Es) + "\n")
//
//      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
//      val df_05Cs = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, false)
//      val c05C = df_05C.count()
//      val c05Cs = df_05Cs.count()
//      println("Número de registros en Clientes = " + c05C)
//      println("Número de registros en Clientes sin Repetición = " + c05Cs)
//      println("Diferencia = " + (c05C - c05Cs) + "\n")
//
//      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
//      val df_16s = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers, false)
//      val c16 = df_16.count()
//      val c16s = df_16s.count()
//      println("Número de registros en Expedientes = " + c16)
//      println("Número de registros en Expedientes sin Repetición = " + c16s)
//      println("Diferencia = " + (c16 - c16s) + "\n")
//
//      val df_01 = LoadTable.loadTable(TabPaths.TAB_01, TabPaths.TAB_01_headers)
//      val df_01s = LoadTable.loadTable(TabPaths.TAB_01, TabPaths.TAB_01_headers, false)
//      val c01 = df_01.count()
//      val c01s = df_01s.count()
//      println("Número de registros en Curvas de Carga = " + c01)
//      df_01.show(40,false)
//      println("Número de registros en Curvas de Carga sin Repetición = " + c01s)
//      df_01s.show(20,false)
//      println("Diferencia = " + (c01 - c01s) + "\n")

      val df_02 = LoadTableCsv.loadTable(TabPaths.TAB02, TabPaths.TAB02_headers)
      val df_02s = LoadTableCsv.loadTable(TabPaths.TAB02, TabPaths.TAB02_headers,dropDuplicates = true)
      val c02 = df_02.count()
      val c02s = df_02s.count()
      df_02.show(40,truncate = false)
      df_02s.show(20,truncate = false)
      println("Número de registros en Bits de Calidad = " + c02)
      println("Número de registros en Bits de Calisdad sin Repetición = " + c02s)
      println("Diferencia = " + (c02 - c02s) + "\n")










//      val df_03 = LoadTable.loadTable(TabPaths.TAB_03, TabPaths.TAB_03_headers)
//      val df_03s = LoadTable.loadTable(TabPaths.TAB_03, TabPaths.TAB_03_headers, false)
//      val c03 = df_03.count()
//      val c03s = df_03s.count()
//      println("Número de registros en Consumos Tipo I - IV = " + c03)
//      println("Número de registros en Consumos Tipo I - IV sin Repetición = " + c03s)
//      println("Diferencia = " + (c03 - c03s) + "\n")
//      df_03s.show(20,false)


//      val df_04 = LoadTable.loadTable(TabPaths.TAB_04, TabPaths.TAB_04_headers)
//      val df_04s = LoadTable.loadTable(TabPaths.TAB_04, TabPaths.TAB_04_headers, false)
//      val c04 = df_04.count()
//      val c04s = df_04s.count()
//      println("Número de registros en Consumos Tipo V = " + c04)
//      println("Número de registros en Consumos Tipo V sin Repetición = " + c04s)
//      println("Diferencia = " + (c04 - c04s) + "\n")
//
//      val df_05B = LoadTable.loadTable(TabPaths.TAB_05B, TabPaths.TAB_05B_headers)
//      val df_05Bs = LoadTable.loadTable(TabPaths.TAB_05B, TabPaths.TAB_05B_headers, false)
//      val c05B = df_05B.count()
//      val c05Bs = df_05Bs.count()
//      println("Número de registros en Geolocalización = " + c05B)
//      println("Número de registros en Geolocalización sin Repetición = " + c05Bs)
//      println("Diferencia = " + (c05B - c05Bs) + "\n")
//      df_05Bs.show(20,false)
//
//      val df_15A = LoadTable.loadTable(TabPaths.TAB_15A, TabPaths.TAB_15A_headers)
//      val df_15As = LoadTable.loadTable(TabPaths.TAB_15A, TabPaths.TAB_15A_headers, false)
//      val c15A = df_15A.count()
//      val c15As = df_15As.count()
//      println("Número de registros en TDC = " + c15A)
//      println("Número de registros en TDC sin Repetición = " + c15As)
//      println("Diferencia = " + (c15A - c15As) + "\n")
//
//      val df_05D = LoadTable.loadTable(TabPaths.TAB_05D, TabPaths.TAB_05D_headers)
//      val df_05Ds = LoadTable.loadTable(TabPaths.TAB_05D, TabPaths.TAB_05D_headers, false)
//      val c05D = df_05D.count()
//      val c05Ds = df_05Ds.count()
//      println("Número de registros en Clientes PTOSE = " + c05D)
//      println("Número de registros en Clientes PTOSE sin Repetición = " + c05Ds)
//      println("Diferencia = " + (c05D - c05Ds) + "\n")

    }

    SparkSessionUtils.sc.stop()


  }

}
