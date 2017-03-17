package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}

/**
  * Created by davgutavi on 15/03/17.
  */
object Main01 {

  def main( args:Array[String] ):Unit = {

    //val df = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)

    //val df = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)

    //val df = LoadTable.loadTable(TabPaths.TAB_01_10,TabPaths.TAB_01_headers)

    //val df = LoadTable.loadTable(TabPaths.TAB_02_14,TabPaths.TAB_02_headers)

    //val df = LoadTable.loadTable(TabPaths.TAB_03,TabPaths.TAB_03_headers)





    val df = LoadTable.loadTable(TabPaths.TAB_04_10,TabPaths.TAB_04_headers)

    df.printSchema()

    df.show(20)

  }

}
