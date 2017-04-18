package es.upo.datalab.entrypoints

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 7/04/17.
  */
object TabTest {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
      df_05C.persist(nivel)
      println("Número de registros en Clientes = " + df_05C.count())
      df_05C.show(50,truncate = false)

    }

  }

}
