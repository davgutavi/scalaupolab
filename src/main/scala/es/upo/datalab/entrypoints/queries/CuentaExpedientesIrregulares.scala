package es.upo.datalab.entrypoints.queries

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 20/04/17.
  */
object CuentaExpedientesIrregulares {
  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("Expedientes")

      sql("""SELECT * FROM Expedientes WHERE irregularidad = 'S'""").show(8, false)

      df_16.unpersist()


    }


    SparkSessionUtils.sc.stop()


  }
}