package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 20/04/17.
  */
object CfincaPorExpediente {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_16 = LoadTableCsv.loadTable(TabPaths.TAB16, TabPaths.TAB16_headers)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("Expedientes")

      sql("""SELECT cfinca, fapexpd, finifran, ffinfran, fnormali, fciexped FROM Expedientes ORDER BY cfinca""").show(20, truncate = false)

      df_16.unpersist()

    }

    SparkSessionUtils.sc.stop()

  }

}
