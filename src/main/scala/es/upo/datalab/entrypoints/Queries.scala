package es.upo.datalab.entrypoints

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/04/17.
  */
object Queries {

  val nivel = StorageLevel.MEMORY_AND_DISK

  val sqlContext = SparkSessionUtils.sqlContext

  import sqlContext._

  TimingUtils.time {

    val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
    df_16.createOrReplaceTempView("expedientes")

    val q1 = sql("""SELECT cfinca, fapexpd, finifran, ffinfran, fnormali, fciexped FROM expedientes""")
    q1.show(20,true)





  }



}
