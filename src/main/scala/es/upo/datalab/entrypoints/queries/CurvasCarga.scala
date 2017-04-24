package es.upo.datalab.entrypoints.queries

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 24/04/17.
  */
object CurvasCarga {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_01 = LoadTable.loadTable(TabPaths.TAB_01_10, TabPaths.TAB_01_headers)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CurvasCarga")

//      df_01.show(20,false)

      val q1 = sql(
        """SELECT flectreg,vsecccar,count(*) as sum
                         FROM CurvasCarga
                         GROUP BY flectreg,vsecccar
                         HAVING sum > 1
                         ORDER BY flectreg
                     """)

      q1.show(30, false)


      //                sql("""SELECT *
      //                      FROM CurvasCarga
      //                      WHERE cupsree = "ES0031406091966006JJ0F"
      //                           """).show(100,false)

    }

    SparkSessionUtils.sc.stop()
  }

}
