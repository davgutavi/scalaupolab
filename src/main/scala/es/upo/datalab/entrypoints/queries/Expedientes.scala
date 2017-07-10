package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 24/04/17.
  */
object Expedientes {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_16 = LoadTableCsv.loadTable(TabPaths.TAB16, TabPaths.TAB16_headers)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("Expedientes")



      val q1 = sql(
        """SELECT cfinca, cptoserv, csecexpe, fapexpd, fciexped, count(*) as sum
                         FROM Expedientes
                         GROUP BY cfinca, cptoserv, csecexpe, fapexpd, fciexped
                         HAVING sum > 1
                         ORDER BY cfinca
                     """)

      q1.show(30, truncate = false)


//          sql("""SELECT *
//                FROM Expedientes
//                WHERE cfinca = "8227373"
//                     """).show(100,false)

    }

    SparkSessionUtils.sc.stop()
  }

}
