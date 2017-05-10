package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTable, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 24/04/17.
  */
object MaestroAparatos {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E, TabPaths.TAB_00E_headers)
      df_00E.persist(nivel)
      df_00E.createOrReplaceTempView("MaestroAparatos")



      val q1 = sql(
        """SELECT cupsree,csecptom,cpuntmed,fbajapm, count(*) as sum
                         FROM MaestroAparatos
                         GROUP BY cupsree,csecptom,cpuntmed,fbajapm
                         HAVING sum > 1
                         ORDER BY cupsree
                     """)

      q1.show(30, false)


//                sql("""SELECT *
//                      FROM MaestroAparatos
//                      WHERE cupsree = "ES0031406091966006JJ0F"
//                           """).show(100,false)

    }

    SparkSessionUtils.sc.stop()
  }


}
