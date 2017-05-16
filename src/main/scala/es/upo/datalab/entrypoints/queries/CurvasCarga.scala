package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
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

      val df_01 = LoadTableCsv.loadTable(TabPaths.TAB_01_10_csv, TabPaths.TAB_01_headers)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CurvasCarga")


      val q1 = sql(
        """SELECT cpuntmed, flectreg, testcaco, vsecccar,obiscode, count(DISTINCT *) as sum
                         FROM CurvasCarga
                         GROUP BY cpuntmed, flectreg,testcaco, vsecccar, obiscode
                         HAVING sum > 1
                         ORDER BY cpuntmed
                     """)
//
//      q1.show(30, false)


                      sql("""SELECT *
                            FROM CurvasCarga
                            WHERE cpuntmed = "CAM645356400100" AND flectreg = "2010-05-14" AND testcaco = 'R' AND obiscode = 'R' AND vsecccar = "01"
                                 """).show(100,truncate = false)

    }

    SparkSessionUtils.sc.stop()
  }

}
