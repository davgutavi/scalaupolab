package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTableCsv, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 25/04/17.
  */
object BitsCalidad {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_02 = LoadTableCsv.loadTable(TabPaths.TAB_02, TabPaths.TAB_02_headers, dropDuplicates = true)
      df_02.persist(nivel)
      df_02.createOrReplaceTempView("BitsCalidad")


//            val q1 = sql(
//              """SELECT cpuntmed, flectreg, vsecccar,hora, count(*) as sum
//                               FROM BitsCalidad
//                               GROUP BY cpuntmed, flectreg, vsecccar,hora
//                               HAVING sum > 1
//                               ORDER BY cpuntmed
//                           """)
//
//            q1.show(30, false)


      sql("""SELECT * FROM BitsCalidad
             WHERE cpuntmed = "CAM638390800100" AND flectreg = "2011-04-01" AND vsecccar = '01' AND hora = '000002'
          """).show(100,truncate = false)

    }

    SparkSessionUtils.sc.stop()
  }

}
