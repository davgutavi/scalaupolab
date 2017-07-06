package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object Test2 {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

      val df_05A = LoadTableParquet.loadTable(TabPaths.TAB_05A)
      df_05A.persist(nivel)
      df_05A.createOrReplaceTempView("TAB_05A")

      val tab05a = sql(
      """
                SELECT DISTINCT origen, cemptitu, ccontrat, faltacon, fbajacon, fpsercon, ffinvesu FROM TAB_05A WHERE ffinvesu > fbajacon
              """)


    //tab05a.persist(nivel)
    tab05a.createOrReplaceTempView("mce_aux")
    tab05a.show(200,truncate=false)
    println("Registros de contratos: "+df_05A.count())
    println("Registros con contrato finalizado y versión activa: "+tab05a.count())


    //tab05a.unpersist()

    println("DONE!")


    SparkSessionUtils.sc.stop()

    }








}
