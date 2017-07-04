package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 7/06/17.
  */
object Test01 {

  def main(args: Array[String]): Unit = {

  val nivel = StorageLevel.MEMORY_AND_DISK

  val sqlContext = SparkSessionUtils.sqlContext

  import sqlContext._

  TimingUtils.time {

    val n = 20

//   val lecturasIrregularidad = LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_04)
//
//    lecturasIrregularidad.createOrReplaceTempView("lect")
//
//    val aux1 = sql("SELECT DISTINCT ccontrat, cnumscct FROM lect")
//
//    println("Registros = "+ aux1.count())
//
//    aux1.show(20,truncate = false)


    println("DONE!")

  }

  SparkSessionUtils.sc.stop()

}

}
