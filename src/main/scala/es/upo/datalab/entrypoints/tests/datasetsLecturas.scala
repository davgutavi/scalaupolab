package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 23/05/17.
  */
object datasetsLecturas {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      println("Lecturas Irregularidad 03:")
      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_03).show(10,truncate = false)
      println("Lecturas Anomalia 03:")
      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_03).show(10,truncate = false)

      println("Lecturas Irregularidad 04:")
      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_04).show(10,truncate = false)
      println("Lecturas Anomalia 04:")
      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_04).show(10,truncate = false)

      println("Lecturas Irregularidad 07:")
      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_07).show(10,truncate = false)
      println("Lecturas Anomalia 07:")
      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_07).show(10,truncate = false)

      println("Lecturas Irregularidad 08:")
      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_08).show(10,truncate = false)

      println("Lecturas Irregularidad 09:")
      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_09).show(10,truncate = false)
      println("Lecturas Anomalia 09:")
      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_09).show(10,truncate = false)

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }


}
