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

      val n = 20

//      println("Lecturas Irregularidad 01:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_01).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_01")
//      println("Lecturas Anomalia 01:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_01).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_01")

//      println("Lecturas Irregularidad 02:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_02).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_02")
//      println("Lecturas Anomalia 02:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_02).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_02")


//      println("Lecturas Irregularidad 03:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_03).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_03")
//      println("Lecturas Anomalia 03:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_03).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_03")

//      println("Lecturas Irregularidad 04:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_04).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_04")
//      println("Lecturas Anomalia 04:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_04).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_04")

//      println("Lecturas Irregularidad 05:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_05).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_05")
//      println("Lecturas Anomalia 05:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_05).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_05")

//      println("Lecturas Irregularidad 06:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_06).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_06")
//      println("Lecturas Anomalia 06:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_06).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_06")

//      println("Lecturas Irregularidad 07:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_07).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_07")
//      println("Lecturas Anomalia 07:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_07).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_07")

//      println("Lecturas Irregularidad 08:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_08).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_08")
//      println("Lecturas Anomalia 08:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_08).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_08")

//      println("Lecturas Irregularidad 09:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_09).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_09")
//      println("Lecturas Anomalia 09:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_09).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_09")

//      println("Lecturas Irregularidad 10:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_10).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_10")
//      println("Lecturas Anomalia 10:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_10).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_10")

//      println("Lecturas Irregularidad 11:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_11).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_11")
//      println("Lecturas Anomalia 11:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_11).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_11")

//      println("Lecturas Irregularidad 12:")
//      LoadTableParquet.loadTable(TabPaths.lecturasIrregularidad_12).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasIrregularidad_12")
//      println("Lecturas Anomalia 12:")
//      LoadTableParquet.loadTable(TabPaths.lecturasAnomalia_12).limit(n).write.option("header","true").csv("/home/davgutavi/Escritorio/print_dataset/lecturasAnomalia_12")

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }


}
