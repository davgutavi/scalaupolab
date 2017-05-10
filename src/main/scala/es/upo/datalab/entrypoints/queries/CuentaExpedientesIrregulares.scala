package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTable, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 20/04/17.
  */
object CuentaExpedientesIrregulares {
  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)

      val t = df_16.count()
      val i = df_16.where("irregularidad = 'S'").count()
      val a = df_16.where("anomalia = 'S'").count()

      println("Expedientes irregularidad = "+i+" registros")
      println("Expedientes anomalia = "+a+" registros")
      println("Suma irregularidad más anomalía = "+(i+a)+" registros")
      println("Registros totales en Expedientes = "+t)
      println("Diferencia Totales - (irregulares + anómalos) = "+(t-(i+a)))

      df_16.unpersist()


    }


    SparkSessionUtils.sc.stop()


  }
}