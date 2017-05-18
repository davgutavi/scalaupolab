package es.upo.datalab.entrypoints.tests

import es.upo.datalab.utilities._
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 17/05/17.
  */
object CurvasIrregularidadTest {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val jL_aux = SparkSessionUtils.sparkSession.read.option("header","true").csv("/mnt/datos/recursos/ENDESA/datasets/curvas_irregularidad.csv")



      val jL = jL_aux.drop("ORIGEN","CPUNTMED","IRR_CNUMSCCT","IRR_CPUNTMED").dropDuplicates()

      val jlc = jL.count()

      jL.show(5,truncate = false)

      val dGa_aux = SparkSessionUtils.sparkSession.read.load(TabPaths.lecturasIrregularidad)

      val dGa = dGa_aux.drop("ffinfran","fciexped").dropDuplicates()

      dGa.createOrReplaceTempView("LI")

//      dGa.show(5,truncate = false)

      val q1 = sql(
        """
            SELECT * FROM LI
            WHERE obiscode = 'A' AND testcaco = 'R' AND (flectreg BETWEEN add_months(fapexpd,-6) AND fapexpd)
        """)

      val q1c = q1.count()

      q1.show(5, truncate = false)

      println("Registros JL = "+jlc)
      println("Registros DGA = "+q1c)
      println("Diferencia = "+(jlc-q1c))








//      select t01.*,tf.CUPSREE as IRR_CUPSREE,tf.CCONTRAT as IRR_CCONTRAT,tf.CNUMSCCT as IRR_CNUMSCCT,tf.CPUNTMED as IRR_CPUNTMED,tf.FINIFRAN as IRR_FINIFRAN,tf.FAPEXPED as IRR_FAPEXPED from endesa.t01 as t01
//      inner join endesa.t_fraud tf on tf.CPUNTMED=t01.CPUNTMED
//      where t01.OBIS_CODE='A'
//      and t01.TESTCACO='R'
//      and from_unixtime(unix_timestamp(t01.FLECTREG ,'yyyyMMdd'), 'yyyy-MM-dd')
//      between add_months(from_unixtime(unix_timestamp(tf.FAPEXPED ,'yyyyMMdd'), 'yyyy-MM-dd'), -6) and tf.FAPEXPED;
//      filtra los registros quedandose solo con los registros que son 6 meses anteriores a la apertura del expediente.











      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}

