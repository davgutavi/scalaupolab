package es.upo.datalab.entrypoints

import es.upo.datalab.datasets.{LoadTable, TabPaths}
import es.upo.datalab.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 18/04/17.
  */
object CupsIrregularidadAnomalia {


  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._


    TimingUtils.time {

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
      df_05C.persist(nivel)
      println("Número de registros en Clientes = " + df_05C.count())
      df_05C.createOrReplaceTempView("clientes")

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      println("Número de registros en Maestro Contratos = " + df_00C.count())
      df_00C.createOrReplaceTempView("contratos")

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      println("Número de registros en Expedientes = " + df_16.count())
      df_16.createOrReplaceTempView("expedientes")

      val jcc = sql(

        """
          SELECT clientes.cnifdnic, clientes.dnombcli, clientes.dapersoc, clientes.ccliente, clientes.fechamov, contratos.ccontrat, contratos.cupsree, contratos.cnumscct,contratos.fpsercon, contratos.ffinvesu,
          contratos.origen, contratos.cemptitu, contratos.cfinca, contratos.cptoserv
          FROM clientes JOIN contratos
          ON clientes.origen=contratos.origen AND clientes.cemptitu=contratos.cemptitu AND clientes.ccontrat=contratos.ccontrat AND clientes.cnumscct=contratos.cnumscct

        """
      )
      df_05C.unpersist()
      df_00C.unpersist()

      jcc.persist(nivel)
      jcc.createOrReplaceTempView("jcc")
      println("Join clientes-contratos = " + jcc.count() + " registros")

      val jccs = jcc.dropDuplicates()
      jcc.unpersist()
      jccs.createOrReplaceTempView("jccs")
      println("Join clientes-contratos sin repetición = " + jccs.count() + " registros")

      val jccse = sql(

        """
          SELECT jccs.cnifdnic, jccs.dnombcli, jccs.dapersoc, jccs.ccliente, jccs.ccontrat, jccs.cupsree, jccs.cnumscct, jccs.fechamov, jccs.fpsercon, jccs.ffinvesu,
          jccs.cfinca, expedientes.fapexpd, expedientes.finifran, expedientes.ffinfran, expedientes.fnormali, expedientes.fciexped,  expedientes.anomalia, expedientes.irregularidad
          FROM jccs JOIN expedientes
          ON jccs.origen=expedientes.origen AND jccs.cemptitu=expedientes.cemptitu AND jccs.cfinca=expedientes.cfinca AND jccs.cptoserv=expedientes.cptoserv

        """
      )

      jccse.persist(nivel)
      println("Join clientes-contratos-expedientes = " + jccse.count() + " registros")

      val jccses = jccse.dropDuplicates()
      jccse.unpersist()
      jccses.persist(nivel)
      println("Join clientes-contratos-expedientes sin repetición = " + jccses.count() + " registros")


      val jccsesi = jccses.where("irregularidad='S'")
      jccsesi.persist(nivel)
      println("Join clientes-contratos-expedientes con irregularidad = " + jccsesi.count() + " registros")
      jccsesi.show(20,truncate = false)

      val w1 = jccsesi.drop("ccliente","ccontrat","cnumscct","fechamov","fpsercon","ffinvesu")
      jccsesi.unpersist()
      val w2 = w1.dropDuplicates()
      println("Join clientes-contratos-expedientes con irregularidad sin duplicados = " + w2.count() + " registros")
      w2.show(20,truncate = false)

      val jccsesa = jccses.where("anomalia='S'")
      jccsesa.persist(nivel)
      println("Join clientes-contratos-expedientes con anomalia = " + jccsesa.count() + " registros")
      jccsesa.show(20,truncate = false)

      val w3 = jccsesa.drop("ccliente","ccontrat","cnumscct","fechamov","fpsercon","ffinvesu")
      jccsesa.unpersist()
      val w4 = w3.dropDuplicates()
      println("Join clientes-contratos-expedientes con anomalía sin duplicados = " + w4.count() + " registros")
      w4.show(20,truncate = false)


      //Parquet
      //w2.write.option("header","true").save(TabPaths.root+"datasets/cups_fraude")
      //w4.write.option("header","true").save(TabPaths.root+"datasets/anomalia")

      //CSV
      w2.write.option("header","true").csv(TabPaths.root+"datasets/cupsf")
      w4.write.option("header","true").csv(TabPaths.root+"datasets/cupsa")

    }

    SparkSessionUtils.sc.stop()

  }

}