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
      val df_05Cs = df_05C.dropDuplicates()
      println("Número de registros en Clientes sin Repetición = " + df_05Cs.count())
      df_05Cs.createOrReplaceTempView("clientess")

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      println("Número de registros en Maestro Contratos = " + df_00C.count())
      df_00C.createOrReplaceTempView("contratos")
      val df_00Cs = df_00C.dropDuplicates()
      println("Número de registros en Maestro Contratos sin Repetición = " + df_00Cs.count())

      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      println("Número de registros en Expedientes = " + df_16.count())
      df_16.createOrReplaceTempView("expedientes")
      val df_16s = df_16.dropDuplicates()
      println("Número de registros en Expedientes sin Repetición = " + df_16s.count()+"\n")

      val jcc = sql(

        """
          SELECT clientess.cnifdnic, clientess.dnombcli, clientess.dapersoc, clientess.ccliente, contratos.ccontrat, contratos.cups, contratos.cnumscct, clientess.fechamov, contratos.fpsercon, contratos.ffinvesu,
          contratos.origen, contratos.cemptitu, contratos.cfinca, contratos.cptoserv
          FROM clientess JOIN contratos
          ON clientess.origen=contratos.origen AND clientess.cemptitu=contratos.cemptitu AND clientess.ccontrat=contratos.ccontrat AND clientess.cnumscct=contratos.cnumscct

        """
      )

      jcc.persist(nivel)
      jcc.createOrReplaceTempView("jcc")
      println("Join clientes-contratos = " + jcc.count() + " registros")
      //    jcc.show(20)
      val jccs = jcc.dropDuplicates()
      jccs.createOrReplaceTempView("jccs")
      println("Join clientes-contratos sin repetición = " + jccs.count() + " registros")
      //    jccs.show(20)

      val jccse = sql(

        """
          SELECT jccs.cnifdnic, jccs.dnombcli, jccs.dapersoc, jccs.ccliente, jccs.ccontrat, jccs.cups, jccs.cnumscct, jccs.fechamov, jccs.fpsercon, jccs.ffinvesu,
          jccs.cfinca, expedientes.fapexpd, expedientes.finifran, expedientes.ffinfran, expedientes.fnormali, expedientes.fciexped, expedientes.irregularidad
          FROM jccs JOIN expedientes
          ON jccs.origen=expedientes.origen AND jccs.cemptitu=expedientes.cemptitu AND jccs.cfinca=expedientes.cfinca AND jccs.cptoserv=expedientes.cptoserv

        """
      )
      jccse.persist(nivel)
      jccse.createOrReplaceTempView("jccse")
      println("Join clientes-contratos-expedientes = " + jccse.count() + " registros")

      val jccses = jccse.dropDuplicates()
      jccses.createOrReplaceTempView("jccs")
      println("Join clientes-contratos-expedientes sin repetición = " + jccses.count() + " registros")


      val jccsesi = jccses.where("irregularidad='S'")
      println("Join clientes-contratos-expedientes con irregularidad = " + jccsesi.count() + " registros")
      jccsesi.show(20,truncate = false)

      val w1 = jccsesi.drop("ccliente","ccontrat","cnumscct","fechamov","fpsercon","ffinvesu")
      val w2 = w1.dropDuplicates()
      println("Join clientes-contratos-expedientes con irregularidad sin duplicados = " + w2.count() + " registros")
      w2.show(20,truncate = false)

      val jccsesa = jccses.where("anomalia='S'")
      println("Join clientes-contratos-expedientes con anomalia = " + jccsesa.count() + " registros")
      jccsesa.show(20,truncate = false)

      val w3 = jccsesa.drop("ccliente","ccontrat","cnumscct","fechamov","fpsercon","ffinvesu")
      val w4 = w3.dropDuplicates()
      println("Join clientes-contratos-expedientes con anomalía sin duplicados = " + w4.count() + " registros")
      w4.show(20,truncate = false)

      w2.write.option("header","true").save(TabPaths.root+"datasets/cups_fraude")
      w4.write.option("header","true").save(TabPaths.root+"datasets/anomalia")

    }

    SparkSessionUtils.sc.stop()

  }








}
