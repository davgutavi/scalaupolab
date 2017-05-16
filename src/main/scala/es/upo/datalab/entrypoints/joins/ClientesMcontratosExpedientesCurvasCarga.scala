package es.upo.datalab.entrypoints.joins


import es.upo.datalab.utilities._
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 5/04/17.
  */
object ClientesMcontratosExpedientesCurvasCarga {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_05C = LoadTableCsv.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
      df_05C.persist(nivel)
      println("Número de registros en Clientes = " + df_05C.count())
//      df_05C.createOrReplaceTempView("clientes")

      val df_05Cs = df_05C.dropDuplicates()
      df_05C.unpersist()
      df_05Cs.persist(nivel)
      println("Número de registros en Clientes sin Repetición = " + df_05Cs.count())
      df_05Cs.createOrReplaceTempView("clientess")

      val df_00C = LoadTableCsv.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      println("Número de registros en Maestro Contratos = " + df_00C.count())
      df_00C.createOrReplaceTempView("contratos")

      val df_16 = LoadTableCsv.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      println("Número de registros en Expedientes = " + df_16.count())
      df_16.createOrReplaceTempView("expedientes")
//      df_16.show(10,truncate=false)

      val df_00E = LoadTableCsv.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)
      df_00E.persist(nivel)
      println("Número de registros en Aparatos = " + df_00E.count())
      df_00E.createOrReplaceTempView("aparatos")

      val df_01_10 = LoadTableCsv.loadTable(TabPaths.TAB_01_10_csv,TabPaths.TAB_01_headers)
      df_01_10.persist(nivel)
      println("Número de registros en Curvas de Carga = " + df_01_10.count())
      df_01_10.createOrReplaceTempView("cargas")

//      val df_01_10s = df_01_10.dropDuplicates()
//      df_01_10.unpersist()
//      df_01_10s.persist(nivel)
//      println("Número de registros en Curvas de Carga sin Repetición = " + df_01_10s.count())
//      df_01_10s.createOrReplaceTempView("cargass")

      val jcc = sql(

        """
          SELECT clientess.cnifdnic, clientess.dnombcli, clientess.dapersoc, clientess.ccliente, contratos.ccontrat, contratos.cnumscct, clientess.fechamov, contratos.fpsercon, contratos.ffinvesu,
          contratos.origen, contratos.cupsree2, contratos.cpuntmed,contratos.cemptitu, contratos.cfinca, contratos.cptoserv
          FROM clientess JOIN contratos
          ON clientess.origen=contratos.origen AND clientess.cemptitu=contratos.cemptitu AND clientess.ccontrat=contratos.ccontrat AND clientess.cnumscct=contratos.cnumscct

        """
      )

          df_05Cs.unpersist()
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
          SELECT jccs.cnifdnic, jccs.dnombcli, jccs.dapersoc, jccs.ccliente, jccs.ccontrat, jccs.cnumscct, jccs.fechamov, jccs.fpsercon, jccs.ffinvesu,
          jccs.cfinca, jccs.origen, jccs.cupsree2,jccs.cpuntmed,expedientes.fapexpd, expedientes.finifran, expedientes.ffinfran, expedientes.fnormali,
          expedientes.fciexped, expedientes.irregularidad
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


      val w1 = jccses.drop("ccliente", "ccontrat", "cnumscct", "fechamov", "fpsercon", "ffinvesu")
          jccses.unpersist()
      w1.persist(nivel)
      val w2 = w1.dropDuplicates()
      w1.unpersist()

      w2.createOrReplaceTempView("w2")
      w2.persist(nivel)
      println("Join clientes-contratos-expedientes sin duplicados = " + w2.count() + " registros")


      val cap = sql(
        """
          SELECT w2.irregularidad, w2.cnifdnic, w2.dnombcli, w2.dapersoc, w2.cfinca, w2.fapexpd, w2.finifran, w2.ffinfran, w2.fnormali, w2.fciexped, aparatos.origen, aparatos.cpuntmed
          FROM w2 JOIN aparatos ON w2.origen=aparatos.origen AND w2.cupsree2=aparatos.cupsree2 AND w2.cpuntmed=aparatos.cpuntmed

        """)
      cap.persist(nivel)
      println("Join Clientes Expedientes y Maestro Aparatos = " + cap.count() + " registros")
//      cap.show(20, truncate = false)
      val caps = cap.dropDuplicates()
      cap.unpersist()
      caps.persist(nivel)
      caps.createOrReplaceTempView("caps")
      println("Join Clientes Expedientes y Maestro Aparatos sin duplicados = " + caps.count() + " registros")


      val capscarga = sql(
        """
          SELECT caps.irregularidad,caps.cnifdnic,caps.dnombcli,caps.dapersoc,caps.cfinca,caps.fapexpd,caps.finifran,caps.ffinfran,caps.fnormali,caps.fciexped,cargas.flectreg,
                cargas.hora_01, cargas.1q_consumo_01, cargas.2q_consumo_01, cargas.3q_consumo_01, cargas.4q_consumo_01,
                cargas.hora_02, cargas.1q_consumo_02, cargas.2q_consumo_02, cargas.3q_consumo_02, cargas.4q_consumo_02,
                cargas.hora_03, cargas.1q_consumo_03, cargas.2q_consumo_03, cargas.3q_consumo_03, cargas.4q_consumo_03,
                cargas.hora_04, cargas.1q_consumo_04, cargas.2q_consumo_04, cargas.3q_consumo_04, cargas.4q_consumo_04,
                cargas.hora_05, cargas.1q_consumo_05, cargas.2q_consumo_05, cargas.3q_consumo_05, cargas.4q_consumo_05,
                cargas.hora_06, cargas.1q_consumo_06, cargas.2q_consumo_06, cargas.3q_consumo_06, cargas.4q_consumo_06,
                cargas.hora_07, cargas.1q_consumo_07, cargas.2q_consumo_07, cargas.3q_consumo_07, cargas.4q_consumo_07,
                cargas.hora_08, cargas.1q_consumo_08, cargas.2q_consumo_08, cargas.3q_consumo_08, cargas.4q_consumo_08,
                cargas.hora_09, cargas.1q_consumo_09, cargas.2q_consumo_09, cargas.3q_consumo_09, cargas.4q_consumo_09,
                cargas.hora_10, cargas.1q_consumo_10, cargas.2q_consumo_10, cargas.3q_consumo_10, cargas.4q_consumo_10,
                cargas.hora_11, cargas.1q_consumo_11, cargas.2q_consumo_11, cargas.3q_consumo_11, cargas.4q_consumo_11,
                cargas.hora_12, cargas.1q_consumo_12, cargas.2q_consumo_12, cargas.3q_consumo_12, cargas.4q_consumo_12,
                cargas.hora_13, cargas.1q_consumo_13, cargas.2q_consumo_13, cargas.3q_consumo_13, cargas.4q_consumo_13,
                cargas.hora_14, cargas.1q_consumo_14, cargas.2q_consumo_14, cargas.3q_consumo_14, cargas.4q_consumo_14,
                cargas.hora_15, cargas.1q_consumo_15, cargas.2q_consumo_15, cargas.3q_consumo_15, cargas.4q_consumo_15,
                cargas.hora_16, cargas.1q_consumo_16, cargas.2q_consumo_16, cargas.3q_consumo_16, cargas.4q_consumo_16,
                cargas.hora_17, cargas.1q_consumo_17, cargas.2q_consumo_17, cargas.3q_consumo_17, cargas.4q_consumo_17,
                cargas.hora_18, cargas.1q_consumo_18, cargas.2q_consumo_18, cargas.3q_consumo_18, cargas.4q_consumo_18,
                cargas.hora_19, cargas.1q_consumo_19, cargas.2q_consumo_19, cargas.3q_consumo_19, cargas.4q_consumo_19,
                cargas.hora_20, cargas.1q_consumo_20, cargas.2q_consumo_20, cargas.3q_consumo_20, cargas.4q_consumo_20,
                cargas.hora_21, cargas.1q_consumo_21, cargas.2q_consumo_21, cargas.3q_consumo_21, cargas.4q_consumo_21,
                cargas.hora_22, cargas.1q_consumo_22, cargas.2q_consumo_22, cargas.3q_consumo_22, cargas.4q_consumo_22,
                cargas.hora_23, cargas.1q_consumo_23, cargas.2q_consumo_23, cargas.3q_consumo_23, cargas.4q_consumo_23,
                cargas.hora_24, cargas.1q_consumo_24, cargas.2q_consumo_24, cargas.3q_consumo_24, cargas.4q_consumo_24,
                cargas.hora_25, cargas.1q_consumo_25, cargas.2q_consumo_25, cargas.3q_consumo_25, cargas.4q_consumo_25

                FROM caps JOIN cargas
                ON caps.origen=cargas.origen AND caps.cpuntmed=cargas.cpuntmed

        """)

      caps.unpersist()

      capscarga.persist(nivel)
      println("Join Clientes Expedientes y Cargas = " + capscarga.count() + " registros")
      val capscargas = capscarga.dropDuplicates()
      capscargas.persist(nivel)
      println("Join Clientes Expedientes y Cargas sin repetición =  " + capscargas.count() + " registros")
      capscargas.show(20,truncate = false)
      capscarga.unpersist()

      capscargas.write.option("header","true").save(TabPaths.root+"datasets/clientes_expedientes_curvas")

      val fraude = capscargas.where("irregularidad='S'")
      val no_fraude = capscargas.where("irregularidad='N'")
      capscargas.unpersist()

      fraude.persist(nivel)
      println("Join Clientes y Cargas con fraude =  " + fraude.count() + " registros")
      fraude.show(20,truncate = false)
      val fraudes = fraude.dropDuplicates()
      fraude.unpersist()
      fraudes.persist(nivel)
      println("Join Clientes y Cargas con fraude sin repetición =  " + fraudes.count() + " registros")
      fraudes.show(20,truncate = false)

      fraudes.write.option("header","true").save(TabPaths.root+"datasets/clientes_expedientes_curvas_con_fraude")
      fraudes.unpersist()

      no_fraude.persist(nivel)
      println("Join Clientes y Cargas sin fraude =  " + no_fraude.count() + " registros")
      no_fraude.show(20,truncate = false)
      val no_fraudes = no_fraude.dropDuplicates()
      no_fraude.unpersist()
      no_fraudes.persist(nivel)
      println("Join Clientes y Cargas sin fraude sin repetición =  " + no_fraudes.count() + " registros")
      no_fraudes.show(20,truncate = false)

      no_fraudes.write.option("header","true").save(TabPaths.root+"datasets/clientes_expedientes_curvas_sin_fraude")
      no_fraudes.unpersist()

    }

    SparkSessionUtils.sc.stop()


  }

}