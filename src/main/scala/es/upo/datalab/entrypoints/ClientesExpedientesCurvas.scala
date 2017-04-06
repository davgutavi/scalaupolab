package es.upo.datalab.entrypoints


import es.upo.datalab.datasets._
import es.upo.datalab.utilities._
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 5/04/17.
  */
object ClientesExpedientesCurvas {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

//      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
//      df_05C.persist(nivel)
//      println("Número de registros en Clientes = " + df_05C.count())
//      df_05C.createOrReplaceTempView("clientes")
//      val df_05Cs = df_05C.dropDuplicates()
//      df_05C.unpersist()
//      println("Número de registros en Clientes sin Repetición = " + df_05Cs.count())
//      df_05Cs.createOrReplaceTempView("clientess")
//
//      df_05Cs.show(10,truncate=false)
//
//      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
//      df_00C.persist(nivel)
//      println("Número de registros en Maestro Contratos = " + df_00C.count())
//      df_00C.createOrReplaceTempView("contratos")
//
//      df_00C.show(10,truncate=false)
//
//      val df_16 = LoadTable.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
//      df_16.persist(nivel)
//      println("Número de registros en Expedientes = " + df_16.count())
//      df_16.createOrReplaceTempView("expedientes")
//
//      df_16.show(10,truncate=false)
//
//
//      val df_00E = LoadTable.loadTable(TabPaths.TAB_00E,TabPaths.TAB_00E_headers)
//      df_00E.persist(nivel)
//      println("Número de registros en Aparatos = " + df_00E.count())
//      df_00E.createOrReplaceTempView("aparatos")
//
//      df_00E.show(10,truncate=false)

      val df_01_10 = LoadTable.loadTable(TabPaths.TAB_01_10,TabPaths.TAB_01_headers)
      println("Número de registros en Curvas de Carga = " + df_01_10.count())
      df_01_10.createOrReplaceTempView("cargas")
      val df_01_10s = df_01_10.dropDuplicates()
      df_01_10.unpersist()
      println("Número de registros en Cuervas de Carga sin Repetición = " + df_01_10s.count() + "\n")
      df_01_10s.persist(nivel)
      df_01_10s.createOrReplaceTempView("cargass")


//      val jcc = sql(
//
//        """
//          SELECT clientess.cnifdnic, clientess.dnombcli, clientess.dapersoc, clientess.ccliente, contratos.ccontrat, contratos.cnumscct, clientess.fechamov, contratos.fpsercon, contratos.ffinvesu,
//          contratos.origen, contratos.cupsree2, contratos.cpuntmed,contratos.cemptitu, contratos.cfinca, contratos.cptoserv
//          FROM clientess JOIN contratos
//          ON clientess.origen=contratos.origen AND clientess.cemptitu=contratos.cemptitu AND clientess.ccontrat=contratos.ccontrat AND clientess.cnumscct=contratos.cnumscct
//
//        """
//      )
//
//      jcc.persist(nivel)
//      jcc.createOrReplaceTempView("jcc")
//      println("Join clientes-contratos = " + jcc.count() + " registros")
//
//      val jccs = jcc.dropDuplicates()
//      jccs.createOrReplaceTempView("jccs")
//      println("Join clientes-contratos sin repetición = " + jccs.count() + " registros")
//
//
//      val jccse = sql(
//
//        """
//          SELECT jccs.cnifdnic, jccs.dnombcli, jccs.dapersoc, jccs.ccliente, jccs.ccontrat, jccs.cnumscct, jccs.fechamov, jccs.fpsercon, jccs.ffinvesu,
//          jccs.cfinca, jccs.origen, jccs.cupsree2,jccs.cpuntmed,expedientes.fapexpd, expedientes.finifran, expedientes.ffinfran, expedientes.fnormali,
//          expedientes.fciexped, expedientes.irregularidad
//          FROM jccs JOIN expedientes
//          ON jccs.origen=expedientes.origen AND jccs.cemptitu=expedientes.cemptitu AND jccs.cfinca=expedientes.cfinca AND jccs.cptoserv=expedientes.cptoserv
//
//        """
//      )
//      jccse.persist(nivel)
//      jcc.unpersist()
//      println("Join clientes-contratos-expedientes = " + jccse.count() + " registros")
//
//      val jccses = jccse.dropDuplicates()
//      jccse.unpersist()
//      jccses.persist(nivel)
//      println("Join clientes-contratos-expedientes sin repetición = " + jccses.count() + " registros")
//
//
//      val w1 = jccses.drop("ccliente", "ccontrat", "cnumscct", "fechamov", "fpsercon", "ffinvesu")
//      w1.persist(nivel)
//      val w2 = w1.dropDuplicates()
//      w1.unpersist()
//      jccses.unpersist()
//      w2.createOrReplaceTempView("w2")
//      w2.persist(nivel)
//      println("Join clientes-contratos-expedientes sin duplicados = " + w2.count() + " registros")
//
//
//      val cap = sql(
//        """
//          SELECT w2.irregularidad, w2.cnifdnic, w2.dnombcli, w2.dapersoc, w2.cfinca, w2.fapexpd, w2.finifran, w2.ffinfran, w2.fnormali, w2.fciexped, aparatos.origen, aparatos.cpuntmed
//          FROM w2 JOIN aparatos ON w2.origen=aparatos.origen AND w2.cupsree2=aparatos.cupsree2 AND w2.cpuntmed=aparatos.cpuntmed
//
//        """)
//      cap.persist(nivel)
//      println("Join Clientes Expedientes y Maestro Aparatos = " + cap.count() + " registros")
//      cap.show(20, truncate = false)
//      val caps = cap.dropDuplicates()
//      cap.unpersist()
//      caps.persist(nivel)
//      caps.createOrReplaceTempView("caps")
//      println("Join Clientes Expedientes y Maestro Aparatos sin duplicados = " + caps.count() + " registros")
//
//
//      val capscarga = sql(
//        """
//          SELECT caps.irregularidad,caps.cnifdnic,caps.dnombcli,caps.dapersoc,caps.cfinca,caps.fapexpd,caps.finifran,caps.ffinfran,caps.fnormali,caps.fciexped,cargass.flectreg,
//                cargass.hora_01, cargass.1q_consumo_01, cargass.2q_consumo_01, cargass.3q_consumo_01, cargass.4q_consumo_01,
//                cargass.hora_02, cargass.1q_consumo_02, cargass.2q_consumo_02, cargass.3q_consumo_02, cargass.4q_consumo_02,
//                cargass.hora_03, cargass.1q_consumo_03, cargass.2q_consumo_03, cargass.3q_consumo_03, cargass.4q_consumo_03,
//                cargass.hora_04, cargass.1q_consumo_04, cargass.2q_consumo_04, cargass.3q_consumo_04, cargass.4q_consumo_04,
//                cargass.hora_05, cargass.1q_consumo_05, cargass.2q_consumo_05, cargass.3q_consumo_05, cargass.4q_consumo_05,
//                cargass.hora_06, cargass.1q_consumo_06, cargass.2q_consumo_06, cargass.3q_consumo_06, cargass.4q_consumo_06,
//                cargass.hora_07, cargass.1q_consumo_07, cargass.2q_consumo_07, cargass.3q_consumo_07, cargass.4q_consumo_07,
//                cargass.hora_08, cargass.1q_consumo_08, cargass.2q_consumo_08, cargass.3q_consumo_08, cargass.4q_consumo_08,
//                cargass.hora_09, cargass.1q_consumo_09, cargass.2q_consumo_09, cargass.3q_consumo_09, cargass.4q_consumo_09,
//                cargass.hora_10, cargass.1q_consumo_10, cargass.2q_consumo_10, cargass.3q_consumo_10, cargass.4q_consumo_10,
//                cargass.hora_11, cargass.1q_consumo_11, cargass.2q_consumo_11, cargass.3q_consumo_11, cargass.4q_consumo_11,
//                cargass.hora_12, cargass.1q_consumo_12, cargass.2q_consumo_12, cargass.3q_consumo_12, cargass.4q_consumo_12,
//                cargass.hora_13, cargass.1q_consumo_13, cargass.2q_consumo_13, cargass.3q_consumo_13, cargass.4q_consumo_13,
//                cargass.hora_14, cargass.1q_consumo_14, cargass.2q_consumo_14, cargass.3q_consumo_14, cargass.4q_consumo_14,
//                cargass.hora_15, cargass.1q_consumo_15, cargass.2q_consumo_15, cargass.3q_consumo_15, cargass.4q_consumo_15,
//                cargass.hora_16, cargass.1q_consumo_16, cargass.2q_consumo_16, cargass.3q_consumo_16, cargass.4q_consumo_16,
//                cargass.hora_17, cargass.1q_consumo_17, cargass.2q_consumo_17, cargass.3q_consumo_17, cargass.4q_consumo_17,
//                cargass.hora_18, cargass.1q_consumo_18, cargass.2q_consumo_18, cargass.3q_consumo_18, cargass.4q_consumo_18,
//                cargass.hora_19, cargass.1q_consumo_19, cargass.2q_consumo_19, cargass.3q_consumo_19, cargass.4q_consumo_19,
//                cargass.hora_20, cargass.1q_consumo_20, cargass.2q_consumo_20, cargass.3q_consumo_20, cargass.4q_consumo_20,
//                cargass.hora_21, cargass.1q_consumo_21, cargass.2q_consumo_21, cargass.3q_consumo_21, cargass.4q_consumo_21,
//                cargass.hora_22, cargass.1q_consumo_22, cargass.2q_consumo_22, cargass.3q_consumo_22, cargass.4q_consumo_22,
//                cargass.hora_23, cargass.1q_consumo_23, cargass.2q_consumo_23, cargass.3q_consumo_23, cargass.4q_consumo_23,
//                cargass.hora_24, cargass.1q_consumo_24, cargass.2q_consumo_24, cargass.3q_consumo_24, cargass.4q_consumo_24,
//                cargass.hora_25, cargass.1q_consumo_25, cargass.2q_consumo_25, cargass.3q_consumo_25, cargass.4q_consumo_25
//
//                FROM caps JOIN cargass
//                ON caps.origen=cargass.origen AND caps.cpuntmed=cargass.cpuntmed
//
//        """)
//
//      caps.unpersist()
//
//      capscarga.persist(nivel)
//      println("Join Clientes Expedientes y Cargas = " + capscarga.count() + " registros")
//      val capscargas = capscarga.dropDuplicates()
//      capscargas.persist(nivel)
//      println("Join Clientes Expedientes y Cargas sin repetición =  " + capscargas.count() + " registros")
//      capscargas.show(20,truncate = false)
//      capscarga.unpersist()
//
//      capscargas.write.option("header","true").save(TabPaths.root+"datasets/clientes_expedientes_curvas")
//
//      val fraude = capscargas.where("irregularidad = S")
//      val no_fraude = capscargas.where("irregularidad = N")
//      capscargas.unpersist()
//
//      fraude.persist(nivel)
//      println("Join Clientes y Cargas con fraude =  " + fraude.count() + " registros")
//      fraude.show(20,truncate = false)
//      val fraudes = fraude.dropDuplicates()
//      fraude.unpersist()
//      fraudes.persist(nivel)
//      println("Join Clientes y Cargas con fraude sin repetición =  " + fraudes.count() + " registros")
//      fraudes.show(20,truncate = false)
//
//      fraudes.write.option("header","true").save(TabPaths.root+"datasets/clientes_expedientes_curvas_con_fraude")
//      fraudes.unpersist()
//
//      no_fraude.persist(nivel)
//      println("Join Clientes y Cargas sin fraude =  " + no_fraude.count() + " registros")
//      no_fraude.show(20,truncate = false)
//      val no_fraudes = no_fraude.dropDuplicates()
//      no_fraude.unpersist()
//      no_fraudes.persist(nivel)
//      println("Join Clientes y Cargas sin fraude sin repetición =  " + no_fraudes.count() + " registros")
//      no_fraudes.show(20,truncate = false)
//
//      no_fraudes.write.option("header","true").save(TabPaths.root+"datasets/clientes_expedientes_curvas_sin_fraude")
//      no_fraudes.unpersist()

    }

    SparkSessionUtils.sc.stop()


  }

}