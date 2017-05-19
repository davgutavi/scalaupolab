package es.upo.datalab.entrypoints.datasets


import es.upo.datalab.utilities._
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



            val cupsAnomalia = LoadTableParquet.loadTable(TabPaths.cupsAnomalia)
            println("cupsAnomalia = " + cupsAnomalia.count() + " registros")

      cupsAnomalia.show(20,truncate = false)

      val cupsIrregularidad = LoadTableParquet.loadTable(TabPaths.cupsIrregularidad)
      println("cupsIrregularidad = " + cupsIrregularidad.count() + " registros")
      cupsIrregularidad.show(20,truncate = false)







//            val df_00C = LoadTableParquet.loadTable(TabPaths.TAB_00C)
//            df_00C.persist(nivel)
//            df_00C.createOrReplaceTempView("MaestroContratos")
//
//            val df_16 = LoadTableParquet.loadTable(TabPaths.TAB_16)
//            df_16.persist(nivel)
//            df_16.createOrReplaceTempView("Expedientes")

//       val maestroContratosExpedientes = sql(
//
//        """
//          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed,
//                 MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
//                 Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
//                 Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,
//                 Expedientes.fciexped
//          FROM MaestroContratos JOIN Expedientes
//          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cemptitu=Expedientes.cemptitu AND MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv
//        """
//      )
//
//      df_00C.unpersist()
//      df_16.unpersist()

//      val maestroContratosExpedientes = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientes)
//      maestroContratosExpedientes.persist(nivel)

//      val maestroContratosExpedientesIrregularidad = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientesIrregularidad)
//      maestroContratosExpedientesIrregularidad.persist(nivel)
//      println("Maestro Contratos - Expedientes con irregularidad = " + maestroContratosExpedientesIrregularidad.count() + " registros")
//
//      maestroContratosExpedientesIrregularidad.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosExpedientesIrregularidad")
//      println("MaestroContratosExpedientesIrregularidad guardado en parquet")
//      maestroContratosExpedientesIrregularidad.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosExpedientesIrregularidad")
//      println("MaestroContratosExpedientesIrregularidad guardado en csv")


//      val maestroContratosExpedientesAnomalia = LoadTableParquet.loadTable(TabPaths.maestroContratosExpedientesAnomalia)
//      maestroContratosExpedientesAnomalia.persist(nivel)
//      println("Maestro Contratos - Expedientes con anomalía = " + maestroContratosExpedientesAnomalia.count() + " registros")
//
//      maestroContratosExpedientesAnomalia.coalesce(1).write.option("header","true").save(TabPaths.prefix_05+"MaestroContratosExpedientesAnomalia")
//      println("MaestroContratosExpedientesAnomalia guardado en parquet")
//      maestroContratosExpedientesAnomalia.coalesce(1).write.option("header","true").csv(TabPaths.prefix_06+"MaestroContratosExpedientesAnomalia")
//      println("MaestroContratosExpedientesAnomalia guardado en csv")
//
//      maestroContratosExpedientes.unpersist()


//      val datasetIrregularidad_aux = maestroContratosExpedientesIrregularidad.select("cupsree","ccontrat", "cnumscct", "csecexpe", "fapexpd" )
//
//      val datasetIrregularidad = datasetIrregularidad_aux.dropDuplicates()
//
//      val di = datasetIrregularidad_aux.count()
//      val dis = datasetIrregularidad.count()
//
//      println("DatasetIrregularidad = " +di + " registros")
//      println("DatasetIrregularidad sin repetición = " + dis + " registros")
//      println("Diferencia = " + (di-dis))
//
//
//
//      val datasetAnomalia_aux = maestroContratosExpedientesAnomalia.select("cupsree","ccontrat", "cnumscct", "csecexpe", "fapexpd" )
//
//      val datasetAnomalia = datasetAnomalia_aux.dropDuplicates()
//
//      val da = datasetAnomalia_aux.count()
//      val das = datasetAnomalia.count()
//
//      println("DatasetAnomalia = " +da + " registros")
//      println("DatasetAnomalia sin repetición = " + das + " registros")
//      println("Diferencia = " + (da-das))
//
//      println("Parquet")
//      datasetIrregularidad.coalesce(1).write.option("header","true").save(TabPaths.prefix_03+"cupsIrregularidad")
//      datasetAnomalia.coalesce(1).write.option("header","true").save(TabPaths.prefix_03+"cupsAnomalia")
//
//      println("Csv")
//      datasetIrregularidad.coalesce(1).write.option("header","true").csv(TabPaths.prefix_04+"cupsIrregularidad")
//      datasetAnomalia.coalesce(1).write.option("header","true").csv(TabPaths.prefix_04+"cupsAnomalia")

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}