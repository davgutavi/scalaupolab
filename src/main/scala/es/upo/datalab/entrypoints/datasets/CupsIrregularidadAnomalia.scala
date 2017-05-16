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

//      val df_05C = LoadTableCsv.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers, true)
//      df_05C.persist(nivel)
//      df_05C.createOrReplaceTempView("Clientes")
//
//      val df_00C = LoadTableCsv.loadTable(TabPaths.TAB_00C, TabPaths.TAB_00C_headers)
//      df_00C.persist(nivel)
//      df_00C.createOrReplaceTempView("MaestroContratos")
//
//      val df_16 = LoadTableCsv.loadTable(TabPaths.TAB_16, TabPaths.TAB_16_headers)
//      df_16.persist(nivel)
//      df_16.createOrReplaceTempView("Expedientes")

//      val df_05C = LoadTableParquet.loadTable(TabPaths.TAB_05C)
//            df_05C.persist(nivel)
//            df_05C.createOrReplaceTempView("Clientes")

            val df_00C = LoadTableParquet.loadTable(TabPaths.TAB_00C)
            df_00C.persist(nivel)
            df_00C.createOrReplaceTempView("MaestroContratos")

            val df_16 = LoadTableParquet.loadTable(TabPaths.TAB_16)
            df_16.persist(nivel)
            df_16.createOrReplaceTempView("Expedientes")

       val maestroContratosExpedientes = sql(

        """
          SELECT MaestroContratos.origen, MaestroContratos.cptocred, MaestroContratos.cfinca, MaestroContratos.cptoserv, MaestroContratos.cderind, MaestroContratos.cupsree, MaestroContratos.cpuntmed, MaestroContratos.tpuntmed,
                 MaestroContratos.vparsist, MaestroContratos.cemptitu, MaestroContratos.ccontrat, MaestroContratos.cnumscct, MaestroContratos.fpsercon, MaestroContratos.ffinvesu,
                 Expedientes.csecexpe, Expedientes.fapexpd, Expedientes.finifran, Expedientes.ffinfran, Expedientes.anomalia, Expedientes.irregularidad,
                 Expedientes.venacord, Expedientes.vennofai, Expedientes.torigexp, Expedientes.texpedie, Expedientes.expclass, Expedientes.testexpe, Expedientes.fnormali, Expedientes.cplan, Expedientes.ccampa, Expedientes.cempresa,
                 Expedientes.fciexped
          FROM MaestroContratos JOIN Expedientes
          ON MaestroContratos.origen=Expedientes.origen AND MaestroContratos.cemptitu=Expedientes.cemptitu AND MaestroContratos.cfinca=Expedientes.cfinca AND MaestroContratos.cptoserv=Expedientes.cptoserv
        """
      )

      df_00C.unpersist()
      df_16.unpersist()

      maestroContratosExpedientes.persist(nivel)

      val maestroContratosExpedientesIrregularidad = maestroContratosExpedientes.where("irregularidad='S'")
      maestroContratosExpedientesIrregularidad.persist(nivel)
      println("Maestro Contratos - Expedientes sin repetición con irregularidad = " + maestroContratosExpedientesIrregularidad.count() + " registros")
//      maestroContratosExpedientesIrregularidad.show(10,truncate = false)

      val maestroContratosExpedientesAnomalia = maestroContratosExpedientes.where("anomalia='S'")
      maestroContratosExpedientesAnomalia.persist(nivel)
      println("Maestro Contratos - Expedientes sin repetición con anomalía = " + maestroContratosExpedientesAnomalia.count() + " registros")
//      maestroContratosExpedientesAnomalia.show(10,truncate = false)

      maestroContratosExpedientes.unpersist()




      val datasetIrregularidad = maestroContratosExpedientesIrregularidad.select("cupsree","ccontrat", "cnumscct", "cpuntmed", "finifran", "fapexpd" )

      println("Registros irregularidad = "+datasetIrregularidad.count())
      datasetIrregularidad.show(10,truncate = false)


      datasetIrregularidad.persist(nivel)

      val datasetIrregularidads = datasetIrregularidad.dropDuplicates()

      val di = datasetIrregularidad.count()
      val dis = datasetIrregularidads.count()

      println("DatasetIrregularidad = " +di + " registros")
      println("DatasetIrregularidad sin repetición = " + dis + " registros")
      println("Diferencia = " + (di-dis))



      val datasetAnomalia = maestroContratosExpedientesAnomalia.select("cupsree","ccontrat", "cnumscct", "cpuntmed", "finifran", "fapexpd" )

      println("Registros anomalía = "+datasetAnomalia.count())

      datasetAnomalia.show(10,truncate = false)

      datasetAnomalia.persist(nivel)
      datasetAnomalia.createOrReplaceTempView("DatasetAnomalia")


      val datasetAnomalias = datasetAnomalia.dropDuplicates()

      val da = datasetAnomalia.count()
      val das = datasetAnomalias.count()

      println("DatasetAnomalia = " +da + " registros")
      println("DatasetAnomalia sin repetición = " + das + " registros")
      println("Diferencia = " + (da-das))

      println("Parquet")
      datasetIrregularidads.coalesce(1).write.option("header","true").save(TabPaths.prefix_03+"irregularidad")
      datasetAnomalias.coalesce(1).write.option("header","true").save(TabPaths.prefix_03+"anomalia")

      println("Csv")
      datasetIrregularidads.coalesce(1).write.option("header","true").csv(TabPaths.prefix_04+"irregularidad")
      datasetAnomalias.coalesce(1).write.option("header","true").csv(TabPaths.prefix_04+"anomalia")


//      sql(
//        """SELECT cnifdnic, ccliente, cupsree, ccontrat, cnumscct, COUNT(DISTINCT *) AS sum FROM DatasetIrregularidad
//           GROUP BY cnifdnic, ccliente, cupsree, ccontrat, cnumscct HAVING sum > 1
//        """).show(20,false)
//
//      sql(
//        """SELECT cnifdnic, cfinca, ccliente, cupsree, ccontrat, cnumscct, COUNT(DISTINCT *) AS sum FROM DatasetAnomalia
//           GROUP BY cnifdnic, ccliente, cupsree, ccontrat, cnumscct HAVING sum > 1
//        """).show(20,false)


      //CSV
//      datasetIrregularidad.coalesce(1).write.option("header","true").csv(TabPaths.root+"datasets/irregularidad")
//      datasetAnomalia.coalesce(1).write.option("header","true").csv(TabPaths.root+"datasets/anomalia")

      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}