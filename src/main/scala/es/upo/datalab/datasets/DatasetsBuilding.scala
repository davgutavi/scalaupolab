package es.upo.datalab.datasets

import es.upo.datalab.datamining.xgb.GBTexperiments.{datasetPath, outputRootPath}
import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.sql.SaveMode


object DatasetsBuilding {




  final val datasetLocal = "/Users/davgutavi/Desktop/endesa/datasets/"

  final val datasetLocalTarget = "/Users/davgutavi/Desktop/endesa/datasets/"


  final val datasetHdfsTarget  = "hdfs://192.168.47.247/user/datos/endesa/datasets"

  final val sqlContext = SparkSessionUtils.sql
  final val sparkSession = SparkSessionUtils.session



  def main(args: Array[String]): Unit = {

//    val d454d_raw_nrr = LoadTableParquet.loadTable(datasetLocal+"454d_raw_nrr")
//      .withColumnRenamed("cenae","cnae")
//      .withColumnRenamed("nle","umr")
//
//    d454d_raw_nrr.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"454d_raw_umr/454d_raw_umr")
//
//    d454d_raw_nrr.printSchema()
//
//    val d454d_raw_con = LoadTableParquet.loadTable(datasetLocal+"454d_raw_con")
//      .withColumnRenamed("cenae","cnae")
//
//    d454d_raw_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"454d_raw_con/454d_raw_con")
//
//    d454d_raw_con.printSchema()

//        val d454d_slo_nrr = LoadTableParquet.loadTable(datasetLocal+"454d_slo_nrr")
//          .withColumnRenamed("cenae","cnae")
//          .withColumnRenamed("nle","umr")
//
//    d454d_slo_nrr.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"454d_slo_umr/454d_slo_umr")
//
//    d454d_slo_nrr.printSchema()



//        val d454d_slo_con = LoadTableParquet.loadTable(datasetLocal+"454d_slo_con")
//          .withColumnRenamed("cenae","cnae")
//
//    d454d_slo_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"454d_slo_con/454d_slo_con")
//
//    d454d_slo_con.printSchema()


//
//            val d364d_raw_nrr= LoadTableParquet.loadTable(datasetLocal+"364d_raw_nrr")
//              .withColumnRenamed("cenae","cnae")
//              .withColumnRenamed("nle","umr")
//
//    d364d_raw_nrr.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"364d_raw_umr/364d_raw_umr")
//
//    d364d_raw_nrr.printSchema()


//            val d364d_raw_con = LoadTableParquet.loadTable(datasetLocal+"364d_raw_con")
//              .withColumnRenamed("cenae","cnae")
//
//    d364d_raw_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"364d_raw_con/364d_raw_con")
//
//    d364d_raw_con.printSchema()


//                val d364d_slo_nrr= LoadTableParquet.loadTable(datasetLocal+"364d_slo_nrr")
//                  .withColumnRenamed("cenae","cnae")
//                  .withColumnRenamed("nle","umr")
//
//    d364d_slo_nrr.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"364d_slo_umr/364d_slo_umr")
//
//    d364d_slo_nrr.printSchema()

//
//    val d364d_slo_con = LoadTableParquet.loadTable(datasetLocal+"364d_slo_con")
//                  .withColumnRenamed("cenae","cnae")
//
//    d364d_slo_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"364d_slo_con/364d_slo_con")
//
//    d364d_slo_con.printSchema()


    val d454d_max_con = LoadTableParquet.loadTable(datasetLocal+"454d_max_umr/454d_max_umr").drop("umr")

    d454d_max_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"454d_max_con/454d_max_con")

    d454d_max_con.printSchema()


    val d364d_max_con = LoadTableParquet.loadTable(datasetLocal+"364d_max_umr/364d_max_umr").drop("umr")

    d364d_max_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetLocalTarget+"364d_max_con/364d_max_con")

    d364d_max_con.printSchema()



    SparkSessionUtils.session.stop()

    }




}
