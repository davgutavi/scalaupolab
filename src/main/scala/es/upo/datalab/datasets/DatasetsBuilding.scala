package es.upo.datalab.datasets

import es.upo.datalab.datamining.GBTpendientes.{datasetPath, outputRootPath}
import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.sql.SaveMode


object DatasetsBuilding {



  final val datasetRoot = "hdfs://192.168.47.247/user/datos/endesa/datasets/"

  final val sqlContext = SparkSessionUtils.sql
  final val sparkSession = SparkSessionUtils.session



  def main(args: Array[String]): Unit = {

        //****364 dias

        //****364 dias****cpuntmed,ccodpost,cenae,nle,consumos,label===================>ALL
//
//        val t123_364d_all = LoadTableParquet.loadTable("/Users/davgutavi/Desktop/endesa_pendientes_fourier/datasets/t123_364d")
//
//        val t123_364d_all_01 =  t123_364d_all.withColumnRenamed("cenae","cnae")
//
//        val t123_364d_all_02 =  t123_364d_all_01.withColumnRenamed("nle","nrl")
//
//    t123_364d_all_02.printSchema()
//
//        t123_364d_all_02.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_364d_all")
//
//
//
//        println("Saved dataset: "+datasetRoot+"t123_364d_all")
//
//         //****364 días****cpuntmed,consumos,label=====================================>CON
//
//        val t123_364d_con = t123_364d_all_02.drop("ccodpost","cnae","nrl")
//
//    t123_364d_con.printSchema()
//
//        t123_364d_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_364d_con")
//
//
//
//        println("Saved dataset: "+datasetRoot+"t123_364d_con")
//
//        //****364 días****cpuntmed,nrl,consumos,label===================================>NRL
//
//        val t123_364d_nrl = t123_364d_all_02.drop("ccodpost","cnae")
//        t123_364d_nrl.printSchema()
//
//        t123_364d_nrl.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_364d_nrl")
//
//         println("Saved dataset: "+datasetRoot+"t123_364d_nrl")




//    //****454 dias
//
//    //****454 dias****cpuntmed,ccodpost,cenae,nle,consumos,label===================>ALL
//
//    val t123_454d_all = LoadTableParquet.loadTable("/Users/davgutavi/Desktop/endesa_pendientes_fourier/datasets/t123_454d")
//      .withColumnRenamed("cenae","cnae")
//      .withColumnRenamed("nle","nrl")
//       .withColumnRenamed("fraude","label")
//
//    t123_454d_all.printSchema()
//
//    t123_454d_all.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_454d_all")
//
//    println("Saved dataset: "+datasetRoot+"t123_454d_all")
//
//    //****454 días****cpuntmed,consumos,label=====================================>CON
//
//    val t123_454d_con = t123_454d_all.drop("ccodpost","cnae","nrl")
//
//    t123_454d_con.printSchema()
//
//    t123_454d_con.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_454d_con")
//
//    println("Saved dataset: "+datasetRoot+"t123_454d_con")
//
//    //****454 días****cpuntmed,nrl,consumos,label===================================>NRL
//
//    val t123_454d_nrl = t123_454d_all.drop("ccodpost","cnae")
//
//    t123_454d_nrl.printSchema()
//
//    t123_454d_nrl.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_454d_nrl")

//    println("Saved dataset: "+datasetRoot+"t123_454d_nrl")

//
//
//    //****364 dias pendientes
//
//    //****364 dias****cpuntmed,ccodpost,cenae,nle,consumos,label===================>ALL
//
//    val t123_364d_all_slo = LoadTableParquet.loadTable("/Users/davgutavi/Desktop/endesa_pendientes_fourier/datasets/t123_364d_pendientes")
//                            .withColumnRenamed("cenae","cnae")
//                            .withColumnRenamed("nle","nrl")
//
//    t123_364d_all_slo.printSchema()
//
//    t123_364d_all_slo.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_364d_all_slo")
//
//    println("Saved dataset: "+datasetRoot+"t123_364d_all_slo")
//
//    //****364 dias****cpuntmed,consumos,label=====================================>CON
//
//    val t123_364d_con_slo = t123_364d_all_slo.drop("ccodpost","cnae","nrl")
//
//    t123_364d_con_slo.printSchema()
//
//    t123_364d_con_slo.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_364d_con_slo")
//
//    println("Saved dataset: "+datasetRoot+"t123_364d_con_slo")
//
//    //****364 dias****cpuntmed,nrl,consumos,label===================================>NRL
//
//    val t123_364d_nrl_slo = t123_364d_all_slo.drop("ccodpost","cnae")
//
//    t123_364d_nrl_slo.printSchema()
//
//    t123_364d_nrl_slo.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_364d_nrl_slo")
//
//    println("Saved dataset: "+datasetRoot+"t123_364d_nrl_slo")


//
//
//    //****454 dias pendientes
//
    //****454 dias pendientes****cpuntmed,ccodpost,cenae,nle,consumos,label===================>ALL

    val t123_454d_all_slo = LoadTableParquet.loadTable("/Users/davgutavi/Desktop/endesa_pendientes_fourier/datasets/t123_454d_pendientes")
      .withColumnRenamed("cenae","cnae")
          .withColumnRenamed("nle","nrl")
           .withColumnRenamed("fraude","label")

    t123_454d_all_slo.printSchema()

    t123_454d_all_slo.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_454d_all_slo")

    println("Saved dataset: "+datasetRoot+"t123_454d_all_slo")

    //****4454 dias pendientes****cpuntmed,consumos,label=====================================>CON

    val t123_454d_con_slo = t123_454d_all_slo.drop("ccodpost","cnae","nrl")

    t123_454d_con_slo.printSchema()

    t123_454d_con_slo.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_454d_con_slo")

    println("Saved dataset: "+datasetRoot+"t123_454d_con_slo")

    //****454 dias pendientes****cpuntmed,nrl,consumos,label===================================>NRL

    val t123_454d_nrl_slo = t123_454d_all_slo.drop("ccodpost","cnae")

    t123_454d_nrl_slo.printSchema()

    t123_454d_nrl_slo.write.option("header", "true").mode(SaveMode.Overwrite).save(datasetRoot+"t123_454d_nrl_slo")

    println("Saved dataset: "+datasetRoot+"t123_454d_nrl_slo")


    SparkSessionUtils.session.stop()

    }




}
