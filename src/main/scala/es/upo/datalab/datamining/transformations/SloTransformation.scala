package es.upo.datalab.datamining.transformations

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row}


object SloTransformation {

  final val inputDataset = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_d_364"

  final val outputDataset = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_d_364_pendientes"

  final val sqlContext = SparkSessionUtils.sql

  final val sc = SparkSessionUtils.sc

  def main(args: Array[String]): Unit = {

    println("Leyendo dataset: "+inputDataset)

    val df = LoadTableParquet.loadTable(inputDataset)

    val rdd: RDD[Row] = df.rdd.map(r => MappingPendientes.call(r))

    val schema =  Schema364.getSchema()

    val customSchema = StructType(schema)

    val newdf: DataFrame = sqlContext.createDataFrame(rdd, customSchema)

    newdf.show(5)

    newdf.write.option("header", "true").save("/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_d_364_pendientes")

    println("DONE!")

    sc.stop()

  }

}
