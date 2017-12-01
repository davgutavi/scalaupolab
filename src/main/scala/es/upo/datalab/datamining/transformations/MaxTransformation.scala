package es.upo.datalab.datamining.transformations

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

object MaxTransformation {

//     final val inputDataset = "/Users/davgutavi/Desktop/endesa/datasets/454d_raw_umr/454d_raw_umr"
     final val inputDataset = "/Users/davgutavi/Desktop/endesa/datasets/364d_raw_umr/364d_raw_umr"

//      final val outputDataset = "/Users/davgutavi/Desktop/endesa/datasets/454d_max_umr/454d_max_umr"
    final val outputDataset = "/Users/davgutavi/Desktop/endesa/datasets/364d_max_umr/364d_max_umr"


  final val sqlContext = SparkSessionUtils.sql
  final val sc = SparkSessionUtils.sc

  def main(args: Array[String]): Unit = {

    println("Leyendo dataset: "+inputDataset)

    val df = LoadTableParquet.loadTable(inputDataset)

    val rdd: RDD[Row] = df.rdd.map(r => MappingMax.call(r))

    val schema = Schema364.getSchema()

    val customSchema = StructType(schema)

    val newdf: DataFrame = sqlContext.createDataFrame(rdd, customSchema)

    newdf.write.option("header", "true").save(outputDataset)

    newdf.printSchema()

    println("DONE!")

    sc.stop()

  }

}
