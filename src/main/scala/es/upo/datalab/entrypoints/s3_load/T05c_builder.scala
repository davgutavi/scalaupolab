package es.upo.datalab.entrypoints.s3_load

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object T05c_builder {

  def main(args: Array[String]): Unit = {

    println("Empezando proceso")

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql

    println("Leyendo t05c")

    val df05c = LoadTableParquet.loadTable("s3a://us-upo-endesa/database_parquet/TAB05C/")
    //    val df05a = LoadTableParquet.loadTable("/Users/davgutavi/Desktop/buffer_endesa/parquet/t05a")

    println("Persistiendo t05c")
    df05c.persist(nivel)

    df05c.show(100)

    println("Esquema")
    val schema = Array(
        StructField("origen",   StringType, true)
      , StructField("cemptitu", StringType, true)
      , StructField("ccontrat", StringType, true)
      , StructField("cnumscct", StringType, true)
      , StructField("ccliente", StringType, true)
      , StructField("fechamov", StringType, true)
      , StructField("tindfiju", StringType, true)
      , StructField("cnifdnic", StringType, true)
      , StructField("dapersoc", StringType, true)
      , StructField("dnombcli", StringType, true)
    )

    val customSchema = StructType(schema)

    println("Aplicando parseo")
    val a = df05c.rdd.map(r => T05c_dateParser.call(r))

    println("Construyendo nuevo dataframe")
    val n: DataFrame = sqlContext.createDataFrame(a, customSchema)

    n.show(20)

    println("Guardando en S3")

    n.coalesce(1).write.option("header", "false").save("s3a://us-upo-endesa/database_parquet/t05c/")

    SparkSessionUtils.session.stop()

    println("DONE!")

  }

}