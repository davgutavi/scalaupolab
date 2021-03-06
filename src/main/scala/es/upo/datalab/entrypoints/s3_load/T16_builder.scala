package es.upo.datalab.entrypoints.s3_load

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object T16_builder {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql

    import sqlContext._

    println("Leyendo TAB16")

    val df16 = LoadTableParquet.loadTable("s3a://us-upo-endesa/database_parquet/parquet_originales/TAB16")
    df16.persist(nivel)

    val schema = Array(
      StructField("origen",        StringType, true),
      StructField("cemptitu",      StringType, true),
      StructField("cfinca",        StringType, true),
      StructField("cptoserv",      StringType, true),
      StructField("cderind",       StringType, true),
      StructField("csecexpe",      StringType, true),
      StructField("fapexpd",       StringType, true),
      StructField("finifran",      StringType, true),
      StructField("ffinfran",      StringType, true),
      StructField("anomalia",      StringType, true),
      StructField("irregularidad", StringType, true),
      StructField("venacord",      DoubleType, true),
      StructField("vennofai",      DoubleType, true),
      StructField("torigexp",      StringType, true),
      StructField("texpedie",      StringType, true),
      StructField("expclass",      StringType, true),
      StructField("testexpe",      StringType, true),
      StructField("tpuntmed",      StringType, true),
      StructField("fnormali",      StringType, true),
      StructField("cplan",         StringType, true),
      StructField("ccampa",        StringType, true),
      StructField("cempresa",      StringType, true),
      StructField( "fciexped",     StringType, true)
    )

    val customSchema = StructType(schema)

    println("Aplicando mapeo")
    val a = df16.rdd.map(r => T16_dateParser.call(r))

    println("Construyendo nuevo dataframe")
    val n: DataFrame = sqlContext.createDataFrame(a,customSchema)

    n.show(20)

    println("Guardando en S3")

    n.coalesce(1).write.option("header","false").save("s3a://us-upo-endesa/database_parquet/t16")

    println("DONE!")

    SparkSessionUtils.session.stop()

  }

}