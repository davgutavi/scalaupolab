package es.upo.datalab.entrypoints.s3_load

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object T05a_builder {

  def main(args: Array[String]): Unit = {

    println("Empezando proceso")

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql


    println("Leyendo t05a")


    val df05a = LoadTableParquet.loadTable("s3a://us-upo-endesa/database_parquet/TAB05A/")
//    val df05a = LoadTableParquet.loadTable("/Users/davgutavi/Desktop/buffer_endesa/parquet/t05a")

    println("Persistiendo t05a")
    df05a.persist(nivel)

    println("Esquema")
    val schema = Array(
        StructField("origen", StringType, true)
      , StructField("cemptitu", StringType, true)
      , StructField("ccontrat", StringType, true)
      , StructField("cnumscct", StringType, true)
      , StructField("tcontrat", StringType, true)
      , StructField("testcont", StringType, true)
      , StructField("ctarifa", StringType, true)
      , StructField("cnae", StringType, true)
      , StructField("fsolicon", StringType, true)
      , StructField("faltacon", StringType, true)
      , StructField("fbajacon", StringType, true)
      , StructField("fpsercon", StringType, true)
      , StructField("ffinvesu", StringType, true)
      , StructField("fvtocon", StringType, true)
      , StructField("csubsect", StringType, true)
      , StructField("vnumcerr", StringType, true)
      , StructField("tubiapar", StringType, true)
      , StructField("csectmie", StringType, true)
      , StructField("vpotads1", DoubleType, true)
      , StructField("fadscri1", StringType, true)
      , StructField("fvalads1", StringType, true)
      , StructField("vpotads2", DoubleType, true)
      , StructField("fadscri2", StringType, true)
      , StructField("fvalads2", StringType, true)
      , StructField("vpotads3", DoubleType, true)
      , StructField("fadscri3", StringType, true)
      , StructField("fvalads3", StringType, true)
      , StructField("tconcort", StringType, true)
      , StructField("testader", StringType, true)
      , StructField("vpotppal", DoubleType, true)
      , StructField("potencia_1", DoubleType, true)
      , StructField("potencia_2", DoubleType, true)
      , StructField("potencia_3", DoubleType, true)
      , StructField("potencia_4", DoubleType, true)
      , StructField("potencia_5", DoubleType, true)
      , StructField("potencia_6", DoubleType, true)
      , StructField("empresa_instaladora", StringType, true)
      , StructField("instalador", StringType, true)
      , StructField("fboletin", StringType, true)
      , StructField("tension", StringType, true)
      , StructField("fases", StringType, true)
      , StructField("vpmaxbie", DoubleType, true)
      , StructField("emergencia", StringType, true)
      , StructField("complemento_perdida", StringType, true)
    )

    val customSchema = StructType(schema)

    println("Aplicando parseo")
    val a = df05a.rdd.map(r => T05a_dateParser.call(r))

    println("Construyendo nuevo dataframe")
    val n: DataFrame = sqlContext.createDataFrame(a, customSchema)

    n.show(20)

    println("Guardando en S3")

    n.coalesce(1).write.option("header", "false").save("s3a://us-upo-endesa/database_parquet/t05a/")

    println("DONE!")

    SparkSessionUtils.session.stop()

  }


}

