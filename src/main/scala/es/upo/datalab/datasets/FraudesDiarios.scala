package es.upo.datalab.datasets


import es.upo.datalab.entrypoints.s3_load.T16_dateParser
import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.PairRDDFunctions

import scala.collection.mutable

object FraudesDiarios {

  val nivel = StorageLevel.MEMORY_AND_DISK



  def main(args: Array[String]): Unit = {

    val sqlContext = SparkSessionUtils.sql

    val sc = SparkSessionUtils.sc
    import sqlContext._

//    println("Leyendo tabla de fraude")
//
//    val df = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/CC123F454")
//
////    println("Obteniendo sample")
////
////    val df = tf.sample(false,0.3)
//
//    val colConsumoDiario = List(
//        col("1q_consumo_01")
//      , col("2q_consumo_01")
//      , col("3q_consumo_01")
//      , col("4q_consumo_01")
//      , col("1q_consumo_02")
//      , col("2q_consumo_02")
//      , col("3q_consumo_02")
//      , col("4q_consumo_02")
//      , col("1q_consumo_03")
//      , col("2q_consumo_03")
//      , col("3q_consumo_03")
//      , col("4q_consumo_03")
//      , col("1q_consumo_04")
//      , col("2q_consumo_04")
//      , col("3q_consumo_04")
//      , col("4q_consumo_04")
//      , col("1q_consumo_05")
//      , col("2q_consumo_05")
//      , col("3q_consumo_05")
//      , col("4q_consumo_05")
//      , col("1q_consumo_06")
//      , col("2q_consumo_06")
//      , col("3q_consumo_06")
//      , col("4q_consumo_06")
//      , col("1q_consumo_07")
//      , col("2q_consumo_07")
//      , col("3q_consumo_07")
//      , col("4q_consumo_07")
//      , col("1q_consumo_08")
//      , col("2q_consumo_08")
//      , col("3q_consumo_08")
//      , col("4q_consumo_08")
//      , col("1q_consumo_09")
//      , col("2q_consumo_09")
//      , col("3q_consumo_09")
//      , col("4q_consumo_09")
//      , col("1q_consumo_10")
//      , col("2q_consumo_10")
//      , col("3q_consumo_10")
//      , col("4q_consumo_10")
//      , col("1q_consumo_11")
//      , col("2q_consumo_11")
//      , col("3q_consumo_11")
//      , col("4q_consumo_11")
//      , col("1q_consumo_12")
//      , col("2q_consumo_12")
//      , col("3q_consumo_12")
//      , col("4q_consumo_12")
//      , col("1q_consumo_13")
//      , col("2q_consumo_13")
//      , col("3q_consumo_13")
//      , col("4q_consumo_13")
//      , col("1q_consumo_14")
//      , col("2q_consumo_14")
//      , col("3q_consumo_14")
//      , col("4q_consumo_14")
//      , col("1q_consumo_15")
//      , col("2q_consumo_15")
//      , col("3q_consumo_15")
//      , col("4q_consumo_15")
//      , col("1q_consumo_16")
//      , col("2q_consumo_16")
//      , col("3q_consumo_16")
//      , col("4q_consumo_16")
//      , col("1q_consumo_17")
//      , col("2q_consumo_17")
//      , col("3q_consumo_17")
//      , col("4q_consumo_17")
//      , col("1q_consumo_18")
//      , col("2q_consumo_18")
//      , col("3q_consumo_18")
//      , col("4q_consumo_18")
//      , col("1q_consumo_19")
//      , col("2q_consumo_19")
//      , col("3q_consumo_19")
//      , col("4q_consumo_19")
//      , col("1q_consumo_20")
//      , col("2q_consumo_20")
//      , col("3q_consumo_20")
//      , col("4q_consumo_20")
//      , col("1q_consumo_21")
//      , col("2q_consumo_21")
//      , col("3q_consumo_21")
//      , col("4q_consumo_21")
//      , col("1q_consumo_22")
//      , col("2q_consumo_22")
//      , col("3q_consumo_22")
//      , col("4q_consumo_22")
//      , col("1q_consumo_23")
//      , col("2q_consumo_23")
//      , col("3q_consumo_23")
//      , col("4q_consumo_23")
//      , col("1q_consumo_24")
//      , col("2q_consumo_24")
//      , col("3q_consumo_24")
//      , col("4q_consumo_24")
//      , col("1q_consumo_25")
//      , col("2q_consumo_25")
//      , col("3q_consumo_25")
//      , col("4q_consumo_25")
//    )
//
//
//    println("Calculando el consumo diario")
//
//    val p1 = df.withColumn("d", colConsumoDiario.reduce(_ + _))
//
////    p1.show(10)
//
//    println("Calculando sumatorio de no reales")
//
//    val p2 = p1.rdd.map(r => MappingEstimadas.call(r))
//
//    val schema = Array(
//      StructField("cpuntmed",    StringType, true),
//      StructField("codpost",     StringType, true),
//      StructField("cenae",       StringType, true),
////      StructField("flectreg",    DateType,   true),
//      StructField("numnoreales", IntegerType, true),
//      StructField("consumida",   DoubleType, true)
//    )
//
//    val customSchema = StructType(schema)
//
//    println("Construyendo nuevo dataframe")
//    val p3: DataFrame = sqlContext.createDataFrame(p2,customSchema)
//
////    p3.show(20)
//
//
//    p3.createOrReplaceTempView("P3")
//
//    val nle = sql(
//      """SELECT cpuntmed, codpost, cenae,SUM(numnoreales) AS nle
//         FROM P3 GROUP BY cpuntmed, codpost, cenae
//      """)
//
//    nle.createOrReplaceTempView("NLE")
//
////     nle.show(20)
//
//    val prev = sql(
//      """SELECT P3.cpuntmed, P3.codpost, P3.cenae, NLE.nle, P3.consumida
//         FROM P3 JOIN NLE ON NLE.cpuntmed = P3.cpuntmed AND NLE.codpost = P3.codpost AND NLE.cenae = P3.cenae
//      """)
//
//    prev.show(20)
//
//
//    prev.write.option("header", "true").save("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/FPREV/")


//    println("Leyendo tabla de fraude")
//    val df = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/FPREV/")
//
//    //val df = tabla.sample(false,0.3)
//
//
//    val r1 = df.rdd.groupBy(r => (r.getString(0),r.getString(1),r.getString(2),r.getLong(3)))
//
//    val ar1 = r1.collect()
//
//
//    var aux= mutable.MutableList[Row]()
//
//    ar1.foreach(e => {
//
//
//        val cpuntmed = e._1._1
//        val ccodpost = e._1._2
//        val cenae = e._1._3
//        val nle = e._1._4
//
//        val cn = e._2.toList
//
//        val l = mutable.MutableList[Any]()
//
//        l+=cpuntmed
//        l+=ccodpost
//         l+=cenae
//         l+=nle
//
//
//      cn.foreach(e => {
//
//          l+= e.getDouble(4)
//
//        })
//
//      aux+=Row.fromSeq(l)
//
//    })
//
//
//
//    val t = sc.parallelize(aux)
//
//    println("Chequeando tamaños:")
//
//    t.foreach(r => {
//
//      if (r.length!=458){println(r.length)}
//
//    })
//
//    println("Tamaños chequeados")
//
//
//
//
//    val schema = Array(
//       StructField("cpuntmed", StringType,  true)
//      ,StructField("ccodpost",  StringType,  true)
//      ,StructField("cenae",    StringType,  true)
//      ,StructField("nle",      LongType, true)
//      ,StructField("d1",       DoubleType,  true)
//      ,StructField("d2",       DoubleType,  true)
//      ,StructField("d3",       DoubleType,  true)
//      ,StructField("d4",       DoubleType,  true)
//      ,StructField("d5",       DoubleType,  true)
//      ,StructField("d6",       DoubleType,  true)
//      ,StructField("d7",       DoubleType,  true)
//      ,StructField("d8",       DoubleType,  true)
//      ,StructField("d9",       DoubleType,  true)
//      ,StructField("d10",       DoubleType,  true)
//      ,StructField("d11",       DoubleType,  true)
//      ,StructField("d12",       DoubleType,  true)
//      ,StructField("d13",       DoubleType,  true)
//      ,StructField("d14",       DoubleType,  true)
//      ,StructField("d15",       DoubleType,  true)
//      ,StructField("d16",       DoubleType,  true)
//      ,StructField("d17",       DoubleType,  true)
//      ,StructField("d18",       DoubleType,  true)
//      ,StructField("d19",       DoubleType,  true)
//      ,StructField("d20",       DoubleType,  true)
//      ,StructField("d21",       DoubleType,  true)
//      ,StructField("d22",       DoubleType,  true)
//      ,StructField("d23",       DoubleType,  true)
//      ,StructField("d24",       DoubleType,  true)
//      ,StructField("d25",       DoubleType,  true)
//      ,StructField("d26",       DoubleType,  true)
//      ,StructField("d27",       DoubleType,  true)
//      ,StructField("d28",       DoubleType,  true)
//      ,StructField("d29",       DoubleType,  true)
//      ,StructField("d30",       DoubleType,  true)
//      ,StructField("d31",       DoubleType,  true)
//      ,StructField("d32",       DoubleType,  true)
//      ,StructField("d33",       DoubleType,  true)
//      ,StructField("d34",       DoubleType,  true)
//      ,StructField("d35",       DoubleType,  true)
//      ,StructField("d36",       DoubleType,  true)
//      ,StructField("d37",       DoubleType,  true)
//      ,StructField("d38",       DoubleType,  true)
//      ,StructField("d39",       DoubleType,  true)
//      ,StructField("d40",       DoubleType,  true)
//      ,StructField("d41",       DoubleType,  true)
//      ,StructField("d42",       DoubleType,  true)
//      ,StructField("d43",       DoubleType,  true)
//      ,StructField("d44",       DoubleType,  true)
//      ,StructField("d45",       DoubleType,  true)
//      ,StructField("d46",       DoubleType,  true)
//      ,StructField("d47",       DoubleType,  true)
//      ,StructField("d48",       DoubleType,  true)
//      ,StructField("d49",       DoubleType,  true)
//      ,StructField("d50",       DoubleType,  true)
//      ,StructField("d51",       DoubleType,  true)
//      ,StructField("d52",       DoubleType,  true)
//      ,StructField("d53",       DoubleType,  true)
//      ,StructField("d54",       DoubleType,  true)
//      ,StructField("d55",       DoubleType,  true)
//      ,StructField("d56",       DoubleType,  true)
//      ,StructField("d57",       DoubleType,  true)
//      ,StructField("d58",       DoubleType,  true)
//      ,StructField("d59",       DoubleType,  true)
//      ,StructField("d60",       DoubleType,  true)
//      ,StructField("d61",       DoubleType,  true)
//      ,StructField("d62",       DoubleType,  true)
//      ,StructField("d63",       DoubleType,  true)
//      ,StructField("d64",       DoubleType,  true)
//      ,StructField("d65",       DoubleType,  true)
//      ,StructField("d66",       DoubleType,  true)
//      ,StructField("d67",       DoubleType,  true)
//      ,StructField("d68",       DoubleType,  true)
//      ,StructField("d69",       DoubleType,  true)
//      ,StructField("d70",       DoubleType,  true)
//      ,StructField("d71",       DoubleType,  true)
//      ,StructField("d72",       DoubleType,  true)
//      ,StructField("d73",       DoubleType,  true)
//      ,StructField("d74",       DoubleType,  true)
//      ,StructField("d75",       DoubleType,  true)
//      ,StructField("d76",       DoubleType,  true)
//      ,StructField("d77",       DoubleType,  true)
//      ,StructField("d78",       DoubleType,  true)
//      ,StructField("d79",       DoubleType,  true)
//      ,StructField("d80",       DoubleType,  true)
//      ,StructField("d81",       DoubleType,  true)
//      ,StructField("d82",       DoubleType,  true)
//      ,StructField("d83",       DoubleType,  true)
//      ,StructField("d84",       DoubleType,  true)
//      ,StructField("d85",       DoubleType,  true)
//      ,StructField("d86",       DoubleType,  true)
//      ,StructField("d87",       DoubleType,  true)
//      ,StructField("d88",       DoubleType,  true)
//      ,StructField("d89",       DoubleType,  true)
//      ,StructField("d90",       DoubleType,  true)
//      ,StructField("d91",       DoubleType,  true)
//      ,StructField("d92",       DoubleType,  true)
//      ,StructField("d93",       DoubleType,  true)
//      ,StructField("d94",       DoubleType,  true)
//      ,StructField("d95",       DoubleType,  true)
//      ,StructField("d96",       DoubleType,  true)
//      ,StructField("d97",       DoubleType,  true)
//      ,StructField("d98",       DoubleType,  true)
//      ,StructField("d99",       DoubleType,  true)
//      ,StructField("d100",      DoubleType,  true)
//      ,StructField("d101",      DoubleType,  true)
//      ,StructField("d102",      DoubleType,  true)
//      ,StructField("d103",      DoubleType,  true)
//      ,StructField("d104",      DoubleType,  true)
//      ,StructField("d105",      DoubleType,  true)
//      ,StructField("d106",      DoubleType,  true)
//      ,StructField("d107",      DoubleType,  true)
//      ,StructField("d108",      DoubleType,  true)
//      ,StructField("d109",      DoubleType,  true)
//      ,StructField("d110",      DoubleType,  true)
//      ,StructField("d111",      DoubleType,  true)
//      ,StructField("d112",      DoubleType,  true)
//      ,StructField("d113",      DoubleType,  true)
//      ,StructField("d114",      DoubleType,  true)
//      ,StructField("d115",      DoubleType,  true)
//      ,StructField("d116",      DoubleType,  true)
//      ,StructField("d117",      DoubleType,  true)
//      ,StructField("d118",      DoubleType,  true)
//      ,StructField("d119",      DoubleType,  true)
//      ,StructField("d120",      DoubleType,  true)
//      ,StructField("d121",      DoubleType,  true)
//      ,StructField("d122",      DoubleType,  true)
//      ,StructField("d123",      DoubleType,  true)
//      ,StructField("d124",      DoubleType,  true)
//      ,StructField("d125",      DoubleType,  true)
//      ,StructField("d126",      DoubleType,  true)
//      ,StructField("d127",      DoubleType,  true)
//      ,StructField("d128",      DoubleType,  true)
//      ,StructField("d129",      DoubleType,  true)
//      ,StructField("d130",      DoubleType,  true)
//      ,StructField("d131",      DoubleType,  true)
//      ,StructField("d132",      DoubleType,  true)
//      ,StructField("d133",      DoubleType,  true)
//      ,StructField("d134",      DoubleType,  true)
//      ,StructField("d135",      DoubleType,  true)
//      ,StructField("d136",      DoubleType,  true)
//      ,StructField("d137",      DoubleType,  true)
//      ,StructField("d138",      DoubleType,  true)
//      ,StructField("d139",      DoubleType,  true)
//      ,StructField("d140",      DoubleType,  true)
//      ,StructField("d141",      DoubleType,  true)
//      ,StructField("d142",      DoubleType,  true)
//      ,StructField("d143",      DoubleType,  true)
//      ,StructField("d144",      DoubleType,  true)
//      ,StructField("d145",      DoubleType,  true)
//      ,StructField("d146",      DoubleType,  true)
//      ,StructField("d147",      DoubleType,  true)
//      ,StructField("d148",      DoubleType,  true)
//      ,StructField("d149",      DoubleType,  true)
//      ,StructField("d150",      DoubleType,  true)
//      ,StructField("d151",      DoubleType,  true)
//      ,StructField("d152",      DoubleType,  true)
//      ,StructField("d153",      DoubleType,  true)
//      ,StructField("d154",      DoubleType,  true)
//      ,StructField("d155",      DoubleType,  true)
//      ,StructField("d156",      DoubleType,  true)
//      ,StructField("d157",      DoubleType,  true)
//      ,StructField("d158",      DoubleType,  true)
//      ,StructField("d159",      DoubleType,  true)
//      ,StructField("d160",      DoubleType,  true)
//      ,StructField("d161",      DoubleType,  true)
//      ,StructField("d162",      DoubleType,  true)
//      ,StructField("d163",      DoubleType,  true)
//      ,StructField("d164",      DoubleType,  true)
//      ,StructField("d165",      DoubleType,  true)
//      ,StructField("d166",      DoubleType,  true)
//      ,StructField("d167",      DoubleType,  true)
//      ,StructField("d168",      DoubleType,  true)
//      ,StructField("d169",      DoubleType,  true)
//      ,StructField("d170",      DoubleType,  true)
//      ,StructField("d171",      DoubleType,  true)
//      ,StructField("d172",      DoubleType,  true)
//      ,StructField("d173",      DoubleType,  true)
//      ,StructField("d174",      DoubleType,  true)
//      ,StructField("d175",      DoubleType,  true)
//      ,StructField("d176",      DoubleType,  true)
//      ,StructField("d177",      DoubleType,  true)
//      ,StructField("d178",      DoubleType,  true)
//      ,StructField("d179",      DoubleType,  true)
//      ,StructField("d180",      DoubleType,  true)
//      ,StructField("d181",      DoubleType,  true)
//      ,StructField("d182",      DoubleType,  true)
//      ,StructField("d183",      DoubleType,  true)
//      ,StructField("d184",      DoubleType,  true)
//      ,StructField("d185",      DoubleType,  true)
//      ,StructField("d186",      DoubleType,  true)
//      ,StructField("d187",      DoubleType,  true)
//      ,StructField("d188",      DoubleType,  true)
//      ,StructField("d189",      DoubleType,  true)
//      ,StructField("d190",      DoubleType,  true)
//      ,StructField("d191",      DoubleType,  true)
//      ,StructField("d192",      DoubleType,  true)
//      ,StructField("d193",      DoubleType,  true)
//      ,StructField("d194",      DoubleType,  true)
//      ,StructField("d195",      DoubleType,  true)
//      ,StructField("d196",      DoubleType,  true)
//      ,StructField("d197",      DoubleType,  true)
//      ,StructField("d198",      DoubleType,  true)
//      ,StructField("d199",      DoubleType,  true)
//      ,StructField("d200", DoubleType, true)
//    ,StructField("d201", DoubleType, true)
//    ,StructField("d202", DoubleType, true)
//    ,StructField("d203", DoubleType, true)
//    ,StructField("d204", DoubleType, true)
//    ,StructField("d205", DoubleType, true)
//    ,StructField("d206", DoubleType, true)
//    ,StructField("d207", DoubleType, true)
//    ,StructField("d208", DoubleType, true)
//    ,StructField("d209", DoubleType, true)
//    ,StructField("d210", DoubleType, true)
//    ,StructField("d211", DoubleType, true)
//    ,StructField("d212", DoubleType, true)
//    ,StructField("d213", DoubleType, true)
//    ,StructField("d214", DoubleType, true)
//    ,StructField("d215", DoubleType, true)
//    ,StructField("d216", DoubleType, true)
//    ,StructField("d217", DoubleType, true)
//    ,StructField("d218", DoubleType, true)
//    ,StructField("d219", DoubleType, true)
//    ,StructField("d220", DoubleType, true)
//    ,StructField("d221", DoubleType, true)
//    ,StructField("d222", DoubleType, true)
//    ,StructField("d223", DoubleType, true)
//    ,StructField("d224", DoubleType, true)
//    ,StructField("d225", DoubleType, true)
//    ,StructField("d226", DoubleType, true)
//    ,StructField("d227", DoubleType, true)
//    ,StructField("d228", DoubleType, true)
//    ,StructField("d229", DoubleType, true)
//    ,StructField("d230", DoubleType, true)
//    ,StructField("d231", DoubleType, true)
//    ,StructField("d232", DoubleType, true)
//    ,StructField("d233", DoubleType, true)
//    ,StructField("d234", DoubleType, true)
//    ,StructField("d235", DoubleType, true)
//    ,StructField("d236", DoubleType, true)
//    ,StructField("d237", DoubleType, true)
//    ,StructField("d238", DoubleType, true)
//    ,StructField("d239", DoubleType, true)
//    ,StructField("d240", DoubleType, true)
//    ,StructField("d241", DoubleType, true)
//    ,StructField("d242", DoubleType, true)
//    ,StructField("d243", DoubleType, true)
//    ,StructField("d244", DoubleType, true)
//    ,StructField("d245", DoubleType, true)
//    ,StructField("d246", DoubleType, true)
//    ,StructField("d247", DoubleType, true)
//    ,StructField("d248", DoubleType, true)
//    ,StructField("d249", DoubleType, true)
//    ,StructField("d250", DoubleType, true)
//    ,StructField("d251", DoubleType, true)
//    ,StructField("d252", DoubleType, true)
//    ,StructField("d253", DoubleType, true)
//    ,StructField("d254", DoubleType, true)
//    ,StructField("d255", DoubleType, true)
//    ,StructField("d256", DoubleType, true)
//    ,StructField("d257", DoubleType, true)
//    ,StructField("d258", DoubleType, true)
//    ,StructField("d259", DoubleType, true)
//    ,StructField("d260", DoubleType, true)
//    ,StructField("d261", DoubleType, true)
//    ,StructField("d262", DoubleType, true)
//    ,StructField("d263", DoubleType, true)
//    ,StructField("d264", DoubleType, true)
//    ,StructField("d265", DoubleType, true)
//    ,StructField("d266", DoubleType, true)
//    ,StructField("d267", DoubleType, true)
//    ,StructField("d268", DoubleType, true)
//    ,StructField("d269", DoubleType, true)
//    ,StructField("d270", DoubleType, true)
//    ,StructField("d271", DoubleType, true)
//    ,StructField("d272", DoubleType, true)
//    ,StructField("d273", DoubleType, true)
//    ,StructField("d274", DoubleType, true)
//    ,StructField("d275", DoubleType, true)
//    ,StructField("d276", DoubleType, true)
//    ,StructField("d277", DoubleType, true)
//    ,StructField("d278", DoubleType, true)
//    ,StructField("d279", DoubleType, true)
//    ,StructField("d280", DoubleType, true)
//    ,StructField("d281", DoubleType, true)
//    ,StructField("d282", DoubleType, true)
//    ,StructField("d283", DoubleType, true)
//    ,StructField("d284", DoubleType, true)
//    ,StructField("d285", DoubleType, true)
//    ,StructField("d286", DoubleType, true)
//    ,StructField("d287", DoubleType, true)
//    ,StructField("d288", DoubleType, true)
//    ,StructField("d289", DoubleType, true)
//    ,StructField("d290", DoubleType, true)
//    ,StructField("d291", DoubleType, true)
//    ,StructField("d292", DoubleType, true)
//    ,StructField("d293", DoubleType, true)
//    ,StructField("d294", DoubleType, true)
//    ,StructField("d295", DoubleType, true)
//    ,StructField("d296", DoubleType, true)
//    ,StructField("d297", DoubleType, true)
//    ,StructField("d298", DoubleType, true)
//    ,StructField("d299", DoubleType, true)
//    ,StructField("d300", DoubleType, true)
//    ,StructField("d301", DoubleType, true)
//    ,StructField("d302", DoubleType, true)
//    ,StructField("d303", DoubleType, true)
//    ,StructField("d304", DoubleType, true)
//    ,StructField("d305", DoubleType, true)
//    ,StructField("d306", DoubleType, true)
//    ,StructField("d307", DoubleType, true)
//    ,StructField("d308", DoubleType, true)
//    ,StructField("d309", DoubleType, true)
//    ,StructField("d310", DoubleType, true)
//    ,StructField("d311", DoubleType, true)
//    ,StructField("d312", DoubleType, true)
//    ,StructField("d313", DoubleType, true)
//    ,StructField("d314", DoubleType, true)
//    ,StructField("d315", DoubleType, true)
//    ,StructField("d316", DoubleType, true)
//    ,StructField("d317", DoubleType, true)
//    ,StructField("d318", DoubleType, true)
//    ,StructField("d319", DoubleType, true)
//    ,StructField("d320", DoubleType, true)
//    ,StructField("d321", DoubleType, true)
//    ,StructField("d322", DoubleType, true)
//    ,StructField("d323", DoubleType, true)
//    ,StructField("d324", DoubleType, true)
//    ,StructField("d325", DoubleType, true)
//    ,StructField("d326", DoubleType, true)
//    ,StructField("d327", DoubleType, true)
//    ,StructField("d328", DoubleType, true)
//    ,StructField("d329", DoubleType, true)
//    ,StructField("d330", DoubleType, true)
//    ,StructField("d331", DoubleType, true)
//    ,StructField("d332", DoubleType, true)
//    ,StructField("d333", DoubleType, true)
//    ,StructField("d334", DoubleType, true)
//    ,StructField("d335", DoubleType, true)
//    ,StructField("d336", DoubleType, true)
//    ,StructField("d337", DoubleType, true)
//    ,StructField("d338", DoubleType, true)
//    ,StructField("d339", DoubleType, true)
//    ,StructField("d340", DoubleType, true)
//    ,StructField("d341", DoubleType, true)
//    ,StructField("d342", DoubleType, true)
//    ,StructField("d343", DoubleType, true)
//    ,StructField("d344", DoubleType, true)
//    ,StructField("d345", DoubleType, true)
//    ,StructField("d346", DoubleType, true)
//    ,StructField("d347", DoubleType, true)
//    ,StructField("d348", DoubleType, true)
//    ,StructField("d349", DoubleType, true)
//    ,StructField("d350", DoubleType, true)
//    ,StructField("d351", DoubleType, true)
//    ,StructField("d352", DoubleType, true)
//    ,StructField("d353", DoubleType, true)
//    ,StructField("d354", DoubleType, true)
//    ,StructField("d355", DoubleType, true)
//    ,StructField("d356", DoubleType, true)
//    ,StructField("d357", DoubleType, true)
//    ,StructField("d358", DoubleType, true)
//    ,StructField("d359", DoubleType, true)
//    ,StructField("d360", DoubleType, true)
//    ,StructField("d361", DoubleType, true)
//    ,StructField("d362", DoubleType, true)
//    ,StructField("d363", DoubleType, true)
//    ,StructField("d364", DoubleType, true)
//    ,StructField("d365", DoubleType, true)
//    ,StructField("d366", DoubleType, true)
//    ,StructField("d367", DoubleType, true)
//    ,StructField("d368", DoubleType, true)
//    ,StructField("d369", DoubleType, true)
//    ,StructField("d370", DoubleType, true)
//    ,StructField("d371", DoubleType, true)
//    ,StructField("d372", DoubleType, true)
//    ,StructField("d373", DoubleType, true)
//    ,StructField("d374", DoubleType, true)
//    ,StructField("d375", DoubleType, true)
//    ,StructField("d376", DoubleType, true)
//    ,StructField("d377", DoubleType, true)
//    ,StructField("d378", DoubleType, true)
//    ,StructField("d379", DoubleType, true)
//    ,StructField("d380", DoubleType, true)
//    ,StructField("d381", DoubleType, true)
//    ,StructField("d382", DoubleType, true)
//    ,StructField("d383", DoubleType, true)
//    ,StructField("d384", DoubleType, true)
//    ,StructField("d385", DoubleType, true)
//    ,StructField("d386", DoubleType, true)
//    ,StructField("d387", DoubleType, true)
//    ,StructField("d388", DoubleType, true)
//    ,StructField("d389", DoubleType, true)
//    ,StructField("d390", DoubleType, true)
//    ,StructField("d391", DoubleType, true)
//    ,StructField("d392", DoubleType, true)
//    ,StructField("d393", DoubleType, true)
//    ,StructField("d394", DoubleType, true)
//    ,StructField("d395", DoubleType, true)
//    ,StructField("d396", DoubleType, true)
//    ,StructField("d397", DoubleType, true)
//    ,StructField("d398", DoubleType, true)
//    ,StructField("d399", DoubleType, true)
//    ,StructField("d400", DoubleType, true)
//    ,StructField("d401", DoubleType, true)
//    ,StructField("d402", DoubleType, true)
//    ,StructField("d403", DoubleType, true)
//    ,StructField("d404", DoubleType, true)
//    ,StructField("d405", DoubleType, true)
//    ,StructField("d406", DoubleType, true)
//    ,StructField("d407", DoubleType, true)
//    ,StructField("d408", DoubleType, true)
//    ,StructField("d409", DoubleType, true)
//    ,StructField("d410", DoubleType, true)
//    ,StructField("d411", DoubleType, true)
//    ,StructField("d412", DoubleType, true)
//    ,StructField("d413", DoubleType, true)
//    ,StructField("d414", DoubleType, true)
//    ,StructField("d415", DoubleType, true)
//    ,StructField("d416", DoubleType, true)
//    ,StructField("d417", DoubleType, true)
//    ,StructField("d418", DoubleType, true)
//    ,StructField("d419", DoubleType, true)
//    ,StructField("d420", DoubleType, true)
//    ,StructField("d421", DoubleType, true)
//    ,StructField("d422", DoubleType, true)
//    ,StructField("d423", DoubleType, true)
//    ,StructField("d424", DoubleType, true)
//    ,StructField("d425", DoubleType, true)
//    ,StructField("d426", DoubleType, true)
//    ,StructField("d427", DoubleType, true)
//    ,StructField("d428", DoubleType, true)
//    ,StructField("d429", DoubleType, true)
//    ,StructField("d430", DoubleType, true)
//    ,StructField("d431", DoubleType, true)
//    ,StructField("d432", DoubleType, true)
//    ,StructField("d433", DoubleType, true)
//    ,StructField("d434", DoubleType, true)
//    ,StructField("d435", DoubleType, true)
//    ,StructField("d436", DoubleType, true)
//    ,StructField("d437", DoubleType, true)
//    ,StructField("d438", DoubleType, true)
//    ,StructField("d439", DoubleType, true)
//    ,StructField("d440", DoubleType, true)
//    ,StructField("d441", DoubleType, true)
//    ,StructField("d442", DoubleType, true)
//    ,StructField("d443", DoubleType, true)
//    ,StructField("d444", DoubleType, true)
//    ,StructField("d445", DoubleType, true)
//    ,StructField("d446", DoubleType, true)
//    ,StructField("d447", DoubleType, true)
//    ,StructField("d448", DoubleType, true)
//    ,StructField("d449", DoubleType, true)
//    ,StructField("d450", DoubleType, true)
//    ,StructField("d451", DoubleType, true)
//    ,StructField("d452", DoubleType, true)
//    ,StructField("d453", DoubleType, true)
//    ,StructField("d454", DoubleType, true)
//          )
//
//
//    println("Tamaño del esquema = "+schema.size)
//
//
//    val customSchema = StructType(schema)
//
//    println("Construyendo nuevo dataframe")
//    val n: DataFrame = sqlContext.createDataFrame( t, customSchema )
//
//
//    n.show(20)



//    n.write.option("header", "true").save("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/dataset_fraude_diario/")




    val df = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/dataset_fraude_diario/")


    println("Número de filas = "+df.count()+"\n")


    df.printSchema()


    df.show(10)

    println("DONE!")

    SparkSessionUtils.session.stop()

  }


}
