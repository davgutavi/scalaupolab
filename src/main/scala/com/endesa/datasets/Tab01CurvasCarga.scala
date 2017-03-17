package com.endesa.datasets

import com.utilities.SparkSessionUtils

/**
  * Created by davgutavi on 15/03/17.
  */
object Tab01CurvasCarga {


  def main( args:Array[String] ):Unit = {


    val sqlContext = SparkSessionUtils.sqlContext

    val data = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .load(TabPaths.TAB_01_10)

    val newNames = Seq("origen", "cpuntmed", "flectreg", "testcaco", "vsecccar","control",
    "hora_01", "1q_consumo_01", "2q_consumo_01", "3q_consumo_01", "4q_consumo_01", "substatus_01", "testmenn_01", "testmecnn_01",
    "hora_02", "1q_consumo_02", "2q_consumo_02", "3q_consumo_02", "4q_consumo_02", "substatus_02", "testmenn_02", "testmecnn_02",
    "hora_03", "1q_consumo_03", "2q_consumo_03", "3q_consumo_03", "4q_consumo_03", "substatus_03", "testmenn_03", "testmecnn_03",
    "hora_04", "1q_consumo_04", "2q_consumo_04", "3q_consumo_04", "4q_consumo_04", "substatus_04", "testmenn_04", "testmecnn_04",
    "hora_05", "1q_consumo_05", "2q_consumo_05", "3q_consumo_05", "4q_consumo_05", "substatus_05", "testmenn_05", "testmecnn_05",
    "hora_06", "1q_consumo_06", "2q_consumo_06", "3q_consumo_06", "4q_consumo_06", "substatus_06", "testmenn_06", "testmecnn_06",
    "hora_07", "1q_consumo_07", "2q_consumo_07", "3q_consumo_07", "4q_consumo_07", "substatus_07", "testmenn_07", "testmecnn_07",
    "hora_08", "1q_consumo_08", "2q_consumo_08", "3q_consumo_08", "4q_consumo_08", "substatus_08", "testmenn_08", "testmecnn_08",
    "hora_09", "1q_consumo_09", "2q_consumo_09", "3q_consumo_09", "4q_consumo_09", "substatus_09", "testmenn_09", "testmecnn_09",
    "hora_10", "1q_consumo_10", "2q_consumo_10", "3q_consumo_10", "4q_consumo_10", "substatus_10", "testmenn_10", "testmecnn_10",
    "hora_11", "1q_consumo_11", "2q_consumo_11", "3q_consumo_11", "4q_consumo_11", "substatus_11", "testmenn_11", "testmecnn_11",
    "hora_12", "1q_consumo_12", "2q_consumo_12", "3q_consumo_12", "4q_consumo_12", "substatus_12", "testmenn_12", "testmecnn_12",
    "hora_13", "1q_consumo_13", "2q_consumo_13", "3q_consumo_13", "4q_consumo_13", "substatus_13", "testmenn_13", "testmecnn_13",
    "hora_14", "1q_consumo_14", "2q_consumo_14", "3q_consumo_14", "4q_consumo_14", "substatus_14", "testmenn_14", "testmecnn_14",
    "hora_15", "1q_consumo_15", "2q_consumo_15", "3q_consumo_15", "4q_consumo_15", "substatus_15", "testmenn_15", "testmecnn_15",
    "hora_16", "1q_consumo_16", "2q_consumo_16", "3q_consumo_16", "4q_consumo_16", "substatus_16", "testmenn_16", "testmecnn_16",
    "hora_17", "1q_consumo_17", "2q_consumo_17", "3q_consumo_17", "4q_consumo_17", "substatus_17", "testmenn_17", "testmecnn_17",
    "hora_18", "1q_consumo_18", "2q_consumo_18", "3q_consumo_18", "4q_consumo_18", "substatus_18", "testmenn_18", "testmecnn_18",
    "hora_19", "1q_consumo_19", "2q_consumo_19", "3q_consumo_19", "4q_consumo_19", "substatus_19", "testmenn_19", "testmecnn_19",
    "hora_20", "1q_consumo_20", "2q_consumo_20", "3q_consumo_20", "4q_consumo_20", "substatus_20", "testmenn_20", "testmecnn_20",
    "hora_21", "1q_consumo_21", "2q_consumo_21", "3q_consumo_21", "4q_consumo_21", "substatus_21", "testmenn_21", "testmecnn_21",
    "hora_22", "1q_consumo_22", "2q_consumo_22", "3q_consumo_22", "4q_consumo_22", "substatus_22", "testmenn_22", "testmecnn_22",
    "hora_23", "1q_consumo_23", "2q_consumo_23", "3q_consumo_23", "4q_consumo_23", "substatus_23", "testmenn_23", "testmecnn_23",
    "hora_24", "1q_consumo_24", "2q_consumo_24", "3q_consumo_24", "4q_consumo_24", "substatus_24", "testmenn_24", "testmecnn_24",
    "hora_25", "1q_consumo_25", "2q_consumo_25", "3q_consumo_25", "4q_consumo_25", "substatus_25", "testmenn_25", "testmecnn_25"
    )

    // "val: _*" : type ascription
    val df = data.toDF(newNames: _*)

    println("Number of rows = " + df.count())
    println("Number of columns = " + df.columns.length+"\n")
    println(df.show(10))

    println("")

    df.printSchema()


  }

}
