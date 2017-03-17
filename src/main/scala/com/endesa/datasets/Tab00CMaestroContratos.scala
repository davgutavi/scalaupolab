package com.endesa.datasets

import com.utilities.SparkSessionUtils

/**
  * Created by davgutavi on 15/03/17.
  */
object Tab00CMaestroContratos {

  def main( args:Array[String] ):Unit = {

    val sqlContext = SparkSessionUtils.sqlContext

    val data = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      //.option("inferSchema", "true")
      .load(TabPaths.TAB_02_10)

    println("Number of rows = " + data.count())
    println("Number of columns = " + data.columns.length + "\n")
    println(data.show(10))

    println("")

    data.printSchema()

  }

}
