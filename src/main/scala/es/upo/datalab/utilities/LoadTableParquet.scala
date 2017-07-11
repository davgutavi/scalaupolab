package es.upo.datalab.utilities

import es.upo.datalab.utilities.LoadTableCsv.{datePattern, dateTime1Pattern}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuilder

/**
  * Created by davgutavi on 15/05/17.
  */
object LoadTableParquet {

  def loadTable(pathToData: String): DataFrame = {

    val r = SparkSessionUtils.sparkSession.read.parquet(pathToData)

    r
  }



}
