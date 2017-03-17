package com.endesa.datasets

import com.utilities.SparkSessionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuilder

/**
  * Created by davgutavi on 15/03/17.
  */
object LoadTable {


  def loadTable(pathToData: String, pathToHeaders: String): DataFrame = {


    val sqlContext = SparkSessionUtils.sqlContext

    val fields = new ArrayBuilder.ofRef[StructField]()

    for (line <- scala.io.Source.fromFile(pathToHeaders).getLines) {

      val values = line.split(";")

      val f = StructField(values(0).trim, getType(values(1).trim), values(2).trim.toBoolean)

      fields += f
    }

    val schema = fields.result()

    //println("Schema size = " + schema.length+"\n")

    val customSchema = StructType(schema)

    val data = sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      //.option("inferSchema", "true")
      .option("dateFormat","yyyyMMdd")
      .option("dateFormat"," yyyyMMdd")
      //.option("dateFormat","yyyy-MM-dd HH:mm:ss")
      .schema(customSchema)
      .load(pathToData)

    data

  }

  private def getType(name: String): DataType = {

    var r: DataType = null

    if (name.equalsIgnoreCase("integer"))      r = DataTypes.IntegerType
    else if (name.equalsIgnoreCase("string"))  r = DataTypes.StringType
    else if (name.equalsIgnoreCase("double"))  r = DataTypes.DoubleType
    else if (name.equalsIgnoreCase("boolean")) r = DataTypes.BooleanType
    else if (name.equalsIgnoreCase("long"))    r = DataTypes.LongType
    //else if (name.equalsIgnoreCase("date"))    r = DataTypes.DateType
    else if (name.equalsIgnoreCase("date"))    r = DataTypes.StringType


    r
  }

}