package com.endesa.datasets

import com.utilities.SparkSessionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuilder
import org.apache.spark.sql.functions
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{trim, length, when}


import scala.collection.immutable
import scala.collection.immutable.StringOps
import scala.reflect.internal.util.StringOps

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

//    val data = sqlContext.read.format("com.databricks.spark.csv")
//      .option("delimiter", ";")
//      //.option("inferSchema", "true")
//      .option("dateFormat","yyyyMMdd")
//      .option("dateFormat"," yyyyMMdd")
//      //.option("dateFormat","yyyy-MM-dd HH:mm:ss")
//      .schema(customSchema)
//      .load(pathToData)

    val data = SparkSessionUtils.sparkSession.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("dateFormat","yyyyMMdd")
      .option("dateFormat"," yyyyMMdd")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace","true")
      .schema(customSchema)
      .load(pathToData)





//    val colnames = data.columns
//
//    for (colname<-colnames){
//
//      val col = data.col(colname)
//
////      functions.trim(col)
//
//      data.withColumn(colname, emptyToNull(col))
//
//    }

//    data.foreach(r => trimRows(r.toSeq) )


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

  private def trimRows (s:Seq[Any]) = {

         for (el <- s){

            if (el.isInstanceOf[String]){

              println("antes ="+el)

              val nel = el.asInstanceOf[String].trim()



              println("después ="+nel)

            }

        }
  }

  def emptyToNull(c: Column) = when(length(trim(c)) > 0, c)



}