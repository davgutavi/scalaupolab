package es.upo.datalab.datasets

import es.upo.datalab.utilities.SparkSessionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuilder



/**
  * Created by davgutavi on 15/03/17.
  */
object LoadTable {

  final val datePattern = "yyyyMMdd"
  final val dateTimePattern = "yyyy-MM-dd HH:mm:ss"


  def loadTable(pathToData: String, pathToHeaders: String): DataFrame = {


    val fields = new ArrayBuilder.ofRef[StructField]

    var pattern = ""

    for (line <- scala.io.Source.fromFile(pathToHeaders).getLines) {

      val values = line.split(";")

      val t = values(1).trim

//    val f = StructField(values(0).trim, getType(values(1).trim), values(2).trim.toBoolean)

      val f = StructField(values(0).trim, getType(t), values(2).trim.toBoolean)

      //      println("t = "+t)
      if      (t == "date") pattern  = datePattern
      else if (t=="datetime") pattern = dateTimePattern

      fields += f

    }

    val schema = fields.result()

    val customSchema = StructType(schema)

    val loader = SparkSessionUtils.sparkSession.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace","true")
      .schema(customSchema)

    if (!(pattern=="")){
      loader.option("dateFormat",pattern)
    }

    val data = loader.load(pathToData)

//    val loader = SparkSessionUtils.sparkSession.read.format("com.databricks.spark.csv")
//      .option("delimiter", ";")
//      .option("ignoreLeadingWhiteSpace", "true")
//      .option("ignoreTrailingWhiteSpace","true")
//      .schema(customSchema)
//      .load(pathToData)

    data

  }

  private def getType(name: String): DataType = {

    var r: DataType = null

    if (name.equalsIgnoreCase("integer"))      r = DataTypes.IntegerType
    else if (name.equalsIgnoreCase("string"))  r = DataTypes.StringType
    else if (name.equalsIgnoreCase("double"))  r = DataTypes.DoubleType
    else if (name.equalsIgnoreCase("boolean")) r = DataTypes.BooleanType
    else if (name.equalsIgnoreCase("long"))    r = DataTypes.LongType
    else if (name.equalsIgnoreCase("date"))    r = DataTypes.DateType
    else if (name.equalsIgnoreCase("datetime"))    r = DataTypes.DateType
//    else if (name.equalsIgnoreCase("date"))    r = DataTypes.StringType
//    else if (name.equalsIgnoreCase("datetime"))    r = DataTypes.StringType


    r
  }

}