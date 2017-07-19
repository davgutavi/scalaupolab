package es.upo.datalab.utilities

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuilder

/**
  * Created by davgutavi on 19/07/17.
  */
object LoadTableCsvHDFS {

  final val datePattern01 = "yyyyMMdd"
  final val datePattern02 = "YYYYMMddHHmmss"

  final val dateTimePattern01 = "yyyy-MM-dd HH:mm:ss"
  final val dateTimePattern02 = "YYYYMMddHHmmss"
  final val dateTimePattern03 =  "EEE MMM dd HH:mm:ss z YYYY"
  final val dateTimePattern04 =  "mmss"
  final val dateTimePattern05 =  "HHmmss"

  var tpattern:String = ""
  var dpattern:String = ""

  def loadTable(pathToData: String, pathToHeaders: String, dropDuplicates:Boolean=false): DataFrame = {


    val rawHeaders = SparkSessionUtils.sparkSession.read.option("delimiter",";").csv(pathToHeaders)

    val customSchema = StructType(rawHeaders.rdd.map(l=>buildStructFields(l)).collect())

//    print(customSchema.mkString)

    val loader = SparkSessionUtils.sparkSession.read
      .option("delimiter", ";")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      //.option("charset","ASCII")
      //.option("mode", "DROPMALFORMED")
      //.option("nullValue","Null")
      .schema(customSchema)

    if (!(dpattern == "")) {
      loader.option("dateFormat", dpattern)
    }

    if (!(tpattern == "")) {
      loader.option("timestampFormat", tpattern)
    }


    //    println(pathToData)

    val data = loader.csv(pathToData)

    //    println("loaded")

    var r:DataFrame = null

    if (dropDuplicates){
      r = data.dropDuplicates()
    }
    else{
      r = data
    }

    r
  }

  private def getType(name: String): DataType = {

    var r: DataType = null

    if (name.equalsIgnoreCase("integer"))      r = DataTypes.IntegerType
    else if (name.equalsIgnoreCase("string"))  r = DataTypes.StringType
    else if (name.equalsIgnoreCase("double"))  r = DataTypes.DoubleType
    else if (name.equalsIgnoreCase("boolean")) r = DataTypes.BooleanType
    else if (name.equalsIgnoreCase("long"))    r = DataTypes.LongType
    else if (name.equalsIgnoreCase("date"))    r = DataTypes.DateType
    else if (name.equalsIgnoreCase("date2"))    r = DataTypes.DateType
    else if (name.equalsIgnoreCase("datetime"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime2"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime3"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime4"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime5"))    r = DataTypes.TimestampType
    r
  }

  private def buildStructFields(line: Row):StructField = {

    val v0 = line.getString(0)
    val v1 = line.getString(1)
    val v2 = line.getString(2)

    val t = v1.trim

    val f = StructField(v0.trim, getType(t), v2.trim.toBoolean)

    if (t == "date") {
      dpattern = datePattern01
    }
    else if (t == "date2") {
      dpattern = datePattern02
    }
    else if (t == "datetime") {
      tpattern = dateTimePattern01
    }
    else if (t == "datetime2") {
      tpattern = dateTimePattern02
    }
    else if (t == "datetime3") {
      tpattern = dateTimePattern03
    }
    else if (t == "datetime4") {
      tpattern = dateTimePattern04
    }
    else if (t == "datetime5") {
      tpattern = dateTimePattern05
    }

//    println("[ "+dpattern+" , "+tpattern+" ] "+f)

    f
  }

}
