package es.upo.datalab.utilities

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuilder



/**
  * Created by davgutavi on 15/03/17.
  */
object LoadTableCsv {

  final val datePattern = "yyyyMMdd"

  final val dateTime1Pattern = "yyyy-MM-dd HH:mm:ss"
  final val dateTime2Pattern = "YYYYMMddHHmmss"
  final val dateTime3Pattern =  "EEE MMM dd HH:mm:ss z YYYY"
  final val dateTime4Pattern =  "mmss"
  final val dateTime5Pattern =  "HHmmss"


  def loadTable(pathToData: String, pathToHeaders: String, dropDuplicates:Boolean=false): DataFrame = {


    val fields = new ArrayBuilder.ofRef[StructField]

    var tpattern = ""
    var dpattern = ""

    for (line <- scala.io.Source.fromFile(pathToHeaders).getLines) {

      val values = line.split(";")

      val t = values(1).trim

      val f = StructField(values(0).trim, getType(t), values(2).trim.toBoolean)

      if (t == "date")           {dpattern = datePattern}
      else if (t == "datetime")  {tpattern = dateTime1Pattern}
      else if (t == "datetime2") {tpattern = dateTime2Pattern}
      else if (t == "datetime3") {tpattern = dateTime3Pattern}
      else if (t == "datetime4") {tpattern = dateTime4Pattern}
      else if (t == "datetime5") {tpattern = dateTime5Pattern}
      fields += f

    }

    val schema = fields.result()

    val customSchema = StructType(schema)

    val loader = SparkSessionUtils.sparkSession.read
      .option("delimiter", ";")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
//        .option("charset","ASCII")
//      .option("mode", "DROPMALFORMED")
//      .option("nullValue","Null")
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
    else if (name.equalsIgnoreCase("datetime"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime2"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime3"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime4"))    r = DataTypes.TimestampType
    else if (name.equalsIgnoreCase("datetime5"))    r = DataTypes.TimestampType
    r
  }

}