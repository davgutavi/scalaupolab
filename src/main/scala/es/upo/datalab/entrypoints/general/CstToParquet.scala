package es.upo.datalab.entrypoints.general


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.sql.Date
import java.util.Calendar

import es.upo.datalab.utilities.{LoadTableCsv, LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.storage.StorageLevel


/**
  * Created by davgutavi on 12/05/17.
  */
object CstToParquet {

  val sqlContext = SparkSessionUtils.sqlContext

  import sqlContext._

  import org.apache.spark.sql._

  def main(args: Array[String]): Unit = {

    TimingUtils.time {

//      println("almacenando TAB00C")
//      val df00C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB00C",
//        "/media/davgutavi/ushdportatil/entregas/headers/TAB00C_headers.csv")
////      df00C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//      df00C.show(40)
//      println("TAB00C almacenada")

//      println("almacenando TAB00E")
//      val df00E = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB00E",
//        "/media/davgutavi/ushdportatil/entregas/headers/TAB00E_headers.csv")
//      //      df00E.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//      df00E.show(40)
//      println("TAB00E almacenada")

//            println("almacenando TAB01")
//            val df01 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB01",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB01_headers.csv")
//            //      df01.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//            df01.show(40)
//            println("TAB01 almacenada")

//                  println("almacenando TAB02")
//                  val df02 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB02",
//                    "/media/davgutavi/ushdportatil/entregas/headers/TAB02_headers.csv")
//                  //      df02.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//                  df02.show(40)
//                  println("TAB02 almacenada")

//      println("almacenando TAB02")
//      val df02 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB02",
//        "/media/davgutavi/ushdportatil/entregas/headers/TAB02_headers.csv")
//      //      df02.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//      df02.show(40)
//      println("TAB02 almacenada")

//            println("almacenando TAB06")
//            val df06 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB06",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB06_headers.csv")
//            //      df06.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB06")
//            df06.show(40)
//            println("TAB06 almacenada")


//                  println("almacenando TAB08")
//                  val df08 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB08",
//                    "/media/davgutavi/ushdportatil/entregas/headers/TAB08_headers.csv")
//                  // df08.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB08")
////                  df08.show(1000)
//                  println("TAB08 almacenada")




//      val loader = SparkSessionUtils.sparkSession.read
//        .option("delimiter", ";")
//        .option("ignoreLeadingWhiteSpace", "true")
//        .option("ignoreTrailingWhiteSpace", "true")
//          .option("inferSchema", "true")
//      val t1 = loader.csv("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB08")
//      t1.printSchema()
////      t1.show(40)
//      t1.createOrReplaceTempView("T")
//      sql("""SELECT * FROM T WHERE _c4 != 0 """).show(40)




//            println("almacenando TAB15A")
//            val df15A = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15A",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB15A_headers.csv")
//            //      df02.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//            //df15A.show(40)
//            println("TAB15A almacenada")
//          df15A.select("*").where("fejectdc!='00000000'").show(40)


//      println("almacenando TAB15B")
//      val df15B = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15B",
//        "/media/davgutavi/ushdportatil/entregas/headers/TAB15B_headers.csv")
//      //      df15B.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//      df15B.show(40)
//      println("TAB15B almacenada")

//            println("almacenando TAB15C")
//            val df15C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15C",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB15C_headers.csv")
//            //      df15C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")
//            df15C.show(40)
//               println("TAB15C almacenada")

      println("almacenando TAB15C")
      val df15C = LoadTableCsv.loadTable(TabPaths.TAB15C_csv_hdpclab,TabPaths.TAB15C_headers_hdpclab)
      //      df15C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB00C")

//      df15C.show(20,truncate = false)


//      val ndf15C = df15C.withColumn("tiempo",func2(df15C.col("fmovtdc"),df15C.col("vhmovtdc")))

      def func1 (date:Date, time:Timestamp):Timestamp={


        val calendar = Calendar.getInstance()

        calendar.setTime(date)


        var t = calendar.getTimeInMillis() + time.getTime()

        val  n = new Timestamp(t)

        n

      }



      val lt = List[Row] ()

      for (l:Row <- df15C){

          val date = l.getDate(9)

          val time = l.getTimestamp(10)

          val ntime = func1(date,time)

          lt :+ Row (ntime)

      }

      println(lt)

      val struct = StructType(StructField("fecha_tiempo", DataTypes.TimestampType, true)::Nil)


//    val dtf =  sqlContext.createDataFrame(lt,struct)
//
//      df15C.join(dtf)
//
//      df15C.show(20)









//
//
//      df15C.write
//        .option("timestampFormat", "HH:mm:ss")
//        .option("dateFormat", "yyyyMMdd")
//           .parquet(TabPaths.TAB15C)
//
//
//      println("TAB15C almacenada")
//
//
//      LoadTableParquet.loadTable(TabPaths.TAB15C).show(40,truncate = false)




//                   val i = df15C.head().getString(9)
//      val f = new SimpleDateFormat("yyyyMMdd")
//      println("Patrón de "+i+" = "+f.parse(i))
//      df15C.printSchema()

//        df15C.select("*").where("fmovtdc!='1970-01-01'").show(40)


      //
      //      println("almacenando TAB15C")
      //      val df15C = LoadTableCsv.loadTable(TabPaths.TAB15C_csv, TabPaths.TAB15C_headers)
      //      df15C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB15C")
      //      println("TAB15C almacenada")
      //
      //      println("almacenando TAB16")
      //      val df16 = LoadTableCsv.loadTable(TabPaths.TAB16_csv, TabPaths.TAB16_headers)
      //      df16.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB16")
      //      println("TAB16 almacenada")
      //
      //      println("almacenando TAB24")
      //      val df24 = LoadTableCsv.loadTable(TabPaths.TAB24_csv, TabPaths.TAB24_headers)
      //
      //      df24.show(10,truncate=false)
      //
      //      val fl = df24.head().getString(1)
      //
      //      val  f = new SimpleDateFormat("EEE MMM dd HH:mm:ss z YYYY")
      //
      //      println("Apply = "+f.applyPattern(fl))
      //
      //
      //      //df24.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB24")
      //      println("TAB24 almacenada")



////      println("almacenando TAB05A")
////      val df05A = LoadTableCsv.loadTable(TabPaths.TAB05A_csv, TabPaths.TAB05A_headers)
////      df05A.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05A")
////      println("TAB05A almacenada")
////
////      println("almacenando TAB05B")
////      val df05B = LoadTableCsv.loadTable(TabPaths.TAB05B_csv, TabPaths.TAB05B_headers)
////      df05B.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05B")
////      println("TAB05B almacenada")
////
////      println("almacenando TAB05C")
////      val df05C = LoadTableCsv.loadTable(TabPaths.TAB05C_csv, TabPaths.TAB05C_headers)
////      df05C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05C")
////      println("TAB05C almacenada")
////
////      println("almacenando TAB05D")
////      val df05D = LoadTableCsv.loadTable(TabPaths.TAB05D_csv, TabPaths.TAB05D_headers)
////      df05D.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05D")
////      println("TAB05D almacenada")
//


      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}
