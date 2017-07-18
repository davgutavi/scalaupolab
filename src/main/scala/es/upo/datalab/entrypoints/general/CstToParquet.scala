package es.upo.datalab.entrypoints.general


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.sql.Date
import java.util.Calendar

import es.upo.datalab.entrypoints.tests.Funcion
import es.upo.datalab.utilities.{FunctionUtilities, LoadTableCsv, LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
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


    val p24 = "hdfs://192.168.47.247/user/gutierrez/TAB24/"

    val p24p = "hdfs://192.168.47.247/user/gutierrez/TAB24_parquet/"
    val pc = "/mnt/datos/recursos/ENDESA/headers/TAB24_headers.csv"

    TimingUtils.time {



      println("almacenando TAB24_01")
      val df24_01 = LoadTableCsv.loadTable(p24+"C3_Endesa_TAB_24_20170222_CZZ_20140601_20170123_1.csv",pc)
      df24_01.coalesce(1).write.option("header", "true").save(p24p + "TAB24_01")
      println("hecho")


      println("almacenando TAB24_02")
      val df24_02 = LoadTableCsv.loadTable(p24+"C3_Endesa_TAB_24_20170223_CZZ_20140601_20170123_2.csv",pc)
      df24_02.coalesce(1).write.option("header", "true").save(p24p + "TAB24_02")
      println("hecho")

      println("almacenando TAB24_03")
      val df24_03 = LoadTableCsv.loadTable(p24+"C3_Endesa_TAB_24_20170224_CZZ_20140601_20170123_3.csv",pc)
      df24_03.coalesce(1).write.option("header", "true").save(p24p + "TAB24_03")
      println("hecho")

//      println("almacenando TAB24_04")
//      val df24_04 = LoadTableCsv.loadTable(p24+"C3_Endesa_TAB_24_20170307_CZZ_20140601_20170123_4.csv",pc)
//      df24_04.coalesce(1).write.option("header", "true").save(p24p + "TAB24_04")
//      println("hecho")

      //      println("almacenando TAB24_05")
      //      val df24_05 = LoadTableCsv.loadTable(p24+"C3_Endesa_TAB_24_20170307_CZZ_20140601_20170123_5.csv",pc)
      //      df24_05.coalesce(1).write.option("header", "true").save(p24p + "TAB24_05")
      //      println("hecho")



















      //      df24.createOrReplaceTempView("T24")
//
//println("primera consulta")
//
//          sql("""SELECT * FROM T24 WHERE  fechalectura = 'ES0031405885059001TL2P'""").show(40)


/*
TAB24: fichero 1 => sin error de parseo


C3_Endesa_TAB_24_20170223_CZZ_20140601_20170123_2.csv
C3_Endesa_TAB_24_20170224_CZZ_20140601_20170123_3.csv

 */



      //            println("almacenando TAB08")
//
//            val df08 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB008_c",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB08_headers.csv")
//
//      df08.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB08")

//      df08.createOrReplaceTempView("T8")
//      val aux1 = sql("""SELECT * FROM T8 WHERE  fechorle>100000000000""")
//      val aux2 = sql("""SELECT * FROM T8 WHERE  fechorle<=100000000000""")
//      aux1.coalesce(1).write.csv("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB008_c")
//      aux2.coalesce(1).write.csv("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB008_i")




//      aux.show(80)

//      println(aux.count())



//      df08.printSchema()
//      df08.show(20,truncate=false)

//      println("df08 = "+df08.count())


//      df08.where("fechorle = 4220300978").show(20)




//      val ti = df08.map(Funcion,encoder = Encoders.TIMESTAMP).toDF()




//      println("ti = "+ti.count())
//
//
//      df08.withColumn("nvalue",ti.col("value")).show(40,truncate = false)








//            println("mostrando TAB00E")
//      val df00E = LoadTableParquet.loadTable(TabPaths.TAB00E)
////            val df00E = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB00E",
////                    "/media/davgutavi/ushdportatil/entregas/headers/TAB00E_headers.csv")
////            df00E.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB00E")
//            df00E.show(40,truncate = false)
//            println("TAB00E almacenada")




//            println("almacenando TAB00C")
//            val df00C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB00C",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB00C_headers.csv")
//            df00C.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB00C")
//      //      df00C.show(40)
//            println("TAB00C almacenada")
//

//
//
//
//            println("almacenando TAB02")
//            val df02 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB02",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB02_headers.csv")
//            df02.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB02")
////            df02.show(40)
//            println("TAB02 almacenada")
//
//            println("almacenando TAB06")
//            val df06 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB06",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB06_headers.csv")
//            df06.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB06")
//      //      df06.show(40)
//            println("TAB06 almacenada")
//
//            println("almacenando TAB15A")
//            val df15A = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15A",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB15A_headers.csv")
//            df15A.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB15A")
//      //      df15A.show(40)
//            println("TAB15A almacenada")
//
//            println("almacenando TAB15B")
//            val df15B = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15B",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB15B_headers.csv")
//            df15B.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB15B")
//      //      df15B.show(40)
//            println("TAB15B almacenada")
//
//            println("almacenando TAB15C")
//            val df15C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB15C",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB15C_headers.csv")
//            df15C.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB15C")
//      //      df15C.show(40)
//            println("TAB15C almacenada")
//
//
//            println("almacenando TAB16")
//            val df16 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB16",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB16_headers.csv")
//            df16.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB16")
//      //      df16.show(40)
//            println("TAB16 almacenada")
//
//            println("almacenando TAB01")
//            val df01 = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB01",
//              "/media/davgutavi/ushdportatil/entregas/headers/TAB01_headers.csv")
//            df01.coalesce(1).write.option("header", "true").save(TabPaths.prefix_database + "TAB01")
//      //      df01.show(40)
//            println("TAB01 almacenada")
//
//


















      //      println("almacenando TAB05A")
      //      val df05A = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05A",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05A_headers.csv")
      //      //      df05A.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05A")
      //      df05A.show(40,truncate = false)
      //      println("TAB05A almacenada")
      //
      //      println("almacenando TAB05B")
      //      val df05B = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05B",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05B_headers.csv")
      //      //      df05B.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05B")
      //      df05B.show(40,truncate = false)
      //      println("TAB05B almacenada")
      //
      //      println("almacenando TAB05C")
      //      val df05C = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05C",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05C_headers.csv")
      //      //      df05C.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05C")
      //      df05C.show(40,truncate = false)
      //      println("TAB05C almacenada")
      //
      //      println("almacenando TAB05D")
      //      val df05D = LoadTableCsv.loadTable("/media/davgutavi/ushdportatil/entregas/absolutas/descomprimidas/TAB05D",
      //        "/media/davgutavi/ushdportatil/entregas/headers/TAB05D_headers.csv")
      //      //      df05D.coalesce(1).write.option("header","true").save(TabPaths.prefix_database+"TAB05D")
      //      df05D.show(40,truncate = false)
      //      println("TAB05D almacenada")


      println("DONE!")

    }

    SparkSessionUtils.sc.stop()

  }

}
