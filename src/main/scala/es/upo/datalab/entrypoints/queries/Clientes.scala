package es.upo.datalab.entrypoints.queries


import es.upo.datalab.utilities.{LoadTable, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 21/04/17.
  */
object Clientes {


  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers,true)
      df_05C.persist(nivel)
      df_05C.createOrReplaceTempView("Clientes")

//      df_05C.show(100,false)
//
////      df_05C.coalesce(1).limit(100).write.option("delimiter",";").option("header","true").csv(TabPaths.root+"datasets/sTAB_05C")
//
//      val q1 = sql("""SELECT origen, cemptitu, ccontrat, cnumscct FROM Clientes""")
//
//      df_05C.unpersist()
//
//      q1.persist(nivel)
//
//      val w1 = q1.count()
//
//      println("origen, cemptitu, ccontrat, cnumscct")
//      q1.show(5,false)
//
//      val q2 = q1.dropDuplicates()
//
//      q1.unpersist()
//
//      q2.persist(nivel)
//
//      val w2 = q2.count()
//
//      println("origen, cemptitu, ccontrat, cnumscct sin repetir")
//      q2.show(5,false)
//
//      q2.unpersist()
//      q2.unpersist()
//
//      println("\n\nRegistros = "+w1)
//      println("Registros sin repetir = "+w2)
//      println("Diferencia = "+(w1-w2))


//      val q3 = sql(
//        """SELECT ccontrat, cnumscct,fpsercon,ffinvesu, cupsree, cpuntmed, tpuntmed
//                            FROM Clientes
//                            ORDER BY ccontrat, cnumscct
//                             """)
//
//      q3.show(20, false)
//      val q3c = q3.count()
//
//      val q3s = q3.dropDuplicates()
//
//      q3s.show(20, false)
//      val q3sc = q3s.count()
//
//      println("q3 = " + q3c + " registros")
//      println("q3s = " + q3sc + " registros")
//      println("Diferencia = " + (q3c - q3sc))



    }

    SparkSessionUtils.sc.stop()

  }


}
