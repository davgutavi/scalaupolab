package es.upo.datalab.entrypoints.procesos

import es.upo.datalab.utilities._
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.storage.StorageLevel

object Tab24PreProcessing {


  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    val sparkSession = SparkSessionUtils.sparkSession

    TimingUtils.time {

      val t24 = LoadTableParquet.loadTable(TabPaths.TAB24)

//      t24.show(20)


//      val w1:Dataset[Row] = t24.map(r => StringToPowerConsumption.call(r) )


      import sparkSession.implicits._
      import sqlContext.implicits._

      val a1 = t24.rdd.map(r => StringToPowerConsumption.call(r))

      t24.sqlContext.createDataFrame(a1,Struct)



    }

    SparkSessionUtils.sc.stop()


  }




}
