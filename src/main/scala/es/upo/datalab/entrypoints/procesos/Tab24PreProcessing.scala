package es.upo.datalab.entrypoints.procesos

import es.upo.datalab.utilities._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.storage.StorageLevel

object Tab24PreProcessing {


  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    val sparkSession = SparkSessionUtils.sparkSession

    import sparkSession.implicits._
    import sqlContext.implicits._


    TimingUtils.time {


     val t24 = LoadTableParquet.loadTable( TabPaths.TAB24 )

      val struct1 =
        StructType(
          StructField( "v_01", IntegerType, true ) ::
          StructField( "v_02", IntegerType, true )::
          StructField( "v_03", IntegerType, true )::
          StructField( "v_04", IntegerType, true )::
          StructField( "v_05", IntegerType, true )::
          StructField( "v_06", IntegerType, true )::
          StructField( "v_07", IntegerType, true )::
          StructField( "v_08", IntegerType, true )::
          StructField( "v_09", IntegerType, true )::
          StructField( "v_10", IntegerType, true )::
          StructField( "v_11", IntegerType, true )::
          StructField( "v_12", IntegerType, true )::
          StructField( "v_13", IntegerType, true )::
          StructField( "v_14", IntegerType, true )::
          StructField( "v_15", IntegerType, true )::
          StructField( "v_16", IntegerType, true )::
          StructField( "v_17", IntegerType, true )::
          StructField( "v_18", IntegerType, true )::
          StructField( "v_19", IntegerType, true )::
          StructField( "v_20", IntegerType, true )::
          StructField( "v_21", IntegerType, true )::
          StructField( "v_22", IntegerType, true )::
          StructField( "v_23", IntegerType, true )::
          StructField( "v_24", IntegerType, true)::
            StructField( "r_01", IntegerType, true ) ::
            StructField( "r_02", IntegerType, true )::
            StructField( "r_03", IntegerType, true )::
            StructField( "r_04", IntegerType, true )::
            StructField( "r_05", IntegerType, true )::
            StructField( "r_06", IntegerType, true )::
            StructField( "r_07", IntegerType, true )::
            StructField( "r_08", IntegerType, true )::
            StructField( "r_09", IntegerType, true )::
            StructField( "r_10", IntegerType, true )::
            StructField( "r_11", IntegerType, true )::
            StructField( "r_12", IntegerType, true )::
            StructField( "r_13", IntegerType, true )::
            StructField( "r_14", IntegerType, true )::
            StructField( "r_15", IntegerType, true )::
            StructField( "r_16", IntegerType, true )::
            StructField( "r_17", IntegerType, true )::
            StructField( "r_18", IntegerType, true )::
            StructField( "r_19", IntegerType, true )::
            StructField( "r_20", IntegerType, true )::
            StructField( "r_21", IntegerType, true )::
            StructField( "r_22", IntegerType, true )::
            StructField( "r_23", IntegerType, true )::
            StructField( "r_24", IntegerType, true)::
            StructField( "t_01", IntegerType, true ) ::
            StructField( "t_02", IntegerType, true )::
            StructField( "t_03", IntegerType, true )::
            StructField( "t_04", IntegerType, true )::
            StructField( "t_05", IntegerType, true )::
            StructField( "t_06", IntegerType, true )::
            StructField( "t_07", IntegerType, true )::
            StructField( "t_08", IntegerType, true )::
            StructField( "t_09", IntegerType, true )::
            StructField( "t_10", IntegerType, true )::
            StructField( "t_11", IntegerType, true )::
            StructField( "t_12", IntegerType, true )::
            StructField( "t_13", IntegerType, true )::
            StructField( "t_14", IntegerType, true )::
            StructField( "t_15", IntegerType, true )::
            StructField( "t_16", IntegerType, true )::
            StructField( "t_17", IntegerType, true )::
            StructField( "t_18", IntegerType, true )::
            StructField( "t_19", IntegerType, true )::
            StructField( "t_20", IntegerType, true )::
            StructField( "t_21", IntegerType, true )::
            StructField( "t_22", IntegerType, true )::
            StructField( "t_23", IntegerType, true )::
            StructField( "t_24", IntegerType, true):: Nil)
//
      val struct2 =
        StructType(
            StructField( "validation_v", StringType, true ) ::
            StructField( "validation_r", StringType, true )::
            StructField("validation_t", StringType, true)::Nil)


      val a1 = t24.rdd.map( r => StringToPowerConsumption.call( r ) )
      val n1:DataFrame = sqlContext.createDataFrame(a1,struct1)
//      n1.show(40,truncate = false)
      n1.coalesce(1).write.option("header", "true").save("hdfs://192.168.47.247/user/gutierrez/endesa/database_parquet/TAB24_aux/n1")

      val a2 = t24.rdd.map( r => ValidationValues.call( r ) )
      val n2:DataFrame = sqlContext.createDataFrame(a2,struct2)
//     n2.show(40,truncate = false)
      n2.coalesce(1).write.option("header", "true").save("hdfs://192.168.47.247/user/gutierrez/endesa/database_parquet/TAB24_aux/n2")









    }

    SparkSessionUtils.sc.stop()


  }


}
