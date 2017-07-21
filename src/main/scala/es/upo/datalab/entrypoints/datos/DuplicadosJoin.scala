package es.upo.datalab.entrypoints.datos


import es.upo.datalab.utilities.{SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 21/04/17.
  */
object DuplicadosJoin {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

    TimingUtils.time {

//
//      val contratos = SparkSessionUtils.sparkSession.read
//        .option("delimiter",";")
//        .option("header", "true")
//        .option("inferSchema","true")
//        .csv(TabPaths.sTAB_00C)
//
//      contratos.show(100,false)
//      contratos.persist(nivel)
//      contratos.createOrReplaceTempView("MaestroContratos")
//
//      val clientes = SparkSessionUtils.sparkSession.read
//        .option("delimiter",";")
//        .option("header", "true")
//        .option("inferSchema","true")
//        .csv(TabPaths.sTAB_05C)
//
//      clientes.show(100,false)
//      clientes.persist(nivel)
//      clientes.createOrReplaceTempView("Clientes")
//
//      val j1 = sql(
//        """SELECT * FROM MaestroContratos JOIN Clientes
//          ON MaestroContratos.origen = Clientes.origen AND MaestroContratos.cemptitu = Clientes.cemptitu AND  MaestroContratos.ccontrat = Clientes.ccontrat AND MaestroContratos.cnumscct = Clientes.cnumscct """)
//
//      contratos.unpersist()
//      clientes.unpersist()
//
//      j1.persist(nivel)
//
//      println("J1 ( "+j1.count()+" registros )")
//      j1.show(100,false)
//
//
//      val j2 = j1.dropDuplicates()
//
//      j1.unpersist()
//
//      println("\n\nJ2 ( "+j2.count()+" registros sin repetición )")
//      j2.show(100,false)

    }

    SparkSessionUtils.sc.stop()

  }


}
