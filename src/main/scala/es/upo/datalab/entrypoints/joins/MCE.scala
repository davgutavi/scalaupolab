import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils, TabPaths, TimingUtils}
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 19/06/17.
  */
object MCE {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sqlContext

    import sqlContext._

      val df_00C = LoadTableParquet.loadTable(TabPaths.TAB_00C)
      df_00C.persist(nivel)
      df_00C.createOrReplaceTempView("MC")

      val df_16 = LoadTableParquet.loadTable(TabPaths.TAB_16)
      df_16.persist(nivel)
      df_16.createOrReplaceTempView("E")

      val df_01 = LoadTableParquet.loadTable(TabPaths.TAB_01)
      df_01.persist(nivel)
      df_01.createOrReplaceTempView("CC")


      ///**********************************PASO 1.1: MC U E ==>  eliminar duplicados por fapexpd >= fpsercon y fapexpd <= ffinvesu y por eliminación del secuencial de contrato del join

      val mce_aux = sql(
        """
                SELECT DISTINCT MC.origen, MC.cptocred, MC.cfinca, MC.cptoserv, MC.cderind, MC.cupsree, MC.ccounips,MC.cupsree2, MC.cpuntmed, MC.tpuntmed, MC.vparsist, MC.cemptitu,MC.ccontrat, MC.fpsercon,
                                MC.ffinvesu,E.csecexpe, E.fapexpd, E.finifran, E.ffinfran, E.anomalia, E.irregularidad, E.venacord, E.vennofai, E.torigexp, E.texpedie,E.expclass, E.testexpe,
                                E.fnormali, E.cplan, E.ccampa, E.cempresa, E.fciexped
                                FROM MC JOIN E
                                ON MC.origen=E.origen AND MC.cfinca=E.cfinca AND MC.cptoserv=E.cptoserv AND MC.cderind=E.cderind AND E.fapexpd >= MC.fpsercon AND E.fapexpd <= MC.ffinvesu
              """)

      println("Persistiendo mce_aux")
      mce_aux.show(5,truncate=false)
      mce_aux.persist(nivel)
      df_00C.unpersist()
      df_16.unpersist()
      mce_aux.createOrReplaceTempView("MCE_aux")


      ///**********************************PASO 1.2: Eliminar duplicados por fechas infinitas

      val mce = sql("""SELECT * FROM MCE_aux WHERE fpsercon <> "0002-11-30" OR ffinvesu <>  "9999-12-31" """)

      println("Persistiendo mce")
      mce.show(5,truncate=false)
      mce.persist(nivel)
      mce_aux.unpersist()
      mce.createOrReplaceTempView("MCE")
      println(mce.count())

      println("DONE!")

    mce.write.parquet("hdfs://10.141.0.224:9000/database/endesa/datasets/MCE")
    SparkSessionUtils.sc.stop()

    }








}
