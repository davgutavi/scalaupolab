package com.entrypoints

import com.endesa.datasets.{LoadTable, TabPaths}
import com.utilities.{SparkSessionUtils, TimingUtils}
import org.apache.spark.storage.StorageLevel

/**
  * Created by davgutavi on 23/03/17.
  */
object ExpedientesFraude {

  def main( args:Array[String] ):Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    import SparkSessionUtils.sqlContext.sql

    TimingUtils.time{

      val df_15C = LoadTable.loadTable(TabPaths.TAB_15C,TabPaths.TAB_15C_headers)
      df_15C.persist(nivel)
      df_15C.registerTempTable("movtdc")

//      val df_15B = LoadTable.loadTable(TabPaths.TAB_15B,TabPaths.TAB_15B_headers)
//      df_15B.persist(nivel)
//      df_15B.registerTempTable("optdc")
//
//      val df_15A = LoadTable.loadTable(TabPaths.TAB_15A,TabPaths.TAB_15A_headers)
//      df_15A.persist(nivel)
//      df_15A.registerTempTable("tdc")

      val df_00C = LoadTable.loadTable(TabPaths.TAB_00C,TabPaths.TAB_00C_headers)
      df_00C.persist(nivel)
      df_00C.registerTempTable("contratos")

//      val df_05C = LoadTable.loadTable(TabPaths.TAB_05C, TabPaths.TAB_05C_headers)
//      df_05C.cache()
//      df_05C.registerTempTable("clientes")
//
      val df_16 = LoadTable.loadTable(TabPaths.TAB_16,TabPaths.TAB_16_headers)
      df_16.persist(nivel)
      df_16.registerTempTable("expedientes")


       val  j1 = sql(
                    """SELECT *
                       FROM movtdc JOIN contratos
                       ON movtdc.cemptitu=contratos.cemptitu
                    """)
      println("\nMov TDC (cemptitu) ("+j1.count()+" registros)\n")

      val  j2 = sql(
        """SELECT *
                       FROM movtdc JOIN contratos
                       ON movtdc.ccontrat= contratos.ccontrat
                    """)
      println("\nMov TDC (ccontrat) ("+j2.count()+" registros)\n")

      val  j3 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.origen = contratos.origen AND expedientes.cfinca = contratos.cfinca AND expedientes.cptoserv = contratos.cptoserv AND expedientes.cderind = contratos.cderind AND expedientes.fapexpd = contratos.fpsercon
                    """)
      println("\nExpedientes (origen, cfinca, cptoserv, cderind, fapexpd = fpsercon) ("+j3.count()+" registros)\n")


      val  j4 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.origen = contratos.origen AND expedientes.cfinca = contratos.cfinca AND expedientes.cptoserv = contratos.cptoserv AND expedientes.cderind = contratos.cderind AND expedientes.fapexpd = contratos.ffinvesu
                    """)
      println("\nExpedientes (origen, cfinca, cptoserv, cderind, fapexpd = ffinvesu) ("+j4.count()+" registros)\n")

      val  j5 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.origen = contratos.origen AND expedientes.cfinca = contratos.cfinca
                    """)
      println("\nExpedientes (origen, cfinca) ("+j5.count()+" registros)\n")


      val  j6 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.origen = contratos.origen AND expedientes.cptoserv = contratos.cptoserv
                    """)
      println("\nExpedientes (origen, cptoserv) ("+j6.count()+" registros)\n")

      val  j7 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.origen = contratos.origen AND expedientes.cderind = contratos.cderind
                    """)
      println("\nExpedientes (origen, cderind) ("+j7.count()+" registros)\n")


      val  j8 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.cfinca = contratos.cfinca
                    """)
      println("\nExpedientes (cfinca) ("+j8.count()+" registros)\n")

      val  j9 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.cptoserv = contratos.cptoserv
                    """)
      println("\nExpedientes (cptoserv) ("+j9.count()+" registros)\n")

      val  j10 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.cderind = contratos.cderind
                    """)
      println("\nExpedientes (cderind) ("+j10.count()+" registros)\n")

      val  j11 = sql(
        """SELECT *
                       FROM expedientes JOIN contratos
                       ON expedientes.cemptitu = contratos.cemptitu
                    """)
      println("\nExpedientes (cemptitu) ("+j11.count()+" registros)\n")



//      val  j2 = sql(
//        """SELECT *
//           FROM optdc JOIN contratos
//           ON optdc.origen = contratos.origen AND optdc.cemptitu = contratos.cemptitu AND optdc.ccontrat = contratos.ccontrat
//                    """)
//      println("\nJ2 ("+j2.count()+" registros)\n")

//      val  j2 = sql(
//        """SELECT *
//           FROM optdc JOIN contratos
//           ON optdc.cemptitu = contratos.cemptitu
//                    """)
//      println("\nJ2 ("+j2.count()+" registros)\n")

//      val  j2 = sql(
//        """SELECT *
//           FROM optdc JOIN contratos
//           ON optdc.ccontrat = contratos.ccontrat
//                    """)
//      println("\nJ2 ("+j2.count()+" registros)\n")


//      j1.persist(nivel)
//      j1.registerTempTable("con-tdc")

//      val  j2 = sql(
//        """SELECT *
//                       FROM con-tdc JOIN contratos
//                       ON tdc.origen = contratos.origen AND tdc.cemptitu=contratos.cemptitu, tdc.ccontrat= contratos.ccontrat
//                    """)
//      println("\nJ2 ("+j2.count()+" registros)\n")
//      j2.show(20)


//      val  j1 = sql(
//              """SELECT *
//                 FROM contratos JOIN expedientes
//                 ON contratos.origen = expedientes.origen
//              """)
//            println("\nJ1 ("+j1.count()+" registros)\n")
//            j1.show(20)

//
//      val  j2 = sql(
//        """SELECT *
//                 FROM contratos JOIN expedientes
//                 ON contratos.cemptitu = expedientes.cemptitu
//              """)
//      println("\nJ2 ("+j2.count()+" registros)\n")
//      j2.show(20)
//
//      val  j3 = sql(
//        """SELECT *
//                 FROM contratos JOIN expedientes
//                 ON contratos.cptoserv = expedientes.cptoserv
//              """)
//      println("\nJ3 ("+j3.count()+" registros)\n")
//      j3.show(20)

//      val  j4 = sql(
//              """SELECT *
//                 FROM contratos JOIN expedientes
//                 ON contratos.cfinca = expedientes.cfinca
//              """)
//            println("\nJ4 ("+j4.count()+" registros)\n")
//            j4.show(20)
//
//
//      val j5 =  sql(
//        """SELECT *
//                 FROM expedientes JOIN contratos
//                 ON expedientes.origen=contratos.origen AND expedientes.cemptitu=contratos.cemptitu AND expedientes.cfinca=contratos.cfinca AND expedientes.cptoserv=contratos.cptoserv
//              """)
//      println("\nJ5 ("+j5.count()+" registros)\n")
//      j5.show(20)



      //      val exp_01 = sql("""SELECT origen, cemptitu, cfinca, cptoserv, irregularidad FROM expedientes""")
//      cn_01.registerTempTable("exp")
//      println("\nExpedientes ("+exp_01.count()+" registros)\n")
//      exp_01.show(20)

//      val  j1 = sql(
//        """SELECT *
//           FROM contratos JOIN expedientes
//           ON contratos.origen = expedientes.origen AND contratos.cemptitu= expedientes.cemptitu AND contratos.cfinca = expedientes.cfinca AND contratos.cptoserv = expedientes.cptoserv
//        """)
//      println("\nJ1 ("+j1.count()+" registros)\n")
//      j1.show(20)
//
//      val  j2 = sql(
//        """SELECT *
//           FROM contratos JOIN expedientes
//           ON contratos.origen = expedientes.origen AND contratos.cemptitu= expedientes.cemptitu AND contratos.cfinca = expedientes.cfinca
//        """)
//      println("\nJ2 ("+j2.count()+" registros)\n")
//      j2.show(20)
//
//
//      val  j3 = sql(
//        """SELECT *
//           FROM contratos JOIN expedientes
//           ON contratos.origen = expedientes.origen AND contratos.cfinca = expedientes.cfinca
//        """)
//      println("\nJ3 ("+j3.count()+" registros)\n")
//      j3.show(20)
//
//      val  j4 = sql(
//        """SELECT *
//           FROM contratos JOIN expedientes
//           ON contratos.cfinca = expedientes.cfinca
//        """)
//      println("\nJ4 ("+j4.count()+" registros)\n")
//      j4.show(20)






//      println("Construyendo Maestro Contratos-Clientes\n")

//      val j1 = sql(
//        """SELECT contratos.origen, contratos.cfinca, contratos.cptoserv, contratos.cderind, contratos.fpsercon, contratos.ffinvesu, clientes.ccliente, clientes.dapersoc
//            FROM contratos JOIN clientes
//            ON contratos.origen = clientes.origen AND contratos.cemptitu = clientes.cemptitu AND contratos.ccontrat = clientes.ccontrat AND contratos.cnumscct = clientes.cnumscct
//        """)
//
//      println("Persistiendo Contratos-Clientes\n")
//      j1.persist(nivel)
//      println("Registrando Tabla Contratos-Clientes\n")
//      j1.registerTempTable("con_cli")
//
//      println("\nJoin Contratos-Clientes ("+j1.count()+" registros)\n")
//      j1.show(5)
//
//      println("Borrando Maestro Contratos, Clientes\n")
//      df_00C.unpersist()
//      df_05C.unpersist()

//
//      println("Construyendo Contratos-Clientes-Expedientes\n")
//
//      val j2 = sql(
//        """SELECT con_cli.ccliente, con_cli.dapersoc, expedientes.irregularidad
//            FROM con_cli JOIN expedientes
//            ON con_cli.origen = expedientes.origen AND con_cli.cfinca = expedientes.cfinca AND con_cli.cptoserv = expedientes.cptoserv AND con_cli.cderind = expedientes.cderind
//      """)

//      println("Construyendo Contratos-Expedientes\n")
//
//      val j2 = sql(
//        """SELECT contratos.cfinca, expedientes.irregularidad
//            FROM contratos JOIN expedientes
//            ON contratos.origen = expedientes.origen AND contratos.cemptitu= expedientes.cemptitu AND contratos.cfinca = expedientes.cfinca AND contratos.cptoserv = expedientes.cptoserv
//      """)
//      println("\nJoin Contratos-Expedientes ("+j2.count()+" registros)\n")
//      j2.show(5)


//      println("Persistiendo Contratos-Clientes-Expedientes\n")
//      j2.persist(nivel)
//       println("Registrando Tabla Contratos-Clientes-Expedientes\n")
//      j2.registerTempTable("con_cli_exp")
//
//      println("\nJoin Contratos-Clientes-Expedientes ("+j2.count()+" registros)\n")
//      j2.show(5)
//
//      println("Borrando Expedientes, Contratos-Clientes\n")
//      df_16.unpersist()
//      j1.unpersist()
//
//      println("Construyendo Expedientes Fraudulentos\n")
//
//      val j3 = sql(
//        """SELECT ccliente, dapersoc, irregularidad
//           FROM con_cli_exp
//            WHERE irregularidad = 'S'
//      """)
//      println("\nBorrando Contratos-Clientes-Expedientes")
//      j2.unpersist()
//
//      println("\nExpedientes Fraudulentos ("+j3.count()+" registros)\n")
//      j3.show(5)

    }



  }

}
