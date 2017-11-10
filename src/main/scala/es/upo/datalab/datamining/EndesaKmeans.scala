package es.upo.datalab.datamining

import java.io.{BufferedWriter, File, FileWriter}
import java.text.{DecimalFormat, DecimalFormatSymbols}

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame


import scala.collection.mutable


object EndesaKmeans {

  //**ENTRADAS:

  //****Nombre global del experimento
  final val experimento = "t123_364d_pen_04"

  //****Ruta raíz de los ficheros de salida
  final val outputRootPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/clustering/"+experimento+"/"

  //****Parámetros del K-means
  //default: {k: 2, maxIterations: 20, initializationSteps: 2, tolerance: 1e-4, seed: random}
  final val k = Array(2)
  final val iteraciones = Array(100)
  final val pasosInicializacion = Array(2)
  final val tolerancia = Array(0.0001)
  final val semilla = Array(1L)



  //****Ruta del dataset de entrada
//  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_454d"
//  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_364d"
//  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_454d_pendientes"
  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_364d_pendientes"


  //**SALIDAS:

  //  Fichero #outputRootPath#/#experimento#/exp_#experimento#_mod_#número de modelo#_pred_comp     => resultados de agrupamiento con todas las columnas
  //  NO: Fichero #outputRootPath#/#experimento#/exp_#experimento#_mod_#número de modelo#_pred_cpuntmed => resultados de agrupamiento sólo con cpuntmed y prediction
  //  Fichero #outputRootPath#/#experimento#/exp_#experimento#_mod_#número de modelo#_centroids.csv => centroides en formato csv con separador ";" para que se pueda abrir directamente con excel
  //  Fichero #outputRootPath#/#experimento#/exp_#experimento#_mod_#número de modelo#_study.csv     => caracterización para no fraude (0) y fraude (1) de los clusters resultantes de cada modelo #i#


  val sparkSession = SparkSessionUtils.sc
  val sqlContext = SparkSessionUtils.sql

  def main(args: Array[String]): Unit = {

    //*******Datos de entrada:
    println("Cargando dataframe")

    val df = LoadTableParquet.loadTable(datasetPath)
    val dataset = df.drop("label", "cenae", "ccodpost","nle")
    val inputCols = dataset.drop("cpuntmed").columns

    //*******Assembler:
    val fa = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")

    //*******GRID K-MEANS:

    val pipmodels = kmeansGrid(fa,dataset,k,iteraciones,semilla,tolerancia,pasosInicializacion)

    println("********************************************************************************************************************************************************\n")

    //*******RESULTADOS

    var i = 0

    for (m <- pipmodels){

      println("Generate prediction "+i+":")

      val test = m.transform(dataset)

//      val res = test.select("cpuntmed", "prediction")

      test.write.option("header", "true").save(outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_comp")

      println("Complete prediction saved: "+outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_comp")

//      res.write.option("header", "true").save(outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_cpuntmed")

//      println("Cpuntmed prediction saved: "+outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_cpuntmed")

      centroidsCsv(m.stages(1).asInstanceOf[KMeansModel].clusterCenters,i)

      writeAnalysis(m.stages(1).asInstanceOf[KMeansModel],df,test,i)

      i = i+1

    }


  }


  private def kmeansGrid(fa:VectorAssembler,dataset:DataFrame, ks:Array[Int],maxIters:Array[Int],seeds:Array[Long],tols:Array[Double],steps:Array[Int]): mutable.MutableList[PipelineModel] = {

    val models = mutable.MutableList[PipelineModel]()

    var i = 0

    for (k <- ks){
      for (maxIt <- maxIters){
        for (seed <- seeds){
          for (tol <- tols){
            for (step <- steps){
              println("Generating model ["+i+"] (k="+k+", maxIter = "+maxIt+", seed = "+seed+" tol = "+tol+", initSteps = "+step+")")
              val kmeans = new KMeans().setK(k).setMaxIter(maxIt).setSeed(seed).setTol(tol).setInitSteps(step)
              val pipeline = new Pipeline().setStages(Array(fa, kmeans))
              val pipmod = pipeline.fit(dataset)
              models+=pipmod
              i = i+1
            }
          }
        }
      }
    }

    models

  }


  private def centroidsCsv(cn:Array[org.apache.spark.ml.linalg.Vector],i:Int):String = {


    var txt =""

    for (v <- cn){

      val ar = v.toArray

      txt+=ar.mkString(";")+"\n"

    }

    val bw = new BufferedWriter(new FileWriter(new File(outputRootPath+experimento+"_mod_"+i+"_centroids.csv")))
    bw.write(txt)
    bw.close()

    println("Centroids saved: "+outputRootPath+experimento+"_mod_"+i+"_centroids.csv")

    txt

  }



  private def writeAnalysis (model: KMeansModel, df:DataFrame, res:DataFrame,i:Int):Unit = {

    import sqlContext._
    res.createOrReplaceTempView("RES")
    df.createOrReplaceTempView("DF")
    val t = sql("""SELECT RES.cpuntmed, RES.prediction, label FROM RES JOIN DF WHERE RES.cpuntmed = DF.cpuntmed""")

    println("\n")

    var text = "cluster;ne_cluster;p_total;ne_et0;ne_et1;p_et0_cluster;p_et1_cluster;p_et0_total;p_et1_total;config\n"

    for (j <- 0 to model.getK-1){

      val otherSymbols = new DecimalFormatSymbols(java.util.Locale.getDefault)
      otherSymbols.setDecimalSeparator('.')
      otherSymbols.setGroupingSeparator(',')
      val fr = new DecimalFormat("#.###", otherSymbols)

      val ne_et0 = t.where("prediction = "+j+" AND label = 0").count().toDouble
      val ne_et1 = t.where("prediction = "+j+" AND label = 1").count().toDouble
      val ne_total = t.count().toDouble

      //****************************************
      val ne_cluster = ne_et0+ne_et1
      val p_total = (ne_cluster/ne_total)*100.0

      val p_et0_cluster = (ne_et1/ne_cluster)*100.0
      val p_et0_total = (ne_et1/ne_total)*100.0

      val p_et1_cluster = (ne_et0/ne_cluster)*100.0
      val p_et1_total = (ne_et0/ne_total)*100.0

      text += j+";"+fr.format(ne_cluster)+";"+fr.format(p_total)+
              ";"+fr.format(ne_et0)+";"+fr.format(ne_et1)+
              ";"+fr.format(p_et0_cluster)+";"+fr.format(p_et1_cluster)+
              ";"+fr.format(p_et0_total)+";"+fr.format(p_et1_total)+
              ";exp="+experimento+"#mod="+i+"#K="+model.getK+"#iter="+model.getMaxIter+"#seed="+model.getSeed+"#tol="+model.getTol+"#itit="+model.getInitSteps

      if (j!=model.getK-1){
        text+="\n"
      }

    }

    println(text)

    println("\n##################################################################################################################################################################\n")

    val bw = new BufferedWriter(new FileWriter(new File(outputRootPath+experimento+"_mod_"+i+"_study.csv")))
    bw.write(text)
    bw.close()

    println("Study saved: "+outputRootPath+experimento+"_mod_"+i+"_study.csv")

  }


}