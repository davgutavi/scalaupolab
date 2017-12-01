package es.upo.datalab.datamining.clustering

import java.io.{BufferedWriter, File, FileWriter}
import java.text.{DecimalFormat, DecimalFormatSymbols}

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable


object EndesaKmeans {

  //**ENTRADAS:

  //****Ruta raíz de los ficheros de salida
  final val outputRootPath = "/Users/davgutavi/Desktop/endesa/clustering/"+experimento+"/"

  //****Parámetros del K-means
  //default: {k: 2, maxIterations: 20, initializationSteps: 2, tolerance: 1e-4, seed: random}
  final val k = Array(2,8,10,20)
  final val iteraciones = Array(20,50)
  final val pasosInicializacion = Array(2)
  final val tolerancia = Array(0.0001)
  final val semilla = Array(1L)

  val sparkSession = SparkSessionUtils.sc
  val sqlContext = SparkSessionUtils.sql

  //****Nombre global del experimento
//     final val experimento = "little_454d_max_umr_kmeans"
//   final val experimento = "little_454d_max_con_kmeans"
//   final val experimento = "little_364d_max_umr_kmeans"
   final val experimento = "little_364d_max_con_kmeans"


  //****Ruta del dataset de entrada
//   final val datasetPath = "/Users/davgutavi/Desktop/endesa/datasets/454d_max_umr/454d_max_umr"
// final val datasetPath = "/Users/davgutavi/Desktop/endesa/datasets/454d_max_con/454d_max_con"
// final val datasetPath = "/Users/davgutavi/Desktop/endesa/datasets/364d_max_umr/364d_max_umr"
 final val datasetPath = "/Users/davgutavi/Desktop/endesa/datasets/364d_max_con/364d_max_con"




  def main(args: Array[String]): Unit = {

    //*******Cargar datos:

    val sourceData = LoadTableParquet.loadTable(datasetPath)

//    sourceData.show(5)

    val dataset = sourceData.drop("ccodpost","cnae","label")

    val inputCols = dataset.drop("cpuntmed").columns

    //*******Assembler:
    val fa = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")

    //*******GRID K-MEANS:

    val pipmodels = kmeansGrid(fa,dataset,k,iteraciones,semilla,tolerancia,pasosInicializacion)

    println("********************************************************************************************************************************************************\n")

    //*******RESULTADOS

    var modelIndex = 0

    for (m <- pipmodels){

      println("Generating clustering "+modelIndex+":")

      val cluResults = m.transform(dataset)

      cluResults.write.option("header", "true").mode(SaveMode.Overwrite).save(outputRootPath+experimento+"_m_"+modelIndex+"_clu")

      println("Clustering saved: "+outputRootPath+experimento+"_m_"+modelIndex+"_clu")

      centroidsCsv(m.stages(1).asInstanceOf[KMeansModel].clusterCenters,modelIndex)

      writeAnalysis(m.stages(1).asInstanceOf[KMeansModel],sourceData,cluResults,modelIndex)

      modelIndex = modelIndex+1

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

    val bw = new BufferedWriter(new FileWriter(new File(outputRootPath+experimento+"_m_"+i+"_cen.csv")))
    bw.write(txt)
    bw.close()

    println("Centroids saved: "+outputRootPath+experimento+"_m_"+i+"_cen.csv")

    txt

  }



  private def writeAnalysis (model: KMeansModel, sourceData:DataFrame, clusteringResults:DataFrame,modelIndex:Int):Unit = {

    import sqlContext._
    clusteringResults.createOrReplaceTempView("RES")
    sourceData.createOrReplaceTempView("DF")
    val t = sql("""SELECT RES.cpuntmed, RES.prediction, label FROM RES JOIN DF WHERE RES.cpuntmed = DF.cpuntmed""")

    println("\n")

    var text = "cluster;ne_cluster;p_total;ne_et0;ne_et1;p_et0_cluster;p_et1_cluster;p_et0_total;p_et1_total;config\n"

    for (j <- 0 to model.getK-1){

      val otherSymbols = new DecimalFormatSymbols(java.util.Locale.getDefault)
      otherSymbols.setDecimalSeparator('.')
      otherSymbols.setGroupingSeparator(',')
      val fr = new DecimalFormat("#.##", otherSymbols)

      val ne_et0 = t.where("prediction = "+j+" AND label = 0").count().toDouble
      val ne_et1 = t.where("prediction = "+j+" AND label = 1").count().toDouble
      val ne_total = t.count().toDouble

      //****************************************
      val ne_cluster = ne_et0+ne_et1
      val p_total = (ne_cluster/ne_total)

      val p_et0_cluster = (ne_et1/ne_cluster)
      val p_et0_total = (ne_et1/ne_total)

      val p_et1_cluster = (ne_et0/ne_cluster)
      val p_et1_total = (ne_et0/ne_total)

      text += j+";"+fr.format(ne_cluster)+";"+fr.format(p_total)+
              ";"+fr.format(ne_et0)+";"+fr.format(ne_et1)+
              ";"+fr.format(p_et0_cluster)+";"+fr.format(p_et1_cluster)+
              ";"+fr.format(p_et0_total)+";"+fr.format(p_et1_total)+
              ";exp="+experimento+"#mod="+modelIndex+"#K="+model.getK+"#iter="+model.getMaxIter+"#seed="+model.getSeed+"#tol="+model.getTol+"#itit="+model.getInitSteps

      if (j!=model.getK-1){
        text+="\n"
      }

    }

    println(text)

    println("\n##################################################################################################################################################################\n")

    val bw = new BufferedWriter(new FileWriter(new File(outputRootPath+experimento+"_m_"+modelIndex+"_std.csv")))
    bw.write(text)
    bw.close()

    println("Study saved: "+outputRootPath+experimento+"_m_"+modelIndex+"_std.csv")

  }


}