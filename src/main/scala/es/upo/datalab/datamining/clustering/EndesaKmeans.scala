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

//  private final val sparkSession = SparkSessionUtils.sc
  private final val sqlContext = SparkSessionUtils.sql

  def clusteringExperiment(datasetPath:String, outputRootPath:String
                           , k:Array[Int], maxIter: Array[Int], inicializationSteps:Array[Int]
                           , tolerance:Array[Double], seed:Array[Long]): Unit = {




    //**Cargar datos fuente

    val sourceData = LoadTableParquet.loadTable(datasetPath)

    //**Obtener el dataset de entrada quitando los campos "ccodpost","cnae","label"

    val dataset = sourceData.drop("ccodpost","cnae","label")

    //**Assembler para construir los vectores input del k-means

    val inputCols = dataset.drop("cpuntmed").columns

    val fa = new VectorAssembler().setInputCols(inputCols).setOutputCol("features")

    //**GRID K-MEANS:

    val pipmodels = clusteringGrid(fa,dataset,k,maxIter,inicializationSteps,tolerance,seed)

    println("*********************************************************************************************************************\n")

    //**RESULTADOS

    var modelFileIndex = 1

    for (m <- pipmodels){

      println("Generating clustering "+modelFileIndex+":")

      //**Clustering

      val cluResults = m.transform(dataset)

      cluResults.write.option("header", "true").mode(SaveMode.Overwrite).save(outputRootPath+"/clu_"+modelFileIndex)

      println("Clustering saved: "+outputRootPath+"/clu_"+modelFileIndex)

      //**Centroides

      val cent= centroidsCsv(m.stages(1).asInstanceOf[KMeansModel].clusterCenters,modelFileIndex)

      val bw1 = new BufferedWriter(new FileWriter(new File(outputRootPath+"/cen_"+modelFileIndex+".csv")))
      bw1.write(cent)
      bw1.close()
      println("Centroids saved: "+outputRootPath+"/cen_"+modelFileIndex+".csv")

      //**Análisis

      val std = writeAnalysis(m.stages(1).asInstanceOf[KMeansModel],sourceData,cluResults,modelFileIndex)

      val bw2 = new BufferedWriter(new FileWriter(new File(outputRootPath+"/std_"+modelFileIndex+".csv")))
      bw2.write(std)
      bw2.close()

      println("Study saved: "+outputRootPath+"/std_"+modelFileIndex+".csv")


      modelFileIndex += 1

    }


  }


  private def clusteringGrid(fa:VectorAssembler,dataset:DataFrame,
                             ks:Array[Int],maxIters:Array[Int],steps:Array[Int],
                             tols:Array[Double],seeds:Array[Long]): mutable.MutableList[PipelineModel] = {

    val models = mutable.MutableList[PipelineModel]()

    var i = 1

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
              i += 1
            }
          }
        }
      }
    }

    models

  }


  private def centroidsCsv(centroids:Array[org.apache.spark.ml.linalg.Vector],modelIndex:Int):String = {


    var txt =""

    var j = 0

    for (v <- centroids){

      txt+=v.toArray.mkString(";")

      if (j!=centroids.length-1){
        txt+="\n"
      }

      j+=1

    }

    txt

  }



  private def writeAnalysis (model: KMeansModel, sourceData:DataFrame, clusteringResults:DataFrame,modelIndex:Int):String = {

    import sqlContext._
    clusteringResults.createOrReplaceTempView("RES")
    sourceData.createOrReplaceTempView("DF")
    val t = sql("""SELECT RES.cpuntmed, RES.prediction, label FROM RES JOIN DF WHERE RES.cpuntmed = DF.cpuntmed""")

    val otherSymbols = new DecimalFormatSymbols(java.util.Locale.getDefault)
    otherSymbols.setDecimalSeparator('.')
    otherSymbols.setGroupingSeparator(',')
    val fr = new DecimalFormat("#.###", otherSymbols)

    println("\n")

    var text = "cluster;ne_cluster;p_total;ne_et0;ne_et1;p_et0_cluster;p_et1_cluster;p_et0_total;p_et1_total;config\n"

    for (j <- 0 to model.getK-1){


      val ne_et0 = t.where("prediction = "+j+" AND label = 0").count().toDouble
      val ne_et1 = t.where("prediction = "+j+" AND label = 1").count().toDouble
      val ne_total = t.count().toDouble

      //****************************************
      val ne_cluster = ne_et0+ne_et1
      val p_total = (ne_cluster/ne_total)

      val p_et0_cluster = (ne_et0/ne_cluster)
      val p_et1_cluster = (ne_et1/ne_cluster)

      val p_et0_total = (ne_et0/ne_total)
      val p_et1_total = (ne_et1/ne_total)

      val clindex = j+1

      text += clindex+";"+fr.format(ne_cluster)+";"+fr.format(p_total)+
              ";"+fr.format(ne_et0)+";"+fr.format(ne_et1)+
              ";"+fr.format(p_et0_cluster)+";"+fr.format(p_et1_cluster)+
              ";"+fr.format(p_et0_total)+";"+fr.format(p_et1_total)+
              ";mod="+modelIndex+"#K="+model.getK+"#iter="+model.getMaxIter+"#seed="+model.getSeed+"#tol="+model.getTol+"#itit="+model.getInitSteps

//      text = text+clindex+";"+fr.format(ne_cluster)+";"+fr.format(p_total)+
//        ";"+fr.format(ne_et0)+";"+fr.format(ne_et1)+
//        ";"+fr.format(p_et0_cluster)+";"+fr.format(p_et1_cluster)+
//        ";"+fr.format(p_et0_total)+";"+fr.format(p_et1_total)+
//        ";exp="+experimento+"#mod="+modelIndex+"#K="+model.getK+"#iter="+model.getMaxIter+"#seed="+model.getSeed+"#tol="+model.getTol+"#itit="+model.getInitSteps


      if (j!=model.getK-1){
        text+="\n"
      }

    }

    println(text)

    println("\n######################################################################################\n")

    text



  }


}