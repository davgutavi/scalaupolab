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
  final val experimento = "experimento_04"

  //****Ruta raíz de los ficheros de salida
  final val outputRootPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/experimentos/"+experimento+"/"

  //****Parámetros del K-means
  final val k = Array(2,10)
  final val iteraciones = Array(50,100)
  final val semilla = Array(1L)
  final val tolerancia = Array(0.0001,0.001)
  final val pasosInicializacion = Array(2)

  //****Ruta del dataset de entrada
  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/dataset_t123_d_pendientes"


  //**SALIDAS:

  //  Fichero #outputRootPath#/#experimento#/exp_#experimento#_mod_#número de modelo#_pred_comp     => resultados de agrupamiento con todas las columnas
  //  Fichero #outputRootPath#/#experimento#/exp_#experimento#_mod_#número de modelo#_pred_cpuntmed => resultados de agrupamiento sólo con cpuntmed y prediction
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

      val res = test.select("cpuntmed", "prediction")

      test.write.option("header", "true").save(outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_comp")

      println("Complete prediction saved: "+outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_comp")

      res.write.option("header", "true").save(outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_cpuntmed")

      println("Cpuntmed prediction saved: "+outputRootPath+"exp_"+experimento+"_mod_"+i+"_pred_cpuntmed")

      centroidsCsv(m.stages(1).asInstanceOf[KMeansModel].clusterCenters,i)

      writeAnalysis(m.stages(1).asInstanceOf[KMeansModel],df,res,i)

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

    val bw = new BufferedWriter(new FileWriter(new File(outputRootPath+"exp_"+experimento+"_mod_"+i+"_centroids.csv")))
    bw.write(txt)
    bw.close()

    println("Centroids saved: "+outputRootPath+"exp_"+experimento+"_mod_"+i+"_centroids.csv")


    txt

  }



  private def writeAnalysis (model: KMeansModel, df:DataFrame, res:DataFrame,i:Int):Unit = {

    import sqlContext._
    res.createOrReplaceTempView("RES")
    df.createOrReplaceTempView("DF")
    val t = sql("""SELECT RES.cpuntmed, RES.prediction, label FROM RES JOIN DF WHERE RES.cpuntmed = DF.cpuntmed""")

    var text = "Experimento "+experimento+", modelo = "+i+": k = "+model.getK+", iteraciones = "+model.getMaxIter+", semilla = "+model.getSeed+", tolerancia = "+model.getTol+", pasos de iniciación = "+model.getInitSteps+"\n\n"

    for (j <- 0 to model.getK-1){

      val fr = new DecimalFormat("#.###")


      val total = t.count().toDouble

      val nf = t.where("prediction = "+j+" AND label = 0").count().toDouble
      val f  = t.where("prediction = "+j+" AND label = 1").count().toDouble
      val total_parcial = nf+f
      val pnf = (nf/total_parcial)*100.0
      val pf = (f/total_parcial)*100.0

      val ptp = (total_parcial/total)*100.0
      val ptnf = (nf/total)*100.0
      val ptf = (f/total)*100.0



      text+= "Cluster "+j+", "+fr.format(total_parcial)+" sobre "+fr.format(total)+" elementos totales, ("+fr.format(ptp)+" %)\n"
      text+= "Etiqueta 0 = "+fr.format(nf)+" elementos ("+fr.format(pnf)+" % del total del cluster) => "+fr.format(ptnf)+" del total del dataset\n"
      text+= "Etiqueta 1 = "+fr.format(f)+"("+fr.format(pf)+" % del total del cluster) => "+fr.format(ptf)+" del total del dataset\n"

    }

    text+="##################################################################################################################################################################\n"

    println(text)

    val bw = new BufferedWriter(new FileWriter(new File(outputRootPath+"exp_"+experimento+"_mod_"+i+"_study.txt")))
    bw.write(text)
    bw.close()

    println("Study saved: "+outputRootPath+"exp_"+experimento+"_mod_"+i+"_study.txt")

  }


}