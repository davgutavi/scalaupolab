package es.upo.datalab.datamining

import java.io.{BufferedWriter, File, FileWriter}
import java.text.DecimalFormat

import es.upo.datalab.datamining.EndesaKmeans.{outputRootPath, sqlContext}
import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GBTpendientes {

  //**ENTRADAS:

  //****Ruta del dataset de entrada
  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/dataset_t123_d_pendientes"

  //****Nombre global del experimento
  final val experimento = "experimento_01"

  //****Ruta raíz de los ficheros de salida
  final val outputRootPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/clasificacion/"+experimento+"/"



  final val sqlContext = SparkSessionUtils.sql
  final val sparkSession = SparkSessionUtils.session

  def main(args: Array[String]): Unit = {


    //**Carga de Dataframe

    println("Cargando Datos")

    val data = LoadTableParquet.loadTable(datasetPath)

    val Array(dataset, campo) = data.randomSplit(Array(0.7,0.3))

    //**Assembler
    val fdat= dataset.drop("cpuntmed","ccodpost", "cenae","label").columns

    val featureAssembler = new VectorAssembler().setInputCols(fdat).setOutputCol("features")

    //**Model
    val gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features")

     //**Pipeline
    val pipeline = new Pipeline().setStages(Array(featureAssembler, gbt))

    //**Training
    val Array(training, test) = dataset.randomSplit(Array(0.7,0.3))

    val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(10, 20)).addGrid(gbt.maxDepth, Array(1, 2)).build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    println("Entrenando")

    val cvModel = cv.fit(training)

    //**Evaluation

    println("Test")

    val rtest = cvModel.transform(test).select("cpuntmed","prediction","label","probability")

    val rl = mutable.MutableList[String]()


    val t1 = getResultsLine(rtest,"test")
    rl+= t1

    println(t1)

    println("Campo")

    val rcamp = cvModel.transform(campo).select("cpuntmed","prediction","label","probability")

    val t2 = getResultsLine(rcamp,"campo")
    rl+= t2

    println(t2)

    val text = getResultsString(rl)

    println("Resultados")

    println(text)

    val bw = new BufferedWriter(new FileWriter(new File(outputRootPath+"exp_"+experimento+"_study.csv")))
    bw.write(text)
    bw.close()


    SparkSessionUtils.session.stop()


  }



  private def getResultsString (lines:mutable.MutableList[String]): String = {

        var r = "tipo;aciertos_totales;fallos_totales;pfraudes_rfraudes;pfraudes_rlegales;plegales_rlegales;plegales_rfraudes\n"

        for (l <- lines){

            r+=l+"\n"

        }

        r

  }

  private def getResultsLine (results: DataFrame, tipo:String):String = {

    val fr = new DecimalFormat("#.##")

    val total = results.count().toDouble

    val n_f_f = results.where("prediction = 0 AND label = 0").count().toDouble
    val n_f_l = results.where("prediction = 0 AND label = 1").count().toDouble
    val n_l_f = results.where("prediction = 1 AND label = 0").count().toDouble
    val n_l_l = results.where("prediction = 1 AND label = 1").count().toDouble

    val aciertos = ((n_f_f+n_l_l)/total)*100.0
    val fallos   = ((n_f_l+n_l_f)/total)*100.0

    val f_f = (n_f_f/total)*100.0
    val f_l = (n_f_l/total)*100.0

    val l_l = (n_l_l/total)*100.0
    val l_f = (n_l_f/total)*100.0


    tipo+";"+fr.format(aciertos)+";"+fr.format(fallos)+";"+fr.format(f_f)+";"+fr.format(f_l)+";"+fr.format(l_l)+";"+fr.format(l_f)


  }





}
