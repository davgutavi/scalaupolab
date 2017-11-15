package es.upo.datalab.datamining

import java.io.{BufferedWriter, File, FileWriter}
import java.text.{DecimalFormat, DecimalFormatSymbols}

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.math._
import scala.collection.mutable

object GBTpendientes {

  //*********************************************************ENTRADAS:

  //****Parametrización del algoritmo (cross validation)

  // PEQUEÑO
//  val folds = 5
//  //imputity: gini (d), entropy
//  val imputity = Array("entropy")
//  //loss type: logistic (d)
//  val lossType = Array("logistic")
//  //max bins: >=2, 32(d)
//  val maxBins = Array(32)
//  //max depth: >=0, 5(d)
//  val maxDepth = Array(20)
//  //max iter: >=0
//  val maxIter = Array(100)
//  //min info gain: >=0.0, 0.0 (d)
//  val minInfoGain = Array(0.2)
//  //min instaces per node: >=1, 1 (d)
//  val minInstancesPerNode = Array(8)
//  //seed
//  val seed = Array(1L)
//  //step size (learning rate): (0,1] 0.1 (d)
//  val stepSize = Array(0.2)
//  //subsampling rate: (0,1] 1.0 (d)
//  val subsamplingRate = Array(0.1)

  // MEDIANO
//  val folds = 5
//    //imputity: gini (d), entropy
//    val imputity = Array("gini","entropy")
//    //loss type: logistic (d)
//    val lossType = Array("logistic")
//    //max bins: >=2, 32(d)
//    val maxBins = Array(32)
//    //max depth: >=0, 5(d)
//    val maxDepth = Array(10,30)
//    //max iter: >=0
//    val maxIter = Array(50,100)
//    //min info gain: >=0.0, 0.0 (d)
//    val minInfoGain = Array(0.0,0.1)
//    //min instaces per node: >=1, 1 (d)
//    val minInstancesPerNode = Array(1,2)
//    //seed
//    val seed = Array(1L)
//    //step size (learning rate): (0,1] 0.1 (d)
//    val stepSize = Array(0.1,0.2)
//    //subsampling rate: (0,1] 1.0 (d)
//    val subsamplingRate = Array(0.1)


  // GORDO
  //folds
  val folds = 5
  //imputity: gini (d), entropy
  val imputity = Array("gini","entropy")
  //loss type: logistic (d)
  val lossType = Array("logistic")
  //max bins: >=2, 32(d)
  val maxBins = Array(32)
  //max depth: >=0, 5(d)
  val maxDepth = Array(5,10,20,30)
  //max iter: >=0
  val maxIter = Array(20,50,100)
  //min info gain: >=0.0, 0.0 (d)
  val minInfoGain = Array(0.0,0.1,0.2)
  //min instaces per node: >=1, 1 (d)
  val minInstancesPerNode = Array(1,2)
  //seed
  val seed = Array(1L)
  //step size (learning rate): (0,1] 0.1 (d)
  val stepSize = Array(0.1,0.2,0.3)
  //subsampling rate: (0,1] 1.0 (d)
  val subsamplingRate = Array(0.1,0.2,0.5,1.0)

//  //DEFAULT
  //folds
//  val folds = 2
//  //imputity: gini (d), entropy
//  val imputity = Array("gini")
//  //loss type: logistic (d)
//  val lossType = Array("logistic")
//  //max bins: >=2, 32(d)
//  val maxBins = Array(32)
//  //max depth: >=0, 5(d)
//  val maxDepth = Array(5)
//  //max iter: >=0
//  val maxIter = Array(10)
//  //min info gain: >=0.0, 0.0 (d)
//  val minInfoGain = Array(0.0)
//  //min instaces per node: >=1, 1 (d)
//  val minInstancesPerNode = Array(1)
//  //seed
//  val seed = Array(1L)
//  //step size (learning rate): (0,1] 0.1 (d)
//  val stepSize = Array(0.1)
//  //subsampling rate: (0,1] 1.0 (d)
//  val subsamplingRate = Array(1.0)

  //****Ruta del dataset de entrada
  //  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_454d"
  //  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_364d"
  //  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_454d_pendientes"
//  final val datasetPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/datasets/t123_364d_pendientes"
  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_364d_pendientes"

  //****Nombre global del experimento
  final val experimento = "364d_pen_cluster_20"

  //****Ruta raíz de los ficheros de salida
//  final val outputRootPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/clasificacion/"+experimento+"/"
  final val outputRootPath = "hdfs://192.168.47.247/user/gutierrez/resultados_endesa/clasificacion/"+experimento+"/"

  //*********************************************************SALIDAS:

  // outputRootPath+experimento+"_study.csv" : csv con matriz de confusión y medidas de validación del modelo
  // outputRootPath+experimento+"_rtest"     : resultados del test
  // outputRootPath+experimento+"_rcamp"     : resultados prueba de campo
  // outputRootPath+experimento+"_model"     : modelo generado

  final val sqlContext = SparkSessionUtils.sql
  final val sparkSession = SparkSessionUtils.session

  def main(args: Array[String]): Unit = {

    //******************************************************
    //**Carga de datos**************************************
    //******************************************************

    println("Cargando Datos")

    val data = LoadTableParquet.loadTable(datasetPath)

    val Array(dataset, campo) = data.randomSplit(Array(0.7,0.3))


    //******************************************************
    //**Configuración Cross Validation**********************
    //******************************************************

    //**Assembler
    val fdat= dataset.drop("cpuntmed","ccodpost", "cenae","label").columns

    val featureAssembler = new VectorAssembler().setInputCols(fdat).setOutputCol("features")

    //**Model
    val gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features")

     //**Pipeline
    val pipeline = new Pipeline().setStages(Array(featureAssembler, gbt))

    //**Training
    val Array(training, test) = dataset.randomSplit(Array(0.7,0.3))

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.impurity, imputity)
      .addGrid(gbt.lossType, lossType)
      .addGrid(gbt.maxBins, maxBins)
      .addGrid(gbt.maxDepth, maxDepth)
      .addGrid(gbt.maxIter, maxIter)
      .addGrid(gbt.minInfoGain, minInfoGain)
      .addGrid(gbt.minInstancesPerNode, minInstancesPerNode)
      .addGrid(gbt.seed, seed)
      .addGrid(gbt.stepSize, stepSize)
      .addGrid(gbt.subsamplingRate, subsamplingRate)
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(folds)

    //******************************************************
    //**Entrenamiento***************************************
    //******************************************************

    println("Entrenando")

    val cvModel = cv.fit(training)

    //******************************************************
    //**Test************************************************
    //******************************************************

    println("Test")

    val rtest = cvModel.transform(test)

    //******************************************************
    //**Campo***********************************************
    //******************************************************

    println("Campo")

    val rcamp = cvModel.transform(campo)

    //******************************************************
    //**Resultados******************************************
    //******************************************************

    println("Resultados")

    val gbtm:GBTClassificationModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(1).asInstanceOf[GBTClassificationModel]

    //**Test******************************************

    rtest.write.option("header", "true").mode(SaveMode.Overwrite).save(outputRootPath+experimento+"_rtest")

    println("Test saved: "+outputRootPath+experimento+"_rtest")

    //**Campo******************************************

    rcamp.write.option("header", "true").mode(SaveMode.Overwrite).save(outputRootPath+experimento+"_rcamp")

    println("Field saved: "+outputRootPath+experimento+"_rcamp")

    //**Modelo******************************************

    gbtm.write.overwrite().save(outputRootPath+experimento+"_model")

    println("Model saved: "+outputRootPath+experimento+"_model")

    //**Estudio*****************************************

    val study:DataFrame = GBTexperiment.getStudyDataframe(rtest,rcamp,cvModel)

    study.show(5)

    study.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).option("delimiter",";").csv(outputRootPath+experimento+"_study")

    println("Study saved: "+outputRootPath+experimento+"_study")

    //**Cerrar sesión de Spark**************************

    SparkSessionUtils.session.stop()


  }



//  private def getResultsString (lines:mutable.MutableList[String]): String = {
//
//        var r = "tipo;tn;fn;fp;tp;tpr;tnr;ppv;npv;fnr;fpr;fdr;for;acc;f1;mcc;bmmk;param\n"
//
//        for (l <- lines){
//
//            r+=l+"\n"
//
//        }
//
//        r
//
//  }
//
//  private def getResultsLine (results: DataFrame, tipo:String, model:GBTClassificationModel):String = {
//
//    //folds
//    val folds = 2
//
//    val config = "impurity="+model.getImpurity+"#lossType="+model.getLossType+"#maxBins="+model.getMaxBins+"#maxDepth="+model.getMaxDepth+"#maxIter="+model.getMaxIter+
//                 "#minInfoGain="+model.getMinInfoGain+"#minInstancesPerNode="+model.getMinInstancesPerNode+"#minInstancesPerNode="+model.getMinInstancesPerNode+
//                  "#seed="+model.getSeed+"#stepSize="+model.getStepSize+"#subsamplingRate="+model.getSubsamplingRate
//
//    val otherSymbols = new DecimalFormatSymbols(java.util.Locale.getDefault)
//    otherSymbols.setDecimalSeparator('.')
//    otherSymbols.setGroupingSeparator(',')
//    val fr = new DecimalFormat("#.###", otherSymbols)
//
//    val tn   = results.where("prediction = 0 AND label = 0").count().toDouble
//    val fn   = results.where("prediction = 0 AND label = 1").count().toDouble
//    val fp   = results.where("prediction = 1 AND label = 0").count().toDouble
//    val tp   = results.where("prediction = 1 AND label = 1").count().toDouble
//
//    val tpr  = tp / (tp+fn)
//    val tnr  = tn / (tn+fp)
//    val ppv  = tp / (tp+fp)
//    val npv  = tn / (tn+fn)
//
//    val fnr  = 1 - tpr
//    val fpr  = 1 - tnr
//    val fdr  = 1 - ppv
//    val forr = 1 - npv
//
//    val acc  = (tp+tn) / (tp+tn+fp+fn)
//
//    val f1   = 2 * ((ppv*tpr)/(ppv+tpr))
//    val mcc  = ((tp*tn)-(fp*fn))/ sqrt((tp+fp)*(tp+fn)*(tn+fp)*(tn+fn))
//    val bm   = tpr + tnr -1
//    val mk   = ppv + npv -1
//
//
//
//    tipo+";"+fr.format(tn)+";"+fr.format(fn)+";"+fr.format(fp)+";"+fr.format(tp)+";"+fr.format(tpr)+";"+fr.format(tnr)+";"+
//      fr.format(ppv)+";"+fr.format(npv)+fr.format(fnr)+";"+fr.format(fpr)+";"+fr.format(fdr)+";"+fr.format(forr)+";"+fr.format(acc)+
//      ";"+fr.format(f1)+";"+fr.format(mcc)+";"+fr.format(bm)+";"+fr.format(mk)+";"+config
//
//
//  }

}