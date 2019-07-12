package es.upo.datalab.datamining.xgb

import com.codahale.metrics.Sampling
import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SaveMode}

object EndesaGBT {

  final val sqlContext = SparkSessionUtils.sql
  final val sparkSession = SparkSessionUtils.session

  def gbtExperiment(inputPath:String, splitgMode:String, outputPath:String, folds:Int, impurity:Array[String], lossType:Array[String]
                    , maxBins:Array[Int], maxDepth:Array[Int], maxIter:Array[Int], minInfoGain:Array[Double]
                    , minInstancesPerNode:Array[Int], seed:Array[Long], stepSize:Array[Double], subsamplingRate:Array[Double]): Unit = {


    //******************************************************
    //**Carga de datos**************************************
    //******************************************************

    println("Loading data")

    val data = LoadTableParquet.loadTable(inputPath)

    val Array(dataset, campo) = data.randomSplit(Array(0.7,0.3))

    //******************************************************
    //**Configuración Cross Validation**********************
    //******************************************************

    //**Assembler
    val fdat= dataset.drop("cpuntmed","label").columns

    val featureAssembler = new VectorAssembler().setInputCols(fdat).setOutputCol("features")

    //**Model
    val gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("features")

     //**Pipeline
    val pipeline = new Pipeline().setStages(Array(featureAssembler, gbt))

    //**Training
    val Array(training, test) = dataset.randomSplit(Array(0.7,0.3))

    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.impurity, impurity)
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


    training.show(5)
//    val tr = training.drop("cpuntmed","ccodpost", "cenae")

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

    rtest.write.option("header", "true").mode(SaveMode.Overwrite).save(outputPath+"_rtest")

    println("Test saved: "+outputPath+"_rtest")

    //**Campo******************************************

    rcamp.write.option("header", "true").mode(SaveMode.Overwrite).save(outputPath+"_rcamp")

    println("Field saved: "+outputPath+"_rcamp")

    //**Modelo******************************************

    gbtm.write.overwrite().save(outputPath+"_model")

    println("Model saved: "+outputPath+"_model")

    //**Estudio*****************************************

    val study:DataFrame = GBTstudy.getStudyDataframe(rtest,rcamp,cvModel)

    study.show(5)

    study.coalesce(1).write.option("header","true").mode(SaveMode.Overwrite).option("delimiter",";").csv(outputPath+"_study")

    println("Study saved: "+outputPath+"_study")

    //**Cerrar sesión de Spark**************************

    SparkSessionUtils.session.stop()

  }

}