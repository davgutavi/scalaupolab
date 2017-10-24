package es.upo.datalab.datamining

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel


object FnoFdiarios {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql

    val sparkSession = SparkSessionUtils.session


    //**Carga de Dataframe

    println("Cargando DataFrames")

    val dataaux = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/ds_123_diario")

    val data = dataaux.withColumnRenamed("fraude", "label")

    val trainingData = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/train_123_diario")

    val testData = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/test_123_diario")


    //INDEXERS

    val ccodpostIndexer = new StringIndexer().setInputCol("ccodpost").setOutputCol("ccodpostCat")

    val cnaeIndexer = new StringIndexer().setInputCol("cenae").setOutputCol("cenaeCat")


    //ASSEMBLER
    val fdat= data.drop("cpuntmed","ccodpost", "cenae","label").columns

    val featureAssembler = new VectorAssembler().setInputCols(fdat).setOutputCol("rawFeatures")


    //FEATURE INDEXER
    //val featureIndexer = new VectorIndexer().setInputCol("rawFeatures").setOutputCol("indexedFeatures").setMaxCategories(1)

    //MODEL
     val gbt = new GBTClassifier().setLabelCol("label").setFeaturesCol("rawFeatures").setMaxIter(10)

//     val gbt = new GBTClassifier().setLabelCol("fraude").setFeaturesCol("rawFeatures").setMaxIter(10)


    //CONVERTERS
    val ccodpostConverter = new IndexToString().setInputCol("ccodpostCat").setOutputCol("ccodpost_conv")

    val cnaeConverter = new IndexToString().setInputCol("cenaeCat").setOutputCol("cenae_conv")

    //PIPELINE
    val pipeline = new Pipeline().setStages(Array(ccodpostIndexer, cnaeIndexer, featureAssembler, gbt, ccodpostConverter, cnaeConverter))


    //TRAINING Y TEST

    val Array(training, test) = data.randomSplit(Array(0.7,0.3))

//    //ENTRENAMIENTO
//
//    val model = pipeline.fit(training)
//
//    //TEST
//
//    val predictions = model.transform(test)
//
//    //PREDICCIONES
//
//    predictions.select("prediction", "fraude", "rawFeatures").show(20)


    //EVALUATION


    val paramGrid = new ParamGridBuilder().addGrid(gbt.maxIter, Array(1, 2)).addGrid(gbt.maxDepth, Array(1, 2)).build()



    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(4)  // Use 3+ in practice

    val cvModel = cv.fit(training)


//    cvModel.transform(test).select("prediction","label","probability","rawFeatures").show(40)

    cvModel.transform(test).select("prediction","label","probability").show(40)


    cvModel.write.overwrite().save("/Users/davgutavi/Desktop/model")

    //GUARDAR MODELO




    SparkSessionUtils.session.stop()


  }




}
