package es.upo.datalab.datamining

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel


object FnoFdiarios {

  def main(args: Array[String]): Unit = {

    val nivel = StorageLevel.MEMORY_AND_DISK

    val sqlContext = SparkSessionUtils.sql

    val sparkSession = SparkSessionUtils.session


    //**Carga de Dataframe

    println("Cargando DataFrames")

    val data = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/ds_123_diario")

//   val trainingData = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/train_123_diario")
//
//   val testData = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/test_123_diario")


    val dataAux = data.drop("fraude")

    val featureCols: Array[String] = dataAux.columns


//    val a2 = featureCols.filter(_!="cpuntmed").filter(_!="ccodpost").filter(_!="cenae")

    //tranformor to convert string to category values
//    val cpuntmedIndexer = new StringIndexer().setInputCol("cpuntmed").setOutputCol("cpuntmedCat")
    val ccodpostIndexer = new StringIndexer().setInputCol("ccodpost").setOutputCol("ccodpostCat")
    val cnaeIndexer = new StringIndexer().setInputCol("cenae").setOutputCol("cenaeCat")

//    val e1:StringIndexerModel = cpuntmedIndexer.fit(data)
    val e2:StringIndexerModel = ccodpostIndexer.fit(data)
    val e3:StringIndexerModel = cnaeIndexer.fit(data)

//    val d1 = e1.transform(data)
    val d2 = e2.transform(data)
    val d3 = e3.transform(d2)



    val fdat= d3.drop("cpuntmed","ccodpost", "cenae","fraude")

    val featureAssembler = new VectorAssembler().setInputCols(fdat.columns).setOutputCol("rawFeatures")

    val dataWithFeatures:DataFrame = featureAssembler.transform(d3)


    val featureIndexer = new VectorIndexer()
      .setInputCol("rawFeatures")
      .setOutputCol("indexedFeatures")
//      .setMaxCategories(1)
      .fit(dataWithFeatures)


    val d4 = featureIndexer.transform(dataWithFeatures)

    d4.show(10)

    val Array (training, test) = d4.randomSplit(Array(0.7, 0.3))




    println("Entrenando modelo")
        // Train a GBT model.
        val gbt = new GBTClassifier()
          .setLabelCol("fraude")
          .setFeaturesCol("indexedFeatures")
//          .setMaxBins(3629)
          .setMaxIter(10)
          .fit(training)



    val predictions = gbt.transform(test)

    predictions.select("prediction", "fraude", "indexedFeatures").show(5)


//    // Train a GBT model.
//    val gbt = new GBTClassifier()
//      .setLabelCol("fraude")
//      .setFeaturesCol("features")
//      .setMaxIter(10)
//      .fit(data)
//
//
//
////    // Convert indexed labels back to original labels.
////    val labelConverter = new IndexToString()
////      .setInputCol("prediction")
////      .setOutputCol("predictedLabel")
////      .setLabels(labelIndexer.labels)
//
//
//    // Chain indexers and GBT in a Pipeline
//    val pipeline = new Pipeline()
//      .setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))
//
//
//    // Train model.  This also runs the indexers.
//    val model = pipeline.fit(trainingData)
//
//    // Make predictions.
//    val predictions = model.transform(testData)
//
//    // Select example rows to display.
//    predictions.select("predictedLabel", "label", "features").show(5)
//
//    // Select (prediction, true label) and compute test error
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("indexedLabel")
//      .setPredictionCol("prediction")
//      .setMetricName("precision")
//
//    val accuracy = evaluator.evaluate(predictions)
//    println("Test Error = " + (1.0 - accuracy))
//
//
//    val gbtModel = model.stages(2).asInstanceOf[GBTClassificationModel]
//
//    println("Learned classification GBT model:\n" + gbtModel.toDebugString)


    SparkSessionUtils.session.stop()


  }




}
