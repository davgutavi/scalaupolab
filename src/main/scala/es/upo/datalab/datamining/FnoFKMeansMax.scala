package es.upo.datalab.datamining

import es.upo.datalab.utilities.{LoadTableParquet, SparkSessionUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler


object FnoFKMeansMax {

  final val experimento = "_04"
  final val k = 2
  final val iteraciones = 5000
  final val semilla = 1L
  final val rootPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/"

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSessionUtils.sc
    val sqlContext = SparkSessionUtils.sql
    import sqlContext._

    println("Cargando dataframe...")

//  val df01 = LoadTableParquet.loadTable("hdfs://10.141.0.224:9000/fabregas/endesa/datasets_parquet/apendientes_01")
    val df = LoadTableParquet.loadTable(rootPath+"dataset_t123_d_pendientes")

    val data = df.drop("label", "cenae", "ccodpost","nle")

//    println("Generando vector de características....")

    val fdat = data.drop("cpuntmed").columns

    val featureAssembler = new VectorAssembler().setInputCols(fdat).setOutputCol("features")

//    println("Generando cluster....")
    val kmeans = new KMeans().setK(k).setSeed(semilla).setMaxIter(iteraciones)

    println("Generando pipeline...")
    val pipeline = new Pipeline().setStages(Array(featureAssembler, kmeans))

    val model = pipeline.fit(data)

    val predictionResult = model.transform(data)

    val centers = model.stages(1).asInstanceOf[KMeansModel].clusterCenters

    sparkSession.parallelize(centers).coalesce(1).saveAsTextFile(rootPath+"centrioides"+experimento)

    val res = predictionResult.select("cpuntmed", "prediction")

    predictionResult.write.option("header", "true").save(rootPath+"modelo_"+experimento)

    res.write.option("header", "true").save(rootPath+"resultados_"+experimento)

//
//    println("cluster 0 =" + res.where("prediction = 0").count())
//    println("cluster 1 =" + res.where("prediction = 1").count())
//    println("cluster 2 =" + res.where("prediction = 2").count())
//    println("cluster 3 =" + res.where("prediction = 3").count())
//    println("cluster 4 =" + res.where("prediction = 4").count())
//    println("cluster 5 =" + res.where("prediction = 5").count())
//    println("cluster 6 =" + res.where("prediction = 6").count())
//    println("cluster 7 =" + res.where("prediction = 7").count())

    res.createOrReplaceTempView("RES")
    df.createOrReplaceTempView("DF")
    val t = sql("""SELECT RES.cpuntmed, RES.prediction, label FROM RES JOIN DF WHERE RES.cpuntmed = DF.cpuntmed""")

    println("Cluster 0, "+res.where("prediction = 0").count()+" elementos:" )
    println ("Etiqueta 0 = "+t.where("prediction = 0 AND label = 0").count())
    println ("Etiqueta 1 = "+t.where("prediction = 0 AND label = 1").count())

    println("Cluster 1, "+res.where("prediction = 1").count()+" elementos:" )
    println ("Etiqueta 0 = "+t.where("prediction = 1 AND label = 0").count())
    println ("Etiqueta 1 = "+t.where("prediction = 1 AND label = 1").count())

  }
}