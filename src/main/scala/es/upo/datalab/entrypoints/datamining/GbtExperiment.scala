package es.upo.datalab.entrypoints.datamining

import es.upo.datalab.datamining.xgb.EndesaGBT
import es.upo.datalab.utilities.TimingUtils


object GbtExperiment {

  //*********************************************************ENTRADAS:

  //****Parametrización del algoritmo (cross validation)


  // SMALL
  //     final val folds = 5
  //    //imputity: gini (d), entropy
  //    final val imputity = Array("gini","entropy")
  //    //loss type: logistic (d)
  //    final val lossType = Array("logistic")
  //    //max bins: >=2, 32(d)
  //    final val maxBins = Array(32)
  //    //max depth: >=0, 5(d)
  //    final val maxDepth = Array(10,30)
  //    //max iter: >=0
  //    final val maxIter = Array(50,100)
  //    //min info gain: >=0.0, 0.0 (d)
  //    final val minInfoGain = Array(0.0,0.1)
  //    //min instaces per node: >=1, 1 (d)
  //    final val minInstancesPerNode = Array(1,2)
  //    //seed
  //    final val seed = Array(1L)
  //    //step size (learning rate): (0,1] 0.1 (d)
  //    final val stepSize = Array(0.1,0.2)
  //    //subsampling rate: (0,1] 1.0 (d)
  //    final val subsamplingRate = Array(0.1)


  // LARGE
//  //folds
//  final val folds = 5
//  //imputity: gini (d), entropy
//  final val imputity = Array("gini", "entropy")
//  //loss type: logistic (d)
//  final val lossType = Array("logistic")
//  //max bins: >=2, 32(d)
//  final val maxBins = Array(32)
//  //max depth: >=0, 5(d)
//  final val maxDepth = Array(5, 10, 20, 30)
//  //max iter: >=0
//  final val maxIter = Array(20, 50, 100)
//  //min info gain: >=0.0, 0.0 (d)
//  final val minInfoGain = Array(0.0, 0.1, 0.2)
//  //min instaces per node: >=1, 1 (d)
//  final val minInstancesPerNode = Array(1, 2)
//  //seed
//  final val seed = Array(1L)
//  //step size (learning rate): (0,1] 0.1 (d)
//  final val stepSize = Array(0.1, 0.2, 0.3)
//  //subsampling rate: (0,1] 1.0 (d)
//  final val subsamplingRate = Array(0.1, 0.2, 0.5, 1.0)

  //  //DEFAULT
  //folds
  //  final val folds = 2
  //  //imputity: gini (d), entropy
  //  final val imputity = Array("gini")
  //  //loss type: logistic (d)
  //  final val lossType = Array("logistic")
  //  //max bins: >=2, 32(d)
  //  final val maxBins = Array(32)
  //  //max depth: >=0, 5(d)
  //  final val maxDepth = Array(5)
  //  //max iter: >=0
  //  final val maxIter = Array(10)
  //  //min info gain: >=0.0, 0.0 (d)
  //  final val minInfoGain = Array(0.0)
  //  //min instaces per node: >=1, 1 (d)
  //  final val minInstancesPerNode = Array(1)
  //  //seed
  //  final val seed = Array(1L)
  //  //step size (learning rate): (0,1] 0.1 (d)
  //  final val stepSize = Array(0.1)
  //  //subsampling rate: (0,1] 1.0 (d)
  //  final val subsamplingRate = Array(1.0)



// Paper HAIS

       final val folds = 5
      //imputity: gini (d), entropy
      final val imputity = Array("gini")
      //loss type: logistic (d)
      final val lossType = Array("logistic")
      //max bins: >=2, 32(d)
      final val maxBins = Array(32)
      //max depth: >=0, 5(d)
      final val maxDepth = Array(5,10)
      //max iter: >=0
      final val maxIter = Array(5,10)
      //min info gain: >=0.0, 0.0 (d)
      final val minInfoGain = Array(0.0)
      //min instaces per node: >=1, 1 (d)
      final val minInstancesPerNode = Array(1)
      //seed
      final val seed = Array(1L)
      //step size (learning rate): (0,1] 0.1 (d)
      final val stepSize = Array(0.1)
      //subsampling rate: (0,1] 1.0 (d)
      final val subsamplingRate = Array(0.1)



  //****Ruta del dataset de entrada

  //    final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_454d_con_slo"
  //    final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_454d_nrl_slo"
  //    final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_364d_con_slo"
  //    final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_364d_nrl_slo"

  final val datasetPath = "/Users/davgutavi/NAS_PROYECTOS/smartfd/investigacion/datasets/454d_raw_con/454d_raw_con"

  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_364d_all"
  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_364d_con"
  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_364d_nrl"
  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_364d_all_slo"
  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_454d_all"
  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_454d_con"
  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_454d_nrl"
  //  final val datasetPath = "hdfs://192.168.47.247/user/datos/endesa/datasets/t123_454d_all_slo"

  //****Nombre global del experimento
  //    final val experimento = "s_454d_con_slo"
  //    final val experimento = "s_454d_nrl_slo"
  //    final val experimento = "s_364d_con_slo"
  //    final val experimento = "s_364d_nrl_slo"

  //    final val experimento = "l_454d_con_slo"
  //    final val experimento = "l_454d_nrl_slo"
  final val experimento = "d455_sinNle"
  //    final val experimento = "l_364d_nrl_slo"


  //****Ruta raíz de los ficheros de salida
  //  final val outputRootPath = "/Users/davgutavi/Desktop/modelos_variables_endesa/clasificacion/"+experimento+"/"
  final val outputRootPath = "/Users/davgutavi/Desktop/hais_experiments/" + experimento + "/"

  //*********************************************************SALIDAS:

  // outputRootPath+experimento+"_study.csv" : csv con matriz de confusión y medidas de validación del modelo
  // outputRootPath+experimento+"_rtest"     : resultados del test
  // outputRootPath+experimento+"_rcamp"     : resultados prueba de campo
  // outputRootPath+experimento+"_model"     : modelo generado

  def main(args: Array[String]): Unit = {

    TimingUtils.time {

      println("Starting experiment")

      EndesaGBT.gbtExperiment(datasetPath,"",outputRootPath,folds,imputity,lossType,maxBins,maxDepth,maxIter,minInfoGain,minInstancesPerNode,seed,stepSize,subsamplingRate)

      println("Done!")
    }

  }

}
