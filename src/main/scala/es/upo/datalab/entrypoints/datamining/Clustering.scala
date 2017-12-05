package es.upo.datalab.entrypoints.datamining

import es.upo.datalab.datamining.clustering.EndesaKmeans
import es.upo.datalab.datasets.DatasetPaths
import es.upo.datalab.utilities.TimingUtils

object Clustering  {


  private final val k = Array(2,8,10,20)
  private final val maxIter = Array(20,50)
  private final val initializationSteps = Array(2)
  private final val tolerance = Array(0.0001)
  private final val seed = Array(1L)

  def main( args:Array[String] ):Unit = {

    TimingUtils.time {

      val output1 = "/Users/davgutavi/Desktop/endesa/clustering/lc_454d_raw_umr"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_raw_umr_macDavid, output1, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 1 done")

      val output2 = "/Users/davgutavi/Desktop/endesa/clustering/lc_454d_raw_con"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_raw_con_macDavid, output2, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 2 done")

      val output3 = "/Users/davgutavi/Desktop/endesa/clustering/lc_454d_max_umr"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_max_umr_macDavid, output3, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 3 done")

      val output4 = "/Users/davgutavi/Desktop/endesa/clustering/lc_454d_max_con"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_max_con_macDavid, output4, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 4 done")


      val output5 = "/Users/davgutavi/Desktop/endesa/clustering/lc_454d_slo_umr"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_slo_umr_macDavid, output5, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 5 done")

      val output6 = "/Users/davgutavi/Desktop/endesa/clustering/lc_454d_slo_con"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_slo_con_macDavid, output6, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 6 done")


      val output7 = "/Users/davgutavi/Desktop/endesa/clustering/lc_364d_raw_umr"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_raw_umr_macDavid, output7, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 7 done")

      val output8 = "/Users/davgutavi/Desktop/endesa/clustering/lc_364d_raw_con"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_raw_con_macDavid, output8, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 8 done")

      val output9 = "/Users/davgutavi/Desktop/endesa/clustering/lc_364d_max_umr"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_max_umr_macDavid, output9, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 9 done")

      val output10 = "/Users/davgutavi/Desktop/endesa/clustering/lc_364d_max_con"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_max_con_macDavid, output10, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 10 done")


      val output11 = "/Users/davgutavi/Desktop/endesa/clustering/lc_364d_slo_umr"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_slo_umr_macDavid, output11, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 11 done")

      val output12 = "/Users/davgutavi/Desktop/endesa/clustering/lc_364d_slo_con"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_slo_con_macDavid, output12, k, maxIter, initializationSteps, tolerance, seed)

      println("Experiment 12 done")

    }

  }

}