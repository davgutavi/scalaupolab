package es.upo.datalab.entrypoints.datamining

import es.upo.datalab.datamining.clustering.EndesaKmeans
import es.upo.datalab.datasets.DatasetPaths
import es.upo.datalab.utilities.TimingUtils

object ClusteringList  {


//  private final val rootOutput = "/mnt/datos/davgutavi/CloudStation/endesa/investigacion_paper/clustering/"
  private final val rootOutput = "/Users/davgutavi/CloudStation/endesa/investigacion_paper/clustering/"
  private final val k = Array(2,8,10,20)
  private final val maxIter = Array(20,50)
  private final val initializationSteps = Array(2)
  private final val tolerance = Array(0.0001)
  private final val seed = Array(1L)

  def main( args:Array[String] ):Unit = {

    TimingUtils.time {

      val outputTest = "/Users/davgutavi/Desktop/endesa/clustering/test"

      EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_raw_umr_macDavid, outputTest, Array(2), Array(20), Array(2), Array(0.0001), Array(1L))

      println("Test done")

//            val output1 = rootOutput+"lc_454d_raw_umr"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_raw_umr_hdfsUpoRoot, output1, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 1 done")
//
//            val output2 = rootOutput+"lc_454d_raw_con"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_raw_con_hdfsUpoRoot, output2, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 2 done")
//
//            val output3 = rootOutput+"lc_454d_max_umr"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_max_umr_hdfsUpoRoot, output3, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 3 done")
//
//            val output4 = rootOutput+"lc_454d_max_con"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_max_con_hdfsUpoRoot, output4, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 4 done")
//
//
//            val output5 = rootOutput+"lc_454d_slo_umr"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_slo_umr_hdfsUpoRoot, output5, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 5 done")
//
//            val output6 = rootOutput+"lc_454d_slo_con"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p454d_slo_con_hdfsUpoRoot, output6, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 6 done")
//
//
//            val output7 = rootOutput+"lc_364d_raw_umr"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_raw_umr_hdfsUpoRoot, output7, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 7 done")
//
//            val output8 = rootOutput+"lc_364d_raw_con"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_raw_con_hdfsUpoRoot, output8, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 8 done")
//
//            val output9 = rootOutput+"lc_364d_max_umr"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_max_umr_hdfsUpoRoot, output9, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 9 done")
//
//            val output10 = rootOutput+"lc_364d_max_con"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_max_con_hdfsUpoRoot, output10, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 10 done")
//
//
//            val output11 = rootOutput+"lc_364d_slo_umr"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_slo_umr_hdfsUpoRoot, output11, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 11 done")
//
//            val output12 = rootOutput+"lc_364d_slo_con"
//
//            EndesaKmeans.clusteringExperiment(DatasetPaths.p364d_slo_con_hdfsUpoRoot, output12, k, maxIter, initializationSteps, tolerance, seed)
//
//            println("Experiment 12 done")

    }

  }

}