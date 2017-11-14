package es.upo.datalab.datamining

import java.text.{DecimalFormat, DecimalFormatSymbols}

import es.upo.datalab.entrypoints.datasets.FraudesDiariosMax.sqlContext
import es.upo.datalab.utilities.SparkSessionUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.GBTClassificationModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.math.sqrt


object GBTexperiment {


  final val sqlContext = SparkSessionUtils.sql

  final val sc = SparkSessionUtils.sc


  def getStudyDataframe(test: DataFrame,campo: DataFrame, cvmodel:CrossValidatorModel):DataFrame={

    val schema = Array(
      StructField("tipo",   StringType,  true)
      ,StructField("tn",    StringType,  true)
      ,StructField("fn",    StringType,  true)
      ,StructField("fp",    StringType,  true)
      ,StructField("tp",    StringType,  true)
      ,StructField("tpr",   StringType,  true)
      ,StructField("tnr",   StringType,  true)
      ,StructField("ppv",   StringType,  true)
      ,StructField("npv",   StringType,  true)
      ,StructField("fnr",   StringType,  true)
      ,StructField("fpr",   StringType,  true)
      ,StructField("fdr",   StringType,  true)
      ,StructField("for",   StringType,  true)
      ,StructField("acc",   StringType,  true)
      ,StructField("f1",    StringType,  true)
      ,StructField("mcc",   StringType,  true)
      ,StructField("bm",    StringType,  true)
      ,StructField("mk",    StringType,  true)
//      ,StructField("param", MapType(StringType,StringType),  true))
    ,StructField("param", StringType,  true))

    val customSchema = StructType(schema)

    val rows = Seq[Row](getResultsLine(test,"test",cvmodel),getResultsLine(campo,"campo",cvmodel))

    val rdd:RDD[Row] = sc.makeRDD[Row](rows)

    val df: DataFrame = sqlContext.createDataFrame(rdd,customSchema)


     df



  }




  private def getResultsLine (results: DataFrame, tipo:String, cvmodel:CrossValidatorModel):Row = {



    val model = cvmodel.bestModel.asInstanceOf[PipelineModel].stages(1).asInstanceOf[GBTClassificationModel]

    val otherSymbols = new DecimalFormatSymbols(java.util.Locale.getDefault)
    otherSymbols.setDecimalSeparator('.')
    otherSymbols.setGroupingSeparator(',')
    val fr = new DecimalFormat("#.##", otherSymbols)

    val impurity            = model.getImpurity+""
    val losstype            = model.getLossType+""
    val maxBins             = model.getMaxBins+""
    val maxDepth            = model.getMaxDepth+""
    val maxIter             = model.getMaxIter+""
    val minInfoGain         = fr.format(model.getMinInfoGain)
    val minInstancesPerNode = model.getMinInstancesPerNode+""
    val seed                = model.getSeed+""
    val stepSize            = fr.format(model.getStepSize)
    val subsamplingRate     = fr.format(model.getSubsamplingRate)
    val folds = cvmodel.getNumFolds+""


//    val config = Map[String,String]("impurity" -> impurity,"lossType"->losstype,
//                                    "maxBins"->maxBins,maxDepth->maxDepth,"maxIter"->maxIter,"minInfoGain"->minInfoGain,
//                                    "minInstancesPerNode"->minInstancesPerNode,"seed"->seed, "stepSize"->stepSize,
//                                    "subsamplingRate"->subsamplingRate,"folds"->folds)


    val config = "impurity="+impurity+"#lossType="+losstype+"#maxBins="+maxBins+"#maxDepth="+maxDepth+"#maxIter="+maxIter+
                     "#minInfoGain="+minInfoGain+"#minInstancesPerNode="+minInstancesPerNode+
                      "#seed="+seed+"#stepSize="+stepSize+"#subsamplingRate="+subsamplingRate+"#folds="+folds




    val tn   = results.where("prediction = 0 AND label = 0").count().toDouble
    val fn   = results.where("prediction = 0 AND label = 1").count().toDouble
    val fp   = results.where("prediction = 1 AND label = 0").count().toDouble
    val tp   = results.where("prediction = 1 AND label = 1").count().toDouble

    val tpr  = tp / (tp+fn)
    val tnr  = tn / (tn+fp)
    val ppv  = tp / (tp+fp)
    val npv  = tn / (tn+fn)

    val fnr  = 1 - tpr
    val fpr  = 1 - tnr
    val fdr  = 1 - ppv
    val forr = 1 - npv

    val acc  = (tp+tn) / (tp+tn+fp+fn)

    val f1   = 2 * ((ppv*tpr)/(ppv+tpr))
    val mcc  = ((tp*tn)-(fp*fn))/ sqrt((tp+fp)*(tp+fn)*(tn+fp)*(tn+fn))
    val bm   = tpr + tnr -1
    val mk   = ppv + npv -1




    Row(tipo,fr.format(tn),fr.format(fn),fr.format(fp),fr.format(tp),
      fr.format(tpr),fr.format(tnr),fr.format(ppv),fr.format(npv),
      fr.format(fnr),fr.format(fpr),fr.format(fdr),fr.format(forr),fr.format(acc),
      fr.format(f1),fr.format(mcc),fr.format(bm),fr.format(mk),config)


  }

}
