/**
  * === Fairness of Usage 
  * Please reference this paper in your paper as 
  * Liang Zhang, Noura Alghamdi, Mohamed Y. Eltabakh, Elke A. Rundensteiner. 
  * TARDIS: Distributed Indexing Framework for Big Time Series Data. 
  * Proceedings of 35th IEEE International Conference on Data Engineering ICDE, 2019.

  * ===  Disclaimer 
  * This TARDIS is copyright protected © 2018 by Liang Zhang, Noura Alghamdi, Mohamed Y. Eltabakh, Elke A. Rundensteiner. 
  * Data Science Research Group @ WPI - Worcester Polytechnic Institute.
  * Unless stated otherwise, all software is provided free of charge. As well, all software is provided on an "as is" basis 
  * without warranty of any kind, express or implied. Under no circumstances and under no legal theory, whether in tort, 
  * contract, or otherwise, shall Liang Zhang, Noura Alghamdi, Mohamed Y. Eltabakh or Elke A. Rundensteiner be liable to 
  * you or to any other person for any indirect, special, incidental, or consequential damages of any character including, 
  * without limitation, damages for loss of goodwill, work stoppage, computer failure or malfunction, or for any and all other damages or losses.
  * If you do not agree with these terms, then you you are advised to not use the software.
  */


package org.apache.spark.edu.wpi.dsrg.tardis

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.tardis.configs.{rwCfg, idxConfig}
import org.apache.spark.edu.wpi.dsrg.tardis.utils.{Util, BreakPoint}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd._

/**
  * Created by leon on 7/6/17.
  * Modified by Dongyu Zhang on 06/13/2019.
  */
object RandWalkTsCreater extends Logging {
  def generateTsAndSave(sc: SparkContext): Unit = {
    rwCfg.printCfg()
    val idxCfg = idxConfig.generateIdxCfg()

    val tsRdd = generateTsRdd(sc,
      rwCfg.rwNumber,
      rwCfg.rwLength,
      rwCfg.rwPartitionNbr,
      rwCfg.rwSeed,
      rwCfg.rwWithRegex,
      rwCfg.rwRegexPercent,
      rwCfg.rwRegexSeg,
      rwCfg.rwRegexRange,
      idxCfg.wordLength,
      idxCfg.cardinality)

    try {
      val savePath = rwCfg.getSaveHdfsPath()
      tsRdd.saveAsObjectFile(savePath)
      writeLog(("==> Random Walk generate Time series successfully! %s").format(savePath))
    } catch {
      case e: Exception => logError(e.toString)
        System.exit(0)
    }
  }

  def generateTsRdd(sc: SparkContext, nbr: Long, length: Int, partitionNbr: Int, seed: Long, withRegex: Boolean,
                    regexPerc: Float, regexSeg: Int, regexRange: Int, wordLength: Int, cardinality: Int): RDD[(Long, Array[Float])] = {
    val random = scala.util.Random
    random.setSeed(seed)
    RandomRDDs.normalVectorRDD(sc, nbr, length, partitionNbr, seed)
      .map(x => convert(x.toArray))
      .map(x => normalize(x))
      .map(x => generateLowerBoundTs(x, length, withRegex, random, regexPerc, regexSeg, regexRange, wordLength, cardinality))
      .zipWithUniqueId()
      .map { case (x, y) => (y, x) }
  }

  private def convert(ts: Array[Double]): Array[Float] = {
    val tn = ts.map(x => x.toFloat)
    for (i <- 1 to tn.length - 1) {
      tn(i) = tn(i - 1) + tn(i)
    }
    tn
  }


  private def normalize(ts: Array[Float]): Array[Float] = {
    val count = ts.length
    val mean = ts.sum / count
    val variance = ts.map(x => math.pow(x - mean, 2)).sum / (count - 1)
    val std = math.sqrt(variance)
    ts.map(x => ((x - mean) / std).toFloat)
  }

  private def generateLowerBoundTs(ts: Array[Float], length: Int, withRegex: Boolean, random: scala.util.Random,
                                   regexPerc: Float, regexSeg: Int, regexRange: Int, wordLength: Int, cardinality: Int) = {
    var ts_final = ts
    val ts_low, ts_up = ts.clone()
    if (withRegex) {
      val regexLength = math.ceil((length * regexPerc) / regexSeg).toInt
      val segLength = math.ceil(length / wordLength).toInt
      val regexSegVec = random.shuffle((0 until wordLength).toVector)
      var regexInit: Int = 0
      val BPsDiffArr = BreakPoint(cardinality).dropRight(1).sliding(2)
        .map{ case Array(x, y) => y - x }.toArray.sortWith((x1 , x2)=> x1 < x2)
      val medBreakDiff = (BPsDiffArr(BPsDiffArr.length/2) + BPsDiffArr(BPsDiffArr.length/2)) /2
      var regexRangeVal, ub, lb: Float = 0f
      for (seg <- 0 until regexSeg){
        regexInit = segLength * regexSegVec(seg) + random.nextInt(segLength - regexLength + 1)
        regexRangeVal = random.nextFloat * regexRange * medBreakDiff
        ub = ts(regexInit) + 0.5f * regexRangeVal
        lb = ub - regexRangeVal
        (regexInit until regexInit + regexLength).foreach(i => ts_low(i) = lb)
        (regexInit until regexInit + regexLength).foreach(i => ts_up(i) = ub)
      }
      ts_final = ts_up ++ ts_low
    }
    ts_final
  }

  def writeLog(content:String): Unit ={
    Util.writeLog(content, true, rwCfg.logPath)
  }
}