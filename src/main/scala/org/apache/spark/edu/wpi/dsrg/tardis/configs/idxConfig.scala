/**
  * === Fairness of Usage 
  * Please reference this paper in your paper as 
  * Liang Zhang, Noura Alghamdi, Mohamed Y. Eltabakh, Elke A. Rundensteiner. 
  * TARDIS: Distributed Indexing Framework for Big Time Series Data. 
  * Proceedings of 35th IEEE International Conference on Data Engineering ICDE, 2019.

  * ===  Disclaimer 
  * This program is copyright protected © 2018 by Liang Zhang, Noura Alghamdi, Mohamed Y. Eltabakh, Elke A. Rundensteiner. 
  * Data Science Research Group @ WPI - Worcester Polytechnic Institute.
  * Unless stated otherwise, all software is provided free of charge. As well, all software is provided on an "as is" basis 
  * without warranty of any kind, express or implied. Under no circumstances and under no legal theory, whether in tort, 
  * contract, or otherwise, shall Liang Zhang, Noura Alghamdi, Mohamed Y. Eltabakh or Elke A. Rundensteiner be liable to 
  * you or to any other person for any indirect, special, incidental, or consequential damages of any character including, 
  * without limitation, damages for loss of goodwill, work stoppage, computer failure or malfunction, or for any and all other damages or losses.
  * If you do not agree with these terms, then you you are advised to not use the software.
  */


package org.apache.spark.edu.wpi.dsrg.tardis.configs

/**
  * Created by leon on 8/21/17.
  */

import java.io._
import java.lang.Math.{ceil, log => mathLog}

import org.apache.spark.edu.wpi.dsrg.tardis.utils.{BreakPoint, Util}

object idxConfig extends AbsConfig {
  val tsFileName: String = configMap("idxTsFileName")
  val cardinality: Int = configMap("idxCardinality").toInt
  val tsLength: Int = configMap("idxTsLength").toInt

  val isSampling: Boolean = Util.fetchBoolFromString(configMap("idxIsSampling"))
  val samplePercent: Double = configMap("idxSamplePercent").toDouble

  val blockOccupyPercent: Double = configMap("idxBlockOccupyPercent").toDouble
//  val cursor: Int = configMap("idxCursor").toInt
  val cursor: Int = 3

  val step: Int = getStep(tsLength, configMap("idxWantedWordLength").toInt)
  val breakPoints: Array[Float] = BreakPoint(this.cardinality).clone()
  val wordLength: Int = getRealWordLength(this.tsLength, this.step)
  val bitStep: Int = math.ceil((wordLength * 1.0) / 4).toInt
  val startHrc: Int = (ceil(mathLog(cardinality) / mathLog(2))).toInt
  val bf: Boolean = Util.fetchBoolFromString(configMap("idxBf"))
//  val idxBfFeature: Boolean = Util.fetchBoolFromString(configMap("idxBfFeature"))
  val idxBfFeature: Boolean = false
  val idxRepartitionTs: Boolean = Util.fetchBoolFromString(configMap("idxRepartitionTs"))
  val blockCapacityTh: Long = getThreshold()
  val idxTh:Int = configMap("idxThreshold").toInt
  var partitionNbr:Long = 0

  checkConfig()

  def checkConfig(): Unit = {
    Util.checkDir(new File(this.logPath))
    require(this.blockOccupyPercent <= 1, "this.blockOccupyPercent<=1")
    val cards = Array(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 32, 64, 128, 256)
    require(cards.contains(this.cardinality), "cards.contains(this.cardinality)")
    require(cursor <= startHrc, "adaptiveHrc<=idxConfig.startHrc")
  }

  def getIndexFilePath(): String = {
    getSaveFilePath() + "-index"
  }

  def getSaveFilePath(): String = {
    val str = this.tsFileName + "-pct-" + this.samplePercent.toString + "-"


    val last = if (this.idxRepartitionTs){
      "-TS"
    }else{
      "-noTS"
    }

    str + this.wordLength.toString + "-BF-" + this.bf.toString + "-Th-" + this.idxTh.toString  + last
  }

  def getBloomFilterPath(): String ={
    if (this.idxBfFeature){
      getSaveFilePath()+"-BF-Feature"
    }else{
      getSaveFilePath()+"-BF"
    }

//    getSaveFilePath()+"-BF"
  }

  private[this] def getStep(tslength: Int, wordlength: Int): Int = {
    if (tslength % wordlength == 0)
      tslength / wordlength
    else {
      logWarning("Real word length is %d rather than %d"
        .format(getRealWordLength(tslength, (tslength / wordlength + 1)), wordlength))
      (tslength / wordlength + 1)
    }
  }

  private[this] def getRealWordLength(tslength: Int, step: Int): Int = {
    ceil((tslength * 1.0) / step).toInt
  }

  private[this] def getThreshold(rate:Double=1.08): Long = {
    if (idxRepartitionTs){
      val diskSpace = blockOccupyPercent * blockSize * 1024 * 1024
      val unitSize = (tsLength * 4 + 8 + (wordLength / 4) * 2 * startHrc)*rate
      (diskSpace / unitSize).toLong
    }else{
      partitionNbr = configMap("idxPartitionNbr").toLong
      val recordNbr = tsFileName.split("/").last.split("-")(1).toLong
      (1.15*recordNbr/partitionNbr).toLong
    }
  }

  override def toString: String = {
      if (idxRepartitionTs){
        ("\n==> Clustered Index Configuration" +
          "\n * idxTsFilename\t%s" +
          "\n * idxWordLength\t%d" +
          "\n * idxCardinality\t%d" +
          "\n * idxTsLength\t%d" +
          "\n * idxStep\t%d" +
          "\n * idxBitStep\t%d" +
          "\n * idxBlockSizeofHdfs\t%d" +
          "\n * idxBlockOccupyPercent\t%.2f" +
          "\n * idxBlockCapacityTh\t%d" +
          "\n * idxTh\t%d" +
          "\n * idxIsSampling\t%b" +
          "\n * idxRepartitionTs\t%b" +
          "\n * idxSamplePercent\t%.2f").format(
          tsFileName,
          wordLength,
          cardinality,
          tsLength,
          step,
          bitStep,
          blockSize,
          blockOccupyPercent,
          blockCapacityTh,
          idxTh,
          isSampling,
          idxRepartitionTs,
          samplePercent)
      }else{
        ("\n==> Unclustered Index Configuration" +
          "\n * idxTsFilename\t%s" +
          "\n * idxWordLength\t%d" +
          "\n * idxCardinality\t%d" +
          "\n * idxTsLength\t%d" +
          "\n * idxStep\t%d" +
          "\n * idxBitStep\t%d" +
          "\n * idxIsSampling\t%b" +
          "\n * idxRepartitionTs\t%b" +
          "\n * idxSamplePercent\t%.2f"+
          "\n * idxPartitionNbr\t%l").format(
          tsFileName,
          wordLength,
          cardinality,
          tsLength,
          step,
          bitStep,
          isSampling,
          idxRepartitionTs,
          samplePercent,
          partitionNbr)
      }
  }

  override def printCfg(): Unit = Util.writeLog(this.toString, true, logPath)

  def generateIdxCfg(): IdxCfg = {
    new IdxCfg(
      this.tsFileName,
      this.cardinality,
      this.startHrc,
      this.bitStep,
      this.step,
      this.cursor,
      this.tsLength,
      this.idxTh,
      this.wordLength,
      this.samplePercent,
      this.blockCapacityTh,
      this.breakPoints)
  }
}

class IdxCfg(val tsFileName: String,
             val cardinality: Int,
             val startHrc: Int,
             val bitStep: Int,
             val step: Int,
             val cursor:Int,
             val tsLength: Int,
             val idxTh:Int,
             val wordLength: Int,
             val samplePercent:Double,
             val blockCapacityTh:Long,
             val breakPoints: Array[Float]
            )extends Serializable{
}