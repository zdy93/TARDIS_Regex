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

import org.apache.spark.edu.wpi.dsrg.tardis.utils.Util
/**
  * Created by leon on 8/21/17.
  * Modified by Dongyu Zhang on 06/13/2019.
  */
object rwCfg extends AbsConfig{
  val rwNumber: Long = configMap("rwNumber").toLong
  val rwLength: Int = configMap("rwLength").toInt
  val rwPartitionNbr: Int = calPartitionNbr()
  val rwSeed: Long = configMap("rwSeed").toLong
  val rwDirPath: String = configMap("rwDirPath")
  val rwWithRegex: Boolean = Util.fetchBoolFromString(configMap("rwWithRegex"))
  val rwRegexPercent: Float = configMap("rwRegexPercent").toFloat
  val rwRegexSeg: Int = configMap("rwRegexSeg").toInt
  val rwRegexRange: Int = configMap("rwRegexRange").toInt

  def printCfg() = Util.writeLog(this.toString, true, logPath)

  def getSaveHdfsPath(): String = {
    rwDirPath + "TS-" + rwNumber.toString + "-" + rwLength.toString + "-" + rwPartitionNbr.toString
  }

  override def toString: String = {
    ("\n==> Create RandomWalk Dataset Configuration" +
      "\n * rwNumber\t%d" +
      "\n * rwLength\t%d" +
      "\n * rwPartitionNbr\t%d" +
      "\n * blockSize\t%d" +
      "\n * rwWithRegex\t%s" +
      "\n * rwRegexPercent\t%s" +
      "\n * rwRegexSeg\t%s" +
      "\n * rwRegexRange\t%s" +
      "\n * rwSavePath\t%s").format(
      rwNumber,
      rwLength,
      rwPartitionNbr,
      blockSize,
      rwWithRegex,
      rwRegexPercent,
      rwRegexSeg,
      rwRegexRange,
      getSaveHdfsPath())
  }

  private def calPartitionNbr(rate:Double=1.05):Int={
    var totalSize: Double = 0.0
    if (rwWithRegex) {
      totalSize = rwNumber*(rwLength * 2 * 4 + 8)*rate/(1024*1024)
    } else {
      totalSize = rwNumber*(rwLength * 4 + 8)*rate/(1024*1024)
    }

    math.ceil(totalSize/this.blockSize).toInt
  }
}

