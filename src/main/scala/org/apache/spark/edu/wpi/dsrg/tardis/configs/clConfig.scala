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
  */
object clCfg extends AbsConfig {
  val clExactNbr: Int = configMap("clExactNbr").toInt
//  val clRangeNbr: Int = configMap("clRangeNbr").toInt
  val clRangeNbr: Int = 100
  val clKnnNbr: Int = configMap("clKnnNbr").toInt
  val clExactPercent: Float = configMap("clExactPercent").toFloat
  val clKValue: Int = configMap("clKValue").toInt
  val clLength: Int = configMap("clLength").toInt
  val clDistance: Double = configMap("clDistance").toDouble
  val clTsFileName: String = configMap("clTsFileName")
  val clRandWalk: Boolean = Util.fetchBoolFromString(configMap("clRandWalk"))
  val clSeed: Long = configMap("clSeed").toLong
//  val clTh: Int = configMap("clTh").toInt
  val clTh: Int = 300000
  val clExcludeTsPath: String = configMap("clExcludeTsPath")

  def getSavePath(queryType: String): String = {
    val queryName = queryType.toLowerCase

    val result = queryName match {
      case "exact" => clExactNbr + "-" + clExactPercent.toString + "-exact-label"
      case "knn" => clKnnNbr + "-" + clKValue.toString + "-NN-label"
      case "range" => clRangeNbr + "-" + clDistance.toString + "-range-label"
      case _ => require(List("exact", "knn", "range").contains(queryName), "==> Only support exact,knn,range")
    }
    clTsFileName + "-" + result
  }

  override def toString: String = {
    ("\n==> Configuration" +
      "\n * clExactNbr\t%d" +
      "\n * clKnnNbr\t%d" +
      "\n * clRangeNbr\t%d" +
      "\n * clExactPercent\t%.2f" +
      "\n * clKValue\t%d" +
      "\n * clLength\t%d" +
      "\n * clDistance\t%f" +
      "\n * clTsFileName\t%s" +
      "\n * clExactMatchFileName\t%s" +
      "\n * clKnnFileName\t%s" +
      "\n * clRangeFileName\t%s" +
      "\n * clRandWalk\t%b" +
      "\n * clSeed\t%d" +
      "\n * clTh\t%d" +
      "\n * clExcludeTsPath\t%s" +
      "\n").format(
      clExactNbr,
      clKnnNbr,
      clRangeNbr,
      clExactPercent,
      clKValue,
      clLength,
      clDistance,
      clTsFileName,
      getSavePath("exact"),
      getSavePath("knn"),
      getSavePath("range"),
      clRandWalk,
      clSeed,
      clTh,
      clExcludeTsPath)
  }

  def toStr(query:String): String ={
    query match {
      case "exact" => ("\n==> Create exact label Configuration" +
        "\n * clExactNbr\t%d" +
        "\n * clExactPercent\t%.2f" +
        "\n * clLength\t%d" +
        "\n * clTsFileName\t%s" +
        "\n * clExactMatchFileName\t%s" +
        "\n * clSeed\t%d" +
        "\n * clExcludeTsPath\t%s" +
        "\n").format(
        clExactNbr,
        clExactPercent,
        clLength,
        clTsFileName,
        getSavePath("exact"),
        clSeed,
        clExcludeTsPath)
      case "knn" => ("\n==> Create knn label Configuration" +
        "\n * clKnnNbr\t%d" +
        "\n * clKValue\t%d" +
        "\n * clLength\t%d" +
        "\n * clDistance\t%f" +
        "\n * clRandWalk\t%b" +
        "\n * clTsFileName\t%s" +
        "\n * clKnnFileName\t%s" +
        "\n").format(
        clKnnNbr,
        clKValue,
        clLength,
        clDistance,
        clRandWalk,
        clTsFileName,
        getSavePath("knn"))
      case _ => "error type, only support exact and knn!"
    }
  }

  override def printCfg(): Unit = Util.writeLog(this.toString, true, logPath)

  def printCfg2(query:String): Unit = Util.writeLog(this.toStr(query), true, logPath)
}