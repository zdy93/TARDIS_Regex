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

import java.io.Serializable
import org.apache.spark.edu.wpi.dsrg.tardis.utils.Util

/**
  * Created by leon on 8/23/17.
  * Modified by Dongyu Zhang on 06/22/2019.
  */
object eqConfig extends AbsConfig {
  val eqQueryType: String = configMap("eqQueryType").toLowerCase
  val eqRepTsPath: String = configMap("eqRepTsPath")
  val eqIndexPath: String = configMap("eqIndexPath")

  val eqExcatNbr: Int = configMap("eqExcatNbr").toInt
  val eqExactInclude: Float  = configMap("eqExactInclude").toFloat
  val eqExactBF: Boolean = Util.fetchBoolFromString(configMap("eqExactBF"))
//  val eqExactBfFeature: Boolean  = Util.fetchBoolFromString(configMap("eqExactBfFeature"))
  val eqExactBfFeature: Boolean  = false
  val eqExactLabelPath:String = configMap("eqExactLabelPath")
  val eqBFPath: String = getBloomFilterPath()
  val eqTh:Int = configMap("idxThreshold").toInt

  //  val eqDEBUG:Boolean = Util.fetchBoolFromString(configMap("eqDEBUG"))
    val eqDEBUG:Boolean = false

  val eqKnnNbr: Int = configMap("eqKnnNbr").toInt
  val eqKnnK: Int = configMap("eqKnnK").toInt
  val eqKnnType:Int = configMap("eqKnnType").toInt
  val eqKnnMax:Int = configMap("eqKnnMax").toInt
  val eqKnnMin:Int = configMap("eqKnnMin").toInt
  val eqSeed: Long = configMap("eqSeed").toLong
  val eqSortSAXType: Int = configMap("eqSortSAXType").toInt
  val eqMaxSAXNbr: Int = configMap("eqMaxSAXNbr").toInt

  val eqKnnLabelPath: String = configMap("eqKnnLabelPath")

//  val eqRangeNbr: Int = configMap("eqRangeNbr").toInt
//  val eqDistance: Double = configMap("eqDistance").toDouble
//  val eqRangeWithNodeNbr: Boolean = Util.fetchBoolFromString(configMap("eqRangeWithNodeNbr"))
//  val eqRangeLabelPath: String = configMap("eqRangeLabelPath")

  val eqRangeNbr: Int = 100
  val eqDistance: Double = 7.5
  val eqRangeWithNodeNbr: Boolean = false
  val eqRangeLabelPath: String = ""

//  checkConfig()
//
//  def checkConfig(): Unit = {
//    require(List("exact", "knn-approx","knn-exact" , "range").contains(eqQueryType), "eqQueryType supports: exact,knn-approx,knn-exact,range")
//  }

  def fetchLabelHdfsName(): String = {
    eqQueryType match {
      case "exact" => "exactLabel"
      case "knn" => this.eqKnnLabelPath
      case "knn-approx" => this.eqKnnLabelPath
      case "knn-exact" => this.eqKnnLabelPath
      case "range" => this.eqRangeLabelPath
    }
  }

  def getBloomFilterPath(): String ={
    if (this.eqExactBfFeature){
      this.eqRepTsPath + "-BF-Feature"
    }else{
      this.eqRepTsPath + "-BF"
    }
  }

  override def toString: String = {
    def toTypeName(inputKnnType:Int): String ={
      inputKnnType match {
        case 0 => "Target Node Access"
        case 1 => "One Partition Access"
        case 2 => "Multi-Partitions Access"
        case _ => "error type"
      }
    }

    def toSortTypeName(sortSAXType:Int): String ={
      sortSAXType match {
        case 0 => "Random Sort"
        case 1 => "Close to Median SAX First"
        case 2 => "Far from Median SAX First"
        case 3 => "Close to Ignore regex SAX First"
        case 4 => "Far from Ignore regex SAX First"
        case 5 => "Equal Width Sort"
        case 6 => "Equal Width Compute"
        case _ => "Error Sort Type"
      }
    }

    val start = ("\n==> Configuration" +
      "\n * eqQueryType\t%s").format(eqQueryType)

    val knnContent = (
      "\n * eqKnnType\t%s" +
      "\n * eqKnnNbr\t%d" +
      "\n * eqKnnK\t%d" +
      "\n * eqKnnMax\t%d" +
      "\n * eqKnnMin\t%d" +
        "\n * eqSeed\t%d" +
        "\n * eqSortSAXType\t%s"+
        "\n * eqMaxSAXNbr\t%d"+
      "\n * eqKnnLabelPath\t%s" +
      "\n * eqRepTsPath\t%s" +
      "\n * eqIndexPath\t%s" +
      "\n").format(
      toTypeName(eqKnnType),
      eqKnnNbr,
      eqKnnK,
      eqKnnMax,
      eqKnnMin,
      eqSeed,
      toSortTypeName(eqSortSAXType),
      eqMaxSAXNbr,
      eqKnnLabelPath,
      eqRepTsPath,
      eqIndexPath)

    val exactContent = (
      "\n * eqExcatNbr\t%d" +
      "\n * eqExactLabelPath\t%s" +
      "\n * eqExactInclude\t%.2f" +
      "\n * eqExactBF\t%b" +
      "\n * eqRepTsPath\t%s" +
      "\n * eqIndexPath\t%s" +
      "\n").format(
      eqExcatNbr,
      eqExactLabelPath,
      eqExactInclude,
      eqExactBF,
      eqBFPath,
      eqRepTsPath,
      eqIndexPath)

    val content = eqQueryType match {
      case "exact" => exactContent
      case "knn"|"knn-approx"|"knn-exact" => knnContent
      case _ => "\n Don't support range query! "
    }

    start + content
  }


  override def printCfg(): Unit = Util.writeLog(this.toString, true, logPath)

  def generateEqCfg(): EqCfg = {
    new EqCfg(
      this.eqQueryType,
      this.eqTh,
      this.eqRepTsPath,
      this.eqIndexPath,
      this.eqExcatNbr,
      this.eqExactLabelPath,
      this.eqExactInclude,
      this.eqExactBF,
      this.eqExactBfFeature,
      this.eqBFPath,
      this.eqKnnNbr,
      this.eqKnnK,
      this.eqKnnType,
      this.eqKnnMax,
      this.eqKnnMin,
      this.eqSeed,
      this.eqSortSAXType,
      this.eqMaxSAXNbr,
      this.eqKnnLabelPath,
      this.eqRangeNbr,
      this.eqRangeWithNodeNbr,
      this.eqDistance,
      this.eqRangeLabelPath)
  }
}

class EqCfg( val eqQueryType: String,
             val eqTh:Int,
             val eqRepTsPath: String,
             val eqIndexPath: String,
             val eqExcatNbr:Int,
             val eqExactLabelPath:String,
             val eqExactInclude: Float,
             val eqExactBF: Boolean,
             val eqExactBfFeature:Boolean,
             val eqBFPath:String,
             val eqKnnNbr: Int,
             val eqKnnK: Int,
             val eqKnnType: Int,
             val eqKnnMax: Int,
             val eqKnnMin: Int,
             val eqSeed: Long,
             val eqSortSAXType: Int,
             val eqMaxSAXNbr: Int,
             val eqKnnLabelPath: String,
             val eqRangeNbr: Int,
             val eqRangeWithNodeNbr:Boolean,
             val eqDistance: Double,
             val eqRangeLabelPath:String
           )extends Serializable{
}