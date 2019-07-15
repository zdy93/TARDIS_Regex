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




package org.apache.spark.edu.wpi.dsrg.tardis.utils

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.tardis.configs.idxConfig

import scala.collection.mutable

/**
  * Created by leon on 8/25/17.
  */
object Statistic extends Serializable{
  def apply(path: String): Statistic = new Statistic(path)
}

class Statistic() extends Serializable{
  var hdfsFileSizeMList: List[Double] = _

  var partitionNbr: Long = _
  var blockNbr: Long = _

  var tsFileSizeG: Double = _
  var diskSizeG: Double = _

  var maxValue: Double = _
  var minValue: Double = _
  var meanValue: Double = _
  var stddevValue: Double = _

  def this(hdfsFilePath: String) {
    this()
    hdfsFileSizeMList = Util.getHdfsFileSizeMList(hdfsFilePath)
  }

  def fetchAll(): String = {
    partitionNbr = hdfsFileSizeMList.size
//    blockNbr = calBlockNbr(hdfsFileSizeMList)
//    diskSizeG = blockNbr * idxConfig.blockSize / 1024.0
    tsFileSizeG = hdfsFileSizeMList.sum / 1024.0
    maxValue = hdfsFileSizeMList.max
    minValue = hdfsFileSizeMList.min
    meanValue = Util.meanDouble(hdfsFileSizeMList)
    stddevValue = Util.stddevDouble(hdfsFileSizeMList, meanValue)

    ("\n * parititionNbr\t%d" +
      "\n * tsFileSizeG\t%.3f G" +
      "\n * max\t%.2f M" +
      "\n * min\t%.2f M" +
      "\n * mean\t%.2f M" +
      "\n * stddev\t%.2f M").format(
      partitionNbr,
      tsFileSizeG,
      maxValue,
      minValue,
      meanValue,
      stddevValue)
  }

  def fetchSizeM(): String = {
    "\n * indexSize\t%.2f M".format(hdfsFileSizeMList.sum)
  }

  private[this] def calBlockNbr(sizeList: List[Double]): Long = {
    val blockSize = idxConfig.blockSize
    sizeList.map(x => math.ceil(x * 1.0 / blockSize)).sum.toLong
  }

  def histogram(sc:SparkContext): String ={
    val th = idxConfig.blockSize
    val bps = Array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0,1.1,1.5,2.0,Double.PositiveInfinity).map{_*th}

    def convert(v:Double): Int = bps.indexWhere(v < _)

    val keyNbr = sc.parallelize(hdfsFileSizeMList)
      .map(x=>(convert(x),1))
      .reduceByKey((a,b)=>a+b)
      .collect().toList

    val keyNbrMap = mutable.HashMap.empty[Int,Int]
    keyNbrMap ++= keyNbr
    val result = keyNbrMap.toMap

    var output = ""

    for((v,idx) <- bps.zipWithIndex){
      val start = if (idx==0) 0.0 else bps(idx-1)
      val nbr = try{
        result(idx)
      }catch{
        case e:Exception => 0
      }
      val content ="\n * %3d <-> %3dM : %d".format(start.toInt, bps(idx).toInt, nbr)
      output += content
    }
    output
  }
}

object RangeLabelStat{
  def histogram(nbrs:List[Int],total_nbr:Int): String ={
    val minValue = nbrs.min
    val maxValue = nbrs.max
    val nbr = Array.fill[Int](11)(0)

    for (v <- nbrs){
      val i = ((v-minValue)*10/(maxValue-minValue))
      nbr(i) += 1
    }

    var output = "Total\t%d/%d".format(nbrs.length,total_nbr)
    for (i <- 0 to 10){
      val start = minValue + i*(maxValue-minValue)/10
        val end = minValue + (i+1)*(maxValue-minValue)/10
      val content ="\n * %5d <-> %7d : %7d   %3.2f".format(start,end,nbr(i),nbr(i)*1.0/nbrs.length)
      output += content
    }

    output
  }
}