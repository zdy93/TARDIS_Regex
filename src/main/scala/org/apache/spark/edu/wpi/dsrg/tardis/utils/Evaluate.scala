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

package org.apache.spark.edu.wpi.dsrg.tardis.utils

import breeze.linalg.{max, min}
import org.apache.spark.edu.wpi.dsrg.tardis.configs.eqConfig
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer

/**
  * Created by lzhang6 on 8/26/17.
  */
object Evaluate extends Logging {
  def getExact(tsNbr: Int, correctNbr: Int, accessBlockNbr: Int, duration: Long, eqExactInclude: Float): Unit = {
    val content =
      ("\n==> Result Report: Exact Matching" +
        "\n * Query Nbr\t%d" +
        "\n * Recall rate\t%.4f" +
        "\n * Average Time\t%d ms" +
        "\n * Include percent\t%.2f" +
        "\n * Block Access rate\t%.4f"
        ).format(
        tsNbr,
        correctNbr * 1.0 / tsNbr,
        duration / tsNbr,
        eqExactInclude,
        accessBlockNbr * 1.0 / tsNbr
      )
    writeLog(content)
  }

  def getKnnRecallErrorRatio(result: List[(List[(Double,Long)], List[(Double,Long)])], duration: Long): Unit = {
    //    result: query_result_List,ground_truth_List

    val recall = Util.meanDouble(result.map{case (query_result,ground_truth) => calKnnAcc(query_result.map(_._2), ground_truth.map(_._2))})
    val errorRatio = Util.meanDouble(result.map{case (query_result,ground_truth) => calKnnErrorRatio(query_result,ground_truth)})

    val content =
      ("\n==> Result Report: %d-NN Query" +
        "\n * query number\t%d" +
        "\n * error ratio\t%.4f" +
        "\n * recall \t%.4f" +
        "\n * avg time\t%d ms"
        ).format(
        eqConfig.eqKnnK,
        result.size,
        errorRatio,
        recall,
        duration / result.size
      )

    writeLog(content)
  }

  def calKnnErrorRatio(values: List[(Double,Long)], gTruth:List[(Double,Long)]): Double ={
    val values_sort = values.sortBy(_._1).map(_._1)
    val gTruth_sort = gTruth.sortBy(_._1).map(_._1)

    val length = min(values_sort.length,gTruth_sort.length)
    val result = new Array[Double](length)

    for (i <- 0 to length - 1) {
      if (gTruth_sort(i) == 0 )
        result(i) = 1
      else{
        result(i) = values_sort(i)/gTruth_sort(i)
      }
    }
    Util.meanDouble(result.toList)
  }

  def getRange(result: List[(List[Long], List[Long],Int)], duration: Long): Unit = {
    val accStr = getAcc2(result, duration, calRangeAcc)
    val content = "\n==> Result Report: %.2f-Range Query".format(eqConfig.eqDistance) + accStr
    writeLog(content)
  }

  /**
    * @param result: cRange, gRange,blkNbr,interNdNbr,termNdNbr
    * @param duration
    */
  def getRangeWithNodeNbr(result: List[(List[Long], List[Long],Int,Int,Int)], duration: Long): Unit = {
    val accStr = getAcc3(result, duration, calRangeAcc)
    val content = "\n==> Result Report: %.2f-Range Query".format(eqConfig.eqDistance) + accStr
    writeLog(content)
  }

  private def calKnnAcc(listC: List[Long], listG: List[Long]): Double = {
    val setSize = (listC ::: listG).toSet.size
    val listCSize = listC.size
    val listGSize = listG.size
//    writeLog("++> %s".format(listC.mkString("-")))
//    writeLog("++> %s".format(listG.mkString("+")))
    val acc = (listCSize + listGSize - setSize) / listGSize.toFloat
//    writeLog("++> cList: %d, gList: %d, setSize: %d,acc: %.2f".format(listC.size,listG.size,setSize,acc))
    acc
  }

  private def calRangeAcc(listC: List[Long], listG: List[Long]): Double = {
    val setSize = (listC ::: listG).toSet.size
    val listCSize = listC.size
    val listGSize = listG.size
    val acc: Double = (listCSize + listGSize - setSize) * 1.000 / listCSize
//    logError("setSize: %5d\tlistCSize: %5d\tlistGSize: %5d".format(setSize, listCSize, listGSize))
    acc
  }

  private[this] def getAcc2(result: List[(List[Long], List[Long],Int)], duration: Long, f: (List[Long], List[Long]) => Double): String = {
    val accs = ListBuffer.empty[Double]
    val blkNbrs = ListBuffer.empty[Int]
    val recordsNbrs = ListBuffer.empty[Int]

    for ((listA, listB,blkNbr) <- result) {
      val acc = f(listA, listB)
      accs += acc
      blkNbrs += blkNbr
      recordsNbrs += listA.size
    }

    val accsList = accs.toList
    val mean = Util.meanDouble(accsList)
    val stddev = Util.stddevDouble(accsList, mean)

    val content =
      ("\n * Query Nbr\t%d" +
        "\n * Acc min\t%.2f" +
        "\n * Acc max\t%.2f" +
        "\n * Acc mean\t%.2f" +
        "\n * Acc stddev\t%.2f" +
        "\n * Average Block Nbr\t%.2f" +
        "\n * Average Records Nbr\t%.2f" +
        "\n * Average Time\t%d ms"
        ).format(
        result.size,
        accsList.min,
        accsList.max,
        mean,
        stddev,
        (1.0*blkNbrs.toList.sum)/result.size,
        (1.0*recordsNbrs.toList.sum)/result.size,
        duration / result.size
      )
    content
  }

  private[this] def getAcc3(result: List[(List[Long], List[Long],Int,Int,Int)], duration: Long, f: (List[Long], List[Long]) => Double): String = {
    val accs = ListBuffer.empty[Double]
    val blkNbrs = ListBuffer.empty[Int]
    val interNdNbrs = ListBuffer.empty[Int]
    val termNdNbrs  = ListBuffer.empty[Int]
    val recordsNbrs = ListBuffer.empty[Int]

    for ((listA, listB,blkNbr,interNdNbr,termNdNbr) <- result) {
      val acc = f(listA, listB)
      accs += acc
      blkNbrs += blkNbr
      interNdNbrs += interNdNbr
      termNdNbrs += termNdNbr
      recordsNbrs += listA.size
    }

    val accsList = accs.toList
    val mean = Util.meanDouble(accsList)
    val stddev = Util.stddevDouble(accsList, mean)

    val content =
      ("\n * Query Nbr\t%d" +
        "\n * Acc min\t%.2f" +
        "\n * Acc max\t%.2f" +
        "\n * Acc mean\t%.2f" +
        "\n * Acc stddev\t%.2f" +
        "\n * Average Block Nbr\t%.2f" +
        "\n * Average internal node Nbr\t%.2f" +
        "\n * Average terminal node Nbr\t%.2f" +
        "\n * Average Records Nbr\t%.2f" +
        "\n * Average Time\t%d ms"
        ).format(
        result.size,
        accsList.min,
        accsList.max,
        mean,
        stddev,
        (1.0*blkNbrs.toList.sum)/result.size,
        (1.0*interNdNbrs.toList.sum)/result.size,
        (1.0*termNdNbrs.toList.sum)/result.size,
        (1.0*recordsNbrs.toList.sum)/result.size,
        duration / result.size
      )
    content
  }

  private[this] def writeLog(content: String): Unit = {
    Util.writeLog(content, true, eqConfig.logPath)
  }
}
