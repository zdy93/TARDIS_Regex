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


package org.apache.spark.edu.wpi.dsrg.tardis

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.tardis.configs.{clCfg, eqConfig, idxConfig, rwCfg}
import org.apache.spark.edu.wpi.dsrg.tardis.isax.RootClusterNodeR
import org.apache.spark.edu.wpi.dsrg.tardis.utils.{RangeLabelStat, Util}
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by leon on 8/21/17.
  */
object CreateLabel extends Logging {
  def apply(sc: SparkContext, queries: String): Unit = {
    clCfg.printCfg2(queries)
    val cl = new CreateLabel(sc, queries)
    cl.generateAllLabels()
  }
}

class CreateLabel(sc: SparkContext, queries: String) extends Logging {
  val indexHex = new IndexHex(sc)
  val idxCfg = idxConfig.generateIdxCfg()
  val eqCfg = eqConfig.generateEqCfg()

  def generateAllLabels(): Unit = {
    val (exactB, knnB, rangeB) = getExcuteQuery(queries)
    if (exactB) createExactLabel()
    if (knnB) createKnnLabel()
    if (rangeB) createRangeLabel()
  }

  def generateExactLables(include: Float, nbr: Int, percent: Float = 0.5F): List[(Array[Float], Boolean)] = {
    if (include > 0 && include < 1) {
      val includeLabels = fetchIncludedLabels((nbr * include).toInt)
      val excludeLabels = generateExcludedLabels((nbr * (1 - include)).toInt)
      Random.shuffle(excludeLabels ++ includeLabels)
    } else {
      if (include == 0)
        generateExcludedLabels(nbr)
      else
        fetchIncludedLabels(nbr)
    }
  }

  def createExactLabel(): Unit = {
    writeLog("==> Retrieve Exact Match Query labels")
    val exactLabels = generateExactLables(clCfg.clExactPercent, clCfg.clExactNbr)

    val exactPath = clCfg.getSavePath("exact")
    sc.parallelize(exactLabels).coalesce(2).saveAsObjectFile(exactPath)
    writeLog("==> Save %s Result to %s".format("exact", exactPath))
  }

  private def fetchIncludedLabels(nbr: Int, blockNbr: Int = 20): List[(Array[Float], Boolean)] = {
    writeLog("==> Begin to fetch time series records from hdfs")

    val tsFileNames = Util.getHdfsFileNameList(clCfg.clTsFileName)

    val hdfsFileNames = if (tsFileNames.length > blockNbr) {
      Random.shuffle(tsFileNames).take(blockNbr).mkString(",")
    } else {
      clCfg.clTsFileName
    }

    sc.objectFile[(Long, Array[Float])](hdfsFileNames)
      .takeSample(false, nbr)
      .map { case (id, ts) => (ts, true) }
      .toList
  }

  private def generateExcludedLabels(excludeTsNbr: Int): List[(Array[Float], Boolean)] = {
    val tsRdd = if (clCfg.clRandWalk) {
      RandWalkTsCreater.generateTsRdd(sc, excludeTsNbr, clCfg.clLength, 1, clCfg.clSeed, rwCfg.rwWithRegex,
        rwCfg.rwRegexPercent, rwCfg.rwRegexSeg, rwCfg.rwRegexRange, idxCfg.wordLength, idxCfg.cardinality)
    } else {
      sc.objectFile[(Long, Array[Float])](clCfg.clExcludeTsPath)
    }

    tsRdd
      .map { x => (x._2, false) }
      .collect().toList
  }

  def getExcuteQuery(queries: String): (Boolean, Boolean, Boolean) = {
    def removeFile(queryType: String): Unit = {
      val path = clCfg.getSavePath(queryType)
      if (Util.hdfsDirExists(path)) {
        Util.removeHdfsFiles(path)
        writeLog("==> Remove %s".format(path))
      }
    }

    val qs = queries.split("-")

    for (q <- List("exact", "knn", "range")) {
      if (qs.contains(q)) {
        removeFile(q)
      }
    }
    (qs.contains("exact"), qs.contains("knn"), qs.contains("range"))
  }

  def createKnnLabel(): Unit = {
    indexHex.readTreeHex(sc, eqCfg.eqIndexPath)

    val nbr = if (clCfg.clKnnNbr > clCfg.clRangeNbr) clCfg.clKnnNbr else clCfg.clRangeNbr



    val tsQuery = if(clCfg.clRandWalk){
      RandWalkTsCreater
        .generateTsRdd(sc, nbr * 50, clCfg.clLength, 1, clCfg.clSeed,
          rwCfg.rwWithRegex, rwCfg.rwRegexPercent, rwCfg.rwRegexSeg, rwCfg.rwRegexRange, idxCfg.wordLength, idxCfg.cardinality)
        .map { case (id, ts) => ts }
        .collect()
    } else{
      sc.objectFile[(Long,Array[Float])](clCfg.clTsFileName)
        .take(nbr * 50)
        .map { case (id, ts) => ts }
    }

    val knnNbrBc = sc.broadcast(clCfg.clKValue)
    val knnRstCol = ListBuffer.empty[(Array[Float], List[(Double, Long)])]

    val rangDistBc = sc.broadcast(clCfg.clDistance)
    var knnIdx = 0

    try {
      for (ts <- tsQuery if (knnIdx < clCfg.clKnnNbr)) {
        val result = queryOneRangDistId(ts, rangDistBc.value)
        if (result.length > clCfg.clKValue) {
          val knnR = result
            .sortWith(_._1 < _._1)
            .take(knnNbrBc.value)

          knnRstCol += ((ts, knnR))
          knnIdx += 1
        }
      }
    } catch {
      case e: Exception => logError(e.toString)
    } finally {
      writeLog("==> number of ts has enough neighbors is %d".format(knnRstCol.size))
      val knnPath = clCfg.getSavePath("knn")
      sc.parallelize(knnRstCol).coalesce(2).saveAsObjectFile(knnPath)
      writeLog("==> Save %s Result to %s".format("knn", knnPath))
    }
  }

  def createRangeLabel(): Unit = {
    indexHex.readTreeHex(sc, eqCfg.eqIndexPath)

    val nbr = if (clCfg.clKnnNbr > clCfg.clRangeNbr) clCfg.clKnnNbr else clCfg.clRangeNbr
    val tsQuery = RandWalkTsCreater
      .generateTsRdd(sc, nbr * 50, clCfg.clLength, 1, clCfg.clSeed,
        rwCfg.rwWithRegex, rwCfg.rwRegexPercent, rwCfg.rwRegexSeg, rwCfg.rwRegexRange, idxCfg.wordLength, idxCfg.cardinality)
      .map { case (id, ts) => ts }
      .collect()

    writeLog("==> generate records %d".format(tsQuery.length))

    val rangDistBc = sc.broadcast(clCfg.clDistance)
    val rangeRstCol = ListBuffer.empty[(Array[Float], List[(Double, Long)])]

    val range_nbr_list = ListBuffer.empty[Int]

    var rangeIdx = 0

    try {
      for (ts <- tsQuery if (rangeIdx < clCfg.clRangeNbr)) {
        val result = queryOneRangDistId(ts, rangDistBc.value)
        writeLog("++> result nbr: %d, clTh: %d, ".format(result.length, clCfg.clTh))

        if (result.length <= clCfg.clTh && result.length > 0) {
          rangeRstCol += ((ts, result))
          range_nbr_list += result.length
          rangeIdx += 1
          writeLog("++> rangeIdex: %d".format(rangeIdx))
        }
      }
    } catch {
      case e: Exception => logError(e.toString)
    } finally {
      val rangePath = clCfg.getSavePath("range")
      sc.parallelize(rangeRstCol).coalesce(2).saveAsObjectFile(rangePath)
      writeLog("==> Save %s Result to %s".format("range", rangePath))
      writeLog(RangeLabelStat.histogram(range_nbr_list.toList, clCfg.clRangeNbr))
    }
  }


  private def queryOneRangDistId(ts: Array[Float], distT: Double): List[(Double, Long)] = {
    val pidList = this.indexHex.getRangePids(ts, distT, idxCfg)

    val path = pidList.map(pid => convertPIdToPath(pid)).mkString(",")
    val tsRdd = sc.objectFile[RootClusterNodeR](path)

    val ifg = idxCfg
    val result = tsRdd.map(x => x.fetchRangDistId(ts, distT, ifg)).collect().toList.flatten
    result
  }

  private def convertPIdToPath(pId: Int): String = {
    require(pId >= 0 && pId < 100000, "pId: %d>= 0 && pId < 100000".format(pId))
    eqCfg.eqRepTsPath + "/part-" + Util.genZero(5 - pId.toString.length) + pId.toString
  }

  private[this] def writeLog(content: String): Unit = {
    Util.writeLog(content, true, clCfg.logPath)
  }
}
