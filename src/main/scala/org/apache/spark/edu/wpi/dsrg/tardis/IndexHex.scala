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

/**
  * Created by leon on 8/20/17.
  */

import bloomfilter.mutable.BloomFilter
import org.apache.spark.edu.wpi.dsrg.tardis.configs.{IdxCfg, idxConfig}
import org.apache.spark.edu.wpi.dsrg.tardis.isax.{iSAXTreeHex, _}
import org.apache.spark.edu.wpi.dsrg.tardis.utils._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkContext, TaskContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.util.Random

object IndexHex extends Logging with Serializable {
  def apply(sc: SparkContext): Unit = {
    idxConfig.printCfg()

    val indexHex = new IndexHex(sc)
    if (!isResultExist())
      indexHex.constructIndex()
  }

  def isResultExist(): Boolean = {
    val indexExists = Util.hdfsDirExists(idxConfig.getIndexFilePath())
    val repartitionFileExists = Util.hdfsDirExists(idxConfig.getSaveFilePath())

    if (indexExists && repartitionFileExists) {
      Util.writeLog("==> Repartitioned Time series data and iSAXTree exists", true, idxConfig.logPath)
      true
    }
    else {
      if (repartitionFileExists)
        Util.removeHdfsFiles(idxConfig.getSaveFilePath())

      if (indexExists)
        Util.removeHdfsFiles(idxConfig.getIndexFilePath())
      false
    }
  }
}

class IndexHex(sc: SparkContext) extends Logging with Serializable {
  var treeHex = new iSAXTreeHex(idxConfig.bitStep)

  def constructIndex(): Unit = {
    val idxCfg = idxConfig.generateIdxCfg()

    if (idxConfig.idxRepartitionTs) {
      this.buildTreeAndRepartitionSampling(idxCfg: IdxCfg)

      this.calIndexQuality(true)
    } else {
      this.buildTreeAndRepartitioniSaxOnly(idxCfg: IdxCfg)
      this.calIndexQuality(false)
    }
  }

  def buildTreeAndRepartitioniSaxOnly(idxCfg: IdxCfg): Unit = {
    val saxRdd = sc.objectFile[(Long, Array[Float])](idxCfg.tsFileName).map { case (id, ts) => (Ops.cvtTsToIsaxHex(ts, idxCfg), id) }.persist(StorageLevel.MEMORY_AND_DISK)

    val nodeList = generateNodeListForTreeSamplingiSaxOnly(idxCfg, saxRdd)

    treeHex.loadCollectionSampling(nodeList)
    treeHex.assignPIdSampling(idxCfg.blockCapacityTh)

    val iSaxPartitioner = new iSAXPartitionerSampling(treeHex)
    val bitStep = idxCfg.bitStep
    val th = idxConfig.idxTh

    val path = idxConfig.getSaveFilePath()
    saxRdd
      .partitionBy(iSaxPartitioner)
      .mapPartitions[RootUnclusterNode](x => RootUnclusterNode(x, bitStep, th))
      .saveAsObjectFile(path)

    writeLog("==> Save to isax file to %s".format(path))
    this.saveTreeHex(sc)
  }


  def buildTreeAndRepartitionSampling(idxCfg: IdxCfg): Unit = {
    val nodeList = generateNodeListForTreeOneSampling(idxCfg)

    treeHex.loadCollectionSampling(nodeList)
    treeHex.assignPIdSampling(idxCfg.blockCapacityTh)

    val saxTsIdPairRdd = loadWholeTsFromHdfs(idxCfg)
    val iSaxPartitioner = new iSAXPartitionerSampling(treeHex)
    repartitionRDD(saxTsIdPairRdd,
      iSaxPartitioner,
      idxConfig.getSaveFilePath(),
      idxConfig.bf,
      idxConfig.getBloomFilterPath())
  }

  def fetchPId(tsSax: String): Int = {
    treeHex.getPId(tsSax)
  }

  def fetchFatherNodePIdList(tsSax: String): List[Int]={
    treeHex.getFatherNodePIdList(tsSax)
  }

  def getRangePids(ts: Array[Float], distT: Double, idxCfg: IdxCfg): List[Int] = {
    val result = this.treeHex.getRangePids(ts, distT, idxCfg)
    result.distinct
  }

  def calIndexQuality(withTS: Boolean): Unit = {
    val repartTsPath = idxConfig.getSaveFilePath()
    val indexPath = idxConfig.getIndexFilePath()
    var content = ""

    if (withTS) {
      val rawTsPath = idxConfig.tsFileName

      content = "==> Index Quality with TS" +
        "\n++> Raw Time series---------------" + Statistic(rawTsPath).fetchAll() +
        "\n++> Repartition Time series-------" + Statistic(repartTsPath).fetchAll() +
        "\n++> Repartition Histogram---------" + Statistic(repartTsPath).histogram(sc) +
        "\n++> Index Hex---------------------" + Statistic(indexPath).fetchSizeM()

      if (idxConfig.bf) {
        content +=
          "\n++> Bloom Filter---------------------" + Statistic(idxConfig.getBloomFilterPath()).fetchSizeM()
      }
    } else {
      content = "==> Index Quality without TS" +
        "\n++> Index Hex---------------------" + Statistic(indexPath).fetchSizeM()
    }

    writeLog(content)
  }

  def saveTreeHex(sc: SparkContext): Unit = {
    try {
      val fileName = idxConfig.getIndexFilePath()

      sc.parallelize[iSAXTreeHex](Seq(this.treeHex)).coalesce(1).saveAsObjectFile(fileName)
      writeLog("==> Save Index to %s".format(fileName))
    } catch {
      case e: Exception => logError(e.toString)
    }
  }

  def readTreeHex(sc: SparkContext, fileName: String): Unit = {
    try {
      this.treeHex = sc.objectFile[iSAXTreeHex](fileName).collect()(0)
      writeLog("==> Read Index from %s".format(fileName))
    } catch {
      case e: Exception => logError("IndexHex: readTreeHex(fileName: String) method\n" + e.toString)
    }
  }

  private def loadWholeTsFromHdfs(idxCfg: IdxCfg): RDD[(String, (Array[Float], Long))] = {
    writeLog("==> Begin to load whole time series datasets")
    readTsFromHDFS(idxCfg.tsFileName, idxCfg)
  }

  private def generateNodeListForTreeSamplingiSaxOnly(idxCfg: IdxCfg, saxRDD: RDD[(String, Long)]): List[List[(String, Long)]] = {
    writeLog("==> Begin to load sampling data for only iSax")
    val sampleThreshold = math.ceil(idxCfg.blockCapacityTh * idxCfg.samplePercent).toLong
    val BITSTEP = idxCfg.bitStep

    var reachBottom: Boolean = false
    var treeLayer = 1
    var outPutCollection = ListBuffer.empty[List[(String, Long)]]

    var pairRDD = saxRDD.sample(false, idxCfg.samplePercent)
      .map { case (sax, id) => (sax, 1L) }
      .reduceByKey((x, y) => x + y)

    while (!reachBottom) {
      val saxNbrPairRDD = pairRDD
        .map { case (sax, nbr) => (Ops.cvtSAXtoSpecHrc(sax, treeLayer, BITSTEP), nbr) }
        .reduceByKey((x, y) => x + y)

      outPutCollection += saxNbrPairRDD.collect().toList

      if (treeLayer == 1) {
        writeLog("==> %d / %d".format(outPutCollection(0).length, math.pow(2, idxCfg.wordLength).toInt))
      }

      val maxValue = saxNbrPairRDD.map { case (k, v) => v }.max

      if (maxValue > sampleThreshold) {
        val smallSaxArray = saxNbrPairRDD
          .filter { case (sax, nbr) => nbr <= sampleThreshold }
          .keys.collect()
        val smallHashSet = HashSet(smallSaxArray: _*)

        pairRDD = pairRDD
          .map { case (isax, nbr) => (Ops.cvtSAXtoSpecHrc(isax, treeLayer, BITSTEP), isax, nbr) }
          .filter { case (sax_s, sax, nbr) => !smallHashSet.contains(sax_s) }
          .map { case (sax_s, sax, nbr) => (sax, nbr) }.persist(StorageLevel.MEMORY_ONLY)

        treeLayer += 1
      } else {
        reachBottom = true
      }
    }

    writeLog("==> Finish collect %d layers nodes for iSAX tree construction".format(treeLayer))
    outPutCollection.toList
  }

  private def generateNodeListForTreeOneSampling(idxCfg: IdxCfg): List[List[(String, Long)]] = {
    writeLog("==> Begin to load sampling data for iSax and Time Series")
    val sampleThreshold = math.ceil(idxCfg.blockCapacityTh * idxCfg.samplePercent).toLong
    val BITSTEP = idxCfg.bitStep

    var reachBottom: Boolean = false
    var treeLayer = 1
    var outPutCollection = ListBuffer.empty[List[(String, Long)]]

    val partFileName = getHdfsFileNamesSampling(idxCfg)
    var pairRDD = sc.objectFile[(Long, Array[Float])](partFileName)
      .map { case (id, ts) => (Ops.cvtTsToIsaxHex(ts, idxCfg), 1L) }
      .reduceByKey((x, y) => x + y).cache()

    while (!reachBottom) {

      //      pairRDD = serialize_and_deserialize(pairRDD)

      val dataRDD = pairRDD
        .map { case (isax, nbr) => (Ops.cvtSAXtoSpecHrc(isax, treeLayer, BITSTEP), isax, nbr) }.cache()

      val saxNbrPairRDD = dataRDD
        .map { case (sax_s, isax, nbr) => (sax_s, nbr) }
        .reduceByKey((x, y) => x + y).cache()

      outPutCollection += saxNbrPairRDD.collect().toList

      writeLog("==> layer-%d collect:  %d / %d".format(treeLayer,outPutCollection(treeLayer-1).length, math.pow(2*treeLayer, idxCfg.wordLength).toInt))

      val maxValue = saxNbrPairRDD.map { case (k, v) => v }.max
//      writeLog("++> max / sampleThreshold: :  %d / %d".format(maxValue,sampleThreshold))

      if (maxValue > sampleThreshold) {
        val smallSaxArray = saxNbrPairRDD
          .filter { case (sax, nbr) => nbr <= sampleThreshold }
          .keys.collect()
        val smallHashSet = HashSet(smallSaxArray: _*)

        pairRDD = dataRDD
          .filter { case (sax_s, sax, nbr) => !smallHashSet.contains(sax_s) }
          .map { case (sax_s, sax, nbr) => (sax, nbr) }.cache()

        treeLayer += 1
      } else {
        reachBottom = true
      }
    }

    writeLog("==> Finish collect %d layers nodes for iSAX tree construction".format(treeLayer))
    outPutCollection.toList
  }

  def serialize_and_deserialize(rdd:RDD[(String,Long)]): RDD[(String,Long)] ={
    sc.parallelize(rdd.collect())
  }

  private def repartitionRDD(saxTsPairRdd: RDD[(String, (Array[Float], Long))],
                                     partitioner: Partitioner,
                                     path: String,
                                     bf: Boolean = false,
                                     bfPath: String = ""): Unit = {
    val bitStep = idxConfig.bitStep
    val th = idxConfig.idxTh

    val saxTsIdRdd = saxTsPairRdd
      .partitionBy(partitioner)

    if (!bf) {
      saxTsIdRdd
        .mapPartitions[RootClusterNodeR](x => RootClusterNodeR(x, bitStep, th))
        .saveAsObjectFile(path)

    } else {
      val isFeature = idxConfig.idxBfFeature

      val bfNodeRdd = saxTsIdRdd
        .mapPartitions[(BloomFilter[String], RootClusterNodeR)](x => bfCvt.cvtBfNode(x, bitStep, isFeature))
        .persist(StorageLevel.MEMORY_AND_DISK)

      bfNodeRdd
        .map(_._2)
        .saveAsObjectFile(path)

      bfNodeRdd
        .map { case (bf, node) => (TaskContext.get().partitionId, bf) }
        .saveAsObjectFile(bfPath)

      writeLog("==> Save Bloom Filter with feature to path: %s".format(bfPath))
    }

    writeLog("==> Finish repartition time series, save path: %s".format(path))
    this.saveTreeHex(sc)
  }


  private def readTsFromHDFS(tsFileName: String, idxCfg: IdxCfg): RDD[(String, (Array[Float], Long))] = {
    val tsRdd = sc.objectFile[(Long, Array[Float])](tsFileName)
    tsRdd.map { case (id, ts) => (Ops.cvtTsToIsaxHex(ts, idxCfg), (ts, id)) }
  }

  private def getHdfsFileNamesSampling(idxCfg: IdxCfg): String = {
    val samplePercent: Double = idxCfg.samplePercent
    val fileNamesList = Util.getHdfsFileNameList(idxConfig.tsFileName)

    val sampleFileNameList = Random.shuffle(fileNamesList).take(math.ceil(fileNamesList.size * samplePercent).toInt)
    sampleFileNameList.mkString(",")
  }

  private def writeLog(content: String) {
    Util.writeLog(content, true, idxConfig.logPath)
  }
}

object bfCvt extends Serializable with Logging {
  def cvt(values: Iterator[(Int, String)]): Iterator[(Int, Iterable[String])] = {
    val saxList = ListBuffer.empty[String]

    val (id, tp) = values.next()
    saxList += tp

    while (values.hasNext) {
      saxList += values.next()._2
    }

    List((id, saxList)).toIterator
  }

  def cvtBfNode(iter: Iterator[(String, (Array[Float], Long))], bitStep: Int, isFeature: Boolean): Iterator[(BloomFilter[String], RootClusterNodeR)] = {
    val datas = iter.toArray
    val bf = BloomFilter[String](datas.length, 0.1)
    val root = new RootClusterNodeR(1)

    if (isFeature) {
      for (elem <- datas) {
        bf.add(elem._1 + elem._2._1.max.toString + elem._2._1.min.toString)
        root.addRecord(elem, bitStep)
      }
    } else {
      for (elem <- datas) {
        bf.add(elem._1)
        root.addRecord(elem, bitStep)
      }
    }

    root.prueSingleElementLayer()
    Iterator((bf, root))
  }
}