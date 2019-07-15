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

import java.util.{Calendar, NoSuchElementException}

import bloomfilter.mutable.BloomFilter
import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.tardis.configs.{eqConfig, idxConfig}
import org.apache.spark.edu.wpi.dsrg.tardis.isax.RootClusterNodeR
import org.apache.spark.edu.wpi.dsrg.tardis.utils._
import org.apache.spark.internal.Logging

import scala.collection.mutable.{HashSet, ListBuffer, Map}
import scala.util.Random


/**
  * Created by leon on 8/22/17.
  * Modified by Dongyu Zhang on 06/20/2019
  */

object ExecuteQuery extends Logging {
  def apply(sc: SparkContext): Unit = {
//    idxConfig.printCfg()
    eqConfig.printCfg()

    val eq = new ExecuteQuery(sc)

    if (eq.existIndexLabel())
      eq.dispatchQuery()
  }
}

class ExecuteQuery(sc: SparkContext) extends Logging with Serializable {
  val indexHex = new IndexHex(sc)
  val idxCfg = idxConfig.generateIdxCfg()
  val eqCfg = eqConfig.generateEqCfg()
  val bfe = BloomFilterRdd()

  def dispatchQuery(): Unit = {
    indexHex.readTreeHex(sc, eqCfg.eqIndexPath)

    eqCfg.eqQueryType match {
      case "exact" => {
        if (eqCfg.eqExactBF)
          executeExactQueryBF()
        else
          executeExactQuery()
      }
      case "knn" | "knn-approx"=> executeKNNQuery(eqCfg.eqKnnType)
      case _ =>println("Only support exact and knn query")
    }
  }

  /**
    * Exact matching query part
    */
  def queryOneTsTestBfFromHdfs(ts: Array[Float], bitStep: Int): (Boolean, Int) = {
    val tsSax = Ops.cvtTsToIsaxHex(ts, idxCfg)

    val pId = try {
      indexHex.fetchPId(tsSax)
    }
    catch {
      case e: NoSuchElementException => {
        logError(tsSax + " : queryOneTsExact : " + e.toString)
        return (false, 0)
      }
    }

    val tsSaxKey = if (eqCfg.eqExactBfFeature) tsSax + ts.max.toString + ts.min.toString else tsSax

    val bfPath = testConvertBfPIdToPath(pId)
    val bfResult = sc.objectFile[(Int, BloomFilter[String])](bfPath).map{case (id,bf) => bf.mightContain(tsSaxKey)}.collect()(0)

    val foundIt =
      if (bfResult) {
//        logError("-- execute -- bfe true")
        val tsHdfsPath = convertPIdToPath(pId)
        val includeTrues = sc.objectFile[RootClusterNodeR](tsHdfsPath)
          .map(x => x.getExactMatching(tsSax, bitStep, ts))
          .collect()
//        logError("queryOneTsExactBF result nbr: %d".format(includeTrues.length))

        (includeTrues(0), 1)
      } else {
        logError("-- execute -- bfe false")
        (false, 0)
      } //bloom filter guarantee query string doesn't exist

    foundIt
  }


  def testConvertBfPIdToPath(pId: Int): String = {
    require(pId >= 0 && pId < 100000, "pId: %d>= 0 && pId < 100000".format(pId))
    eqCfg.eqBFPath + "/part-" + Util.genZero(5 - pId.toString.length) + pId.toString
  }

  def executeExactQueryBF(): Unit = {
    bfe.readAndPersistRdd(sc, eqCfg.eqBFPath)
    writeLog("==> Read Bloom Filter, feature: %b".format(eqCfg.eqExactBfFeature))

    val tsRcds = fetchExactTsAndLabel(eqCfg.eqExcatNbr)
    var correctNbr = 0
    var accessBlockNbr = 0
    val startTime = Calendar.getInstance().getTime().getTime
    val bitStep = idxConfig.bitStep

    writeLog("==> Start Exact Matching with Bloom Filter")
    for ((ts, isExist) <- tsRcds) {
      val foundIt = queryOneTsExactBF(ts, bitStep)
      accessBlockNbr += foundIt._2
      if (foundIt._1 == isExist)
        correctNbr += 1
    }

    val duration = Calendar.getInstance().getTime().getTime - startTime
    Evaluate.getExact(tsRcds.size, correctNbr, accessBlockNbr, duration, eqCfg.eqExactInclude)
  }

  private def queryOneTsExactBF(ts: Array[Float], bitStep: Int): (Boolean, Int) = {
    val tsSax = Ops.cvtTsToIsaxHex(ts, idxCfg) //Hrc may need change

    val pId = try {
      indexHex.fetchPId(tsSax)
    }
    catch {
      case e: NoSuchElementException => {
        logError(tsSax + " : queryOneTsExact : " + e.toString)
        return (false, 0)
      }
    }

    val tsSaxKey = if (eqCfg.eqExactBfFeature) tsSax + ts.max.toString + ts.min.toString else tsSax

    val foundIt =
      if (bfe.querySax(tsSaxKey, pId)) {
//        logError("-- execute -- bfe true")
        val tsHdfsPath = convertPIdToPath(pId)
        val includeTrues = sc.objectFile[RootClusterNodeR](tsHdfsPath)
          .map(x => x.getExactMatching(tsSax, bitStep, ts))
          .collect()
//        logError("queryOneTsExactBF result nbr: %d".format(includeTrues.length))

        (includeTrues(0), 1)
      } else {
        logError("-- execute -- bfe false")
        (false, 0)
      } //bloom filter guarantee query string doesn't exist

    foundIt
  }

  def executeExactQuery(): Unit = {
    val tsRcds = fetchExactTsAndLabel(eqCfg.eqExcatNbr)
    var correctNbr = 0
    var accessBlockNbr = 0
    val startTime = Calendar.getInstance().getTime().getTime
    val bitStep = idxConfig.bitStep

    writeLog("==> Start Exact Matching without Bloom Filter")
    for ((ts, isExist) <- tsRcds) {
      val foundIt = queryOneTsExact(ts, bitStep)
      accessBlockNbr += foundIt._2
      if (foundIt._1 == isExist)
        correctNbr += 1
    }

    val duration = Calendar.getInstance().getTime().getTime - startTime
    Evaluate.getExact(tsRcds.size, correctNbr, accessBlockNbr, duration, eqCfg.eqExactInclude)
  }

  private def queryOneTsExact(ts: Array[Float], bitStep: Int): (Boolean, Int) = {
    val tsSax = Ops.cvtTsToIsaxHex(ts, idxCfg)

    val pId = try {
      indexHex.fetchPId(tsSax)
    }
    catch {
      case e: NoSuchElementException => {
        logError(tsSax + " : queryOneTsExact : " + e.toString)
        return (false, 0)
      }
    }

    /**
      * val foundIt = {
      * val tsHdfsPath = convertPIdToPath(pId)
      * val tsLists = sc.objectFile[RootClusterNodeR](tsHdfsPath)
      * .map(x => x.fetchTerminalRecords(tsSax,bitStep))
      * .collect()(0)
      * *
      * if (tsLists == List()){
      * (false,1)
      * }else{
      * val tsList = tsLists(0)._2
      * if (tsList.length > 0) {
      * val compareList = tsList.map { case (tsB, id) => tsB.sameElements(ts) }
      * logError("queryOneTsExact: %s".format(compareList.mkString("-")))
      * if (compareList.contains(true))(true,1) else (false,1)
      * } else (false,1)
      * }
      * }
      */

    val foundIt = {
      val tsHdfsPath = convertPIdToPath(pId)
      val includeTrues = sc.objectFile[RootClusterNodeR](tsHdfsPath)
        .map(x => x.getExactMatching(tsSax, bitStep, ts))
        .collect()
      logError("queryOneTsExactBF result nbr: %d".format(includeTrues.length))
      includeTrues(0)
    }

    (foundIt, 1)
  }

  def fetchExactTsAndLabel(nbr: Int): List[(Array[Float], Boolean)] = {
    //    writeLog("==> Generate label for exacting matching")
    //    clCfg.printCfg()
    //    val cl = new CreateLabel(sc, "")
    //    cl.generateExactLables(eqCfg.eqExactInclude, eqCfg.eqExcatNbr)

    sc.objectFile[(Array[Float], Boolean)](eqCfg.eqExactLabelPath).collect().toList
  }

  /**
    * Knn query
    */

  def executeKNNQuery(knnType:Int): Unit = {
    require(knnType == 0 || knnType == 1|| knnType == 2,"knnType only support 0,1,2")
    val tsRcds = fetchKnnTsAndIds(eqCfg.eqKnnNbr, eqCfg.eqKnnK)
    val result = ListBuffer.empty[(List[(Double, Long)], List[(Double, Long)])]

    val startTime = Calendar.getInstance().getTime().getTime
    val bitStep = idxConfig.bitStep
    val k = eqCfg.eqKnnK

    writeLog("==> Start %dNN-approx Query".format(eqCfg.eqKnnK))
    for ((ts, ground_truth_List) <- tsRcds) {
      val query_result_List = knnType match {
        case 0 => queryOneTsKnn1Nd(ts, bitStep, k)
        case 1 => queryOneTsKnn1NdWhole(ts, bitStep, k)
        case 2 => queryOneTsKnnMNdWhole(ts, bitStep, k)
      }

      if (query_result_List.size == k) {
        result += ((query_result_List,ground_truth_List))
      } else {
        writeLog("==> skip one record due to no enough k number")
      }
    }

    val duration = Calendar.getInstance().getTime().getTime - startTime

    Evaluate.getKnnRecallErrorRatio(result.toList, duration)
  }

  private def queryOneTsKnn1Nd(ts: Array[Float], bitStep: Int, k: Int): List[(Double, Long)] = {
    if (ts.length == idxCfg.tsLength){
      val tsSax = Ops.cvtTsToIsaxHex(ts, idxCfg)

      val pid = indexHex.fetchPId(tsSax)
      val pidPath = convertPIdToPath(pid)
      val tsRdd = sc.objectFile[RootClusterNodeR](pidPath)

      val result = tsRdd.map(x => x.calTopKRecords(ts, tsSax, bitStep, k)).collect().toList
      result(0)
    } else {
      require(ts.length == 2*idxCfg.tsLength)
      val tsSaxArray = Ops.cvtTsULToIsaxHexArray(ts, idxCfg, eqCfg)
//      writeLog("==> the tsSAXArray Length is: %d".format(tsSaxArray.length))
//      if (tsSaxArray.length > eqCfg.eqMaxSAXNbr){
//        tsSaxArray = tsSaxArray.take(eqCfg.eqMaxSAXNbr)
//      }
      val tsRddArray = tsSaxArray.map(tsSax => indexHex.fetchPId(tsSax))
        .map(pid => convertPIdToPath(pid))
        .map(pidPath => sc.objectFile[RootClusterNodeR](pidPath))
      val tsSaxRdd = (tsRddArray zip tsSaxArray).map{case (tsRdd, tsSax) => tsRdd.map(x => (x, tsSax))}.reduce(_ union _)
      var finalResult: List[(Double, Long)] = Nil
      try{
        finalResult = tsSaxRdd.map{case (x, tsSax)=> x.calTopKRecords(ts, tsSax, bitStep, k)}.collect().toList.reduce(_ ++ _).distinct.sortWith(_._1 < _._1)
        if (finalResult.head._1 == Double.PositiveInfinity) throw new RuntimeException ("Did not collect result due to some exception")
      } catch{
        case e: Exception => {
          writeLog("==> Did not receive result due to some exception, try for loop now")
          val Result = ListBuffer.empty[(Double, Long)]
          try {
            for (i <- tsSaxArray.indices) {
//              writeLog("==> the %d tsSAX is processed".format(i))
              var result = List((Double.PositiveInfinity, -1L))
              try {
                result = tsRddArray(i).map(x => x.calTopKRecords(ts, tsSaxArray(i), bitStep, k)).collect().toList.head
              } catch{
                case e: Exception => Unit
//                case e: Exception => writeLog("==> The %d tsSAX did not receive result due to exception: %s".format(i, e.toString))
              } finally {
                Result ++= result

              }
            }
            finalResult = Result.toList.distinct.sortWith(_._1 < _._1)
            if (finalResult.head._1 == Double.PositiveInfinity) {
              finalResult = Nil
              throw new RuntimeException ("Did not collect result due to some exception")
            }
          } catch{
//            case e: Exception => Unit
            case e: Exception => writeLog("==> Did not receive result due to exception: %s".format(e.toString))
          }

        }
      }
      if (finalResult.size > k) finalResult.take(k)
      else finalResult
    }
  }

  private def queryOneTsKnn1NdWhole(ts: Array[Float], bitStep: Int, k: Int): List[(Double, Long)] = {
    if (ts.length == idxCfg.tsLength){
      val tsSax = Ops.cvtTsToIsaxHex(ts, idxCfg)

      val pid = indexHex.fetchPId(tsSax)
      val pidPath = convertPIdToPath(pid)
      val tsRdd = sc.objectFile[RootClusterNodeR](pidPath)

      val ifg = idxCfg
      val result = tsRdd.map(x => x.calTopKRecords1NdWhole(ts, tsSax, bitStep, k,ifg)).collect().toList
      result(0)
    }else{
      require(ts.length == 2*idxCfg.tsLength)
      val tsSaxArray = Ops.cvtTsULToIsaxHexArray(ts, idxCfg, eqCfg)
//      writeLog("==> the tsSAXArray Length is: %d".format(tsSaxArray.length))
//      if (tsSaxArray.length > eqCfg.eqMaxSAXNbr){
//        tsSaxArray = tsSaxArray.take(eqCfg.eqMaxSAXNbr)
//      }
      val tsRddArray = tsSaxArray.map(tsSax => indexHex.fetchPId(tsSax))
        .map(pid => convertPIdToPath(pid))
        .map(pidPath => sc.objectFile[RootClusterNodeR](pidPath))
      val tsSaxRdd = (tsRddArray zip tsSaxArray).map{case (tsRdd, tsSax) => tsRdd.map(x => (x, tsSax))}.reduce(_ union _)
      var finalResult: List[(Double, Long)] = Nil
      val ifg = idxCfg
      try{
        finalResult = tsSaxRdd.map{case (x, tsSax)=> x.calTopKRecords1NdWhole(ts, tsSax, bitStep, k, ifg)}.collect().toList.reduce(_ ++ _).distinct.sortWith(_._1 < _._1)
        if (finalResult.head._1 == Double.PositiveInfinity) throw new RuntimeException ("Did not collect result due to some exception")
      } catch{
        case e: Exception => {
          writeLog("==> Did not receive result due to some exception, try for loop now")
          val Result = ListBuffer.empty[(Double, Long)]
          try {
            for (i <- tsSaxArray.indices) {
//              writeLog("==> the %d tsSAX is processed".format(i))
              var result = List((Double.PositiveInfinity, -1L))
              try {
                result = tsRddArray(i).map(x => x.calTopKRecords1NdWhole(ts, tsSaxArray(i), bitStep, k, ifg)).collect().toList.head
              } catch{
                case e: Exception => Unit
//                case e: Exception => writeLog("==> The %d tsSAX did not receive result due to exception: %s".format(i, e.toString))
              } finally {
                Result ++= result
              }
            }
            finalResult = Result.toList.distinct.sortWith(_._1 < _._1)
            if (finalResult.sortWith(_._1 < _._1).head._1 == Double.PositiveInfinity){
              finalResult = Nil
              throw new RuntimeException ("Did not collect result due to some exception")
            }
          } catch{
//            case e: Exception => Unit
            case e: Exception => writeLog("==> Did not receive result due to exception: %s".format(e.toString))
          }
        }
      }
      if (finalResult.size > k) finalResult.take(k)
      else finalResult
    }

  }

  private def queryOneTsKnnMNdWhole(ts: Array[Float], bitStep: Int, k: Int): List[(Double, Long)] = {
    if (ts.length == idxCfg.tsLength){
      val tsSax = Ops.cvtTsToIsaxHex(ts, idxCfg)

      val pid = indexHex.fetchPId(tsSax)
      val pidPath = convertPIdToPath(pid)
      val tsRdd = sc.objectFile[RootClusterNodeR](pidPath)
      val threshold = tsRdd.map(x => x.calTopKRecordsMNdThreshold(ts, tsSax, bitStep, k)).collect().last

      var pidList = indexHex.fetchFatherNodePIdList(tsSax)

      if (pidList.size > eqCfg.eqKnnMax){
        if (eqCfg.eqKnnMin == 1){
          pidList = List(pid)
        }else{
          pidList = (pid :: pidList.take(eqCfg.eqKnnMax)).distinct
        }
      }

      val fatherNodepath = pidList.map(pid => convertPIdToPath(pid)).mkString(",")
      val tsFatherRdd = sc.objectFile[RootClusterNodeR](fatherNodepath)
      val ifg = idxCfg
      val rangeValue = tsFatherRdd.map(x=>x.fetchRangDistId(ts,threshold,ifg)).collect().toList.flatten
      if (rangeValue.size >= k){
        rangeValue
          .sortWith(_._1 < _._1)
          .take(k)
      }else{
        rangeValue
          .sortWith(_._1 < _._1)
      }
    } else{
      require(ts.length == 2*idxCfg.tsLength)
      val tsSaxArray = Ops.cvtTsULToIsaxHexArray(ts, idxCfg, eqCfg)
//      writeLog("==> the tsSAXArray Length is: %d".format(tsSaxArray.length))
//      if (tsSaxArray.length > eqCfg.eqMaxSAXNbr){
//        tsSaxArray = tsSaxArray.take(eqCfg.eqMaxSAXNbr)
//      }
      val pidArray = tsSaxArray.map(tsSax => indexHex.fetchPId(tsSax))
      val tsRddArray = pidArray.map(pid => convertPIdToPath(pid))
        .map(pidPath => sc.objectFile[RootClusterNodeR](pidPath))
      val tsSaxRdd = (tsRddArray zip tsSaxArray).map{case (tsRdd, tsSax) => tsRdd.map(x => (x, tsSax))}.reduce(_ union _)
      var saxThresList: List[(String, Double)] = Nil
      var saxPidList: List[(String, List[Int])] = Nil
      val pidThresMap: Map[Int, Double] = Map()
      val saxThresMap: Map[String, Double] = Map()
      try{
        saxThresList = tsSaxRdd.map{case (x, tsSax)=> (tsSax, x.calTopKRecordsMNdThreshold(ts, tsSax, bitStep, k))}.collect().toList

        for((tsSax, threshold) <- saxThresList){
          if(saxThresMap.contains(tsSax)) saxThresMap(tsSax) = threshold
          else saxThresMap += (tsSax -> threshold)
        }
        saxPidList = tsSaxArray.map(tsSax => (tsSax, indexHex.fetchFatherNodePIdList(tsSax))).toList
        for((tsSax, pidList) <- saxPidList){
          val threshold = saxThresMap(tsSax)
          val oPid = indexHex.fetchPId(tsSax)
          var newPidList = pidList
          if (pidList.size > eqCfg.eqKnnMax){
            if (eqCfg.eqKnnMin == 1){
              newPidList = List(oPid)
            }else{
              newPidList = (oPid :: pidList.take(eqCfg.eqKnnMax)).distinct
            }
          }
          for(pid <- newPidList){
            if(pidThresMap.contains(pid)) {
              if(pidThresMap(pid) > threshold){
                pidThresMap(pid) = threshold
              }
            }else{
              pidThresMap += (pid -> threshold)
            }
          }
        }
      } catch{
        case e: Exception => {
          writeLog("==> Did not receive thresholds due to some exception, try for loop now")
          try {
            for (i <- tsSaxArray.indices) {
//              writeLog("==> the %d tsSAX is processed".format(i))
              try {
                val threshold = tsRddArray(i).map(x => x.calTopKRecordsMNdThreshold(ts, tsSaxArray(i), bitStep, k)).collect().last
                require(threshold != Double.PositiveInfinity)
                var pidList = indexHex.fetchFatherNodePIdList(tsSaxArray(i))
                if (pidList.size > eqCfg.eqKnnMax){
                  if (eqCfg.eqKnnMin == 1){
                    pidList = List(pidArray(i))
                  }else{
                    pidList = (pidArray(i) :: pidList.take(eqCfg.eqKnnMax)).distinct
                  }
                }
                for(pid <- pidList){
                  if(pidThresMap.contains(pid)){
                    if(pidThresMap(pid) > threshold){
                      pidThresMap(pid) = threshold
                    }
                  }else{
                    pidThresMap += (pid -> threshold)
                  }
                }

              } catch{
                case e: Exception => Unit
//                case e: Exception => writeLog("==> The %d tsSAX did not receive result due to exception: %s".format(i, e.toString))
              }
            }
            if (pidThresMap.isEmpty) throw new RuntimeException ("Did not collect pid due to some exception")
          } catch{
//            case e: Exception => Unit
            case e: Exception => writeLog("==> Did not receive thresholds due to exception: %s".format(e.toString))
          }
        }
      }
      var finalPidList = pidThresMap.keys.toList
      if (finalPidList.size > eqCfg.eqKnnMax){
        Random.setSeed(eqCfg.eqSeed)
        if (eqCfg.eqKnnMin == 1){
          finalPidList = Random.shuffle(pidArray.toList).take(1)
        }else{
          finalPidList = (pidArray.toList ++ Random.shuffle(finalPidList).take(eqCfg.eqKnnMax)).distinct
        }
      }
      val fatherNodePathList = finalPidList.map(pid => convertPIdToPath(pid))
      val tsFatherRddList = fatherNodePathList.map(path => sc.objectFile[RootClusterNodeR](path))
      var finalResult: List[(Double, Long)] = Nil
      if (finalPidList.isEmpty){
        finalResult
      } else{
        val ifg = idxCfg
        try{
          val tsFatherThresRdd = (tsFatherRddList zip finalPidList).map{case (tsRdd, pid) => tsRdd.map(x => (x, pidThresMap(pid)))}.reduce(_ union _)
          finalResult = tsFatherThresRdd.map{case(x, threshold) => x.fetchRangDistId(ts, threshold, ifg)}.collect().toList.flatten.distinct.sortWith(_._1 < _._1)
        } catch{
          case e: Exception =>{
            writeLog("==> Did not receive result due to some exception, try for loop now")
            val rangeValueResult = ListBuffer.empty[(Double, Long)]
            try{
              for(i <- tsFatherRddList.indices){
                var rangeValue = List((Double.PositiveInfinity, -1L))
                try{
                  val threshold = pidThresMap(finalPidList(i))

                  rangeValue = tsFatherRddList(i).map(x=>x.fetchRangDistId(ts,threshold,ifg)).collect().toList.flatten
                  if (rangeValue.size >= k){
                    rangeValue
                      .sortWith(_._1 < _._1)
                      .take(k)
                  }else{
                    rangeValue
                      .sortWith(_._1 < _._1)
                  }
                } catch {
                  case e: Exception => Unit
                  //                case e: Exception => writeLog("==> The %d tsSAX did not receive result due to exception: %s".format(i, e.toString))
                } finally {
                  rangeValueResult ++= rangeValue
                }
              }
              finalResult = rangeValueResult.toList.distinct.sortWith(_._1 < _._1)
              if (finalResult.head._1 == Double.PositiveInfinity) throw new RuntimeException ("Did not collect result due to some exception")
            } catch{
              //            case e: Exception => Unit
              case e: Exception => writeLog("==> Did not receive result due to exception: %s".format(e.toString))
            }
          }
        }
        if (finalResult.size > k) finalResult.take(k)
        else finalResult
      }
    }
  }


  def fetchKnnTsAndIds(nbr: Int, topK: Int): List[(Array[Float], List[(Double, Long)])] = {
//    writeLog("==> Retrieve KNN records and labels from %s".format(eqCfg.eqKnnLabelPath))

    val tsRcds = sc.objectFile[(Array[Float], List[(Double, Long)])](eqCfg.eqKnnLabelPath).collect()

    tsRcds.take(nbr)
      .map { case (qTs, values) => (qTs, values.take(topK)) }.toList
  }

  /**
    * Range query part
    */

  def executeRangeQuery(): Unit = {
    val tsRcds = fetchRangeTsAndLabel(eqCfg.eqRangeNbr, eqCfg.eqDistance)

    val result = ListBuffer.empty[(List[Long], List[Long], Int)]

    val startTime = Calendar.getInstance().getTime().getTime

    writeLog("==> Start Range Query: %d".format(tsRcds.length))
    for ((ts, cRange) <- tsRcds) {
      val (blkNbr, gRange) = queryOneTsRang(ts, eqCfg.eqDistance)
      writeLog(("==> Result Nbr:%9d, " +
        "\tCorrect Nbr:%9d, " +
        "\tBlock Nbr:%9d").format(gRange.size, cRange.size, blkNbr))
      result += ((cRange, gRange, blkNbr))
    }

    val duration = Calendar.getInstance().getTime().getTime - startTime
    Evaluate.getRange(result.toList, duration)
  }

  def queryOneTsRang(ts: Array[Float], distT: Double): (Int, List[Long]) = {
    val pidList = this.indexHex.getRangePids(ts, distT, idxCfg)

    val path = pidList.map(pid => convertPIdToPath(pid)).mkString(",")
    val tsRdd = sc.objectFile[RootClusterNodeR](path)

    val ifg = idxCfg
    val result = tsRdd.map(x => x.fetchRangId(ts, distT, ifg)).collect().toList.flatten
    (pidList.size, result)
  }

  def executeRangeQueryWithNodeNbr(): Unit = {
    val tsRcds = fetchRangeTsAndLabel(eqCfg.eqRangeNbr, eqCfg.eqDistance)

    val result = ListBuffer.empty[(List[Long], List[Long], Int, Int, Int)]

    val startTime = Calendar.getInstance().getTime().getTime

    writeLog("==> Start Range Query with node number statistic: %d".format(tsRcds.length))
    for ((ts, cRange) <- tsRcds) {
      val (blkNbr, gRange, interNdNbr, termNdNbr) = queryOneTsRangWithNodeNbr(ts, eqCfg.eqDistance)
      writeLog(("==> Result Nbr:%9d, " +
        "\tCorrect Nbr:%9d, " +
        "\tBlock Nbr:%9d, " +
        "\tInternal Node Nbr:%9d, " +
        "\tTerminal Node Nbr:%9d").format(gRange.size, cRange.size, blkNbr, interNdNbr, termNdNbr))
      result += ((cRange, gRange, blkNbr, interNdNbr, termNdNbr))
    }

    val duration = Calendar.getInstance().getTime().getTime - startTime
    Evaluate.getRangeWithNodeNbr(result.toList, duration)
  }

  def queryOneTsRangWithNodeNbr(ts: Array[Float], distT: Double): (Int, List[Long], Int, Int) = {
    val pidList = this.indexHex.getRangePids(ts, distT, idxCfg)

    val path = pidList.map(pid => convertPIdToPath(pid)).mkString(",")
    val tsRdd = sc.objectFile[RootClusterNodeR](path)

    val ifg = idxCfg
    val result = tsRdd.map(x => x.fetchRangIdWithNodeNbr(ts, distT, ifg)).collect().toList

    var idList = ListBuffer.empty[Long]
    var iNdNbr = 0
    var tNdNbr = 0
    for (r <- result) {
      idList ++= r._1
      iNdNbr += r._2._1
      tNdNbr += r._2._2
    }

    (pidList.size, idList.toList, iNdNbr, tNdNbr)
  }

  def fetchRangeTsAndLabel(nbr: Int, distance: Double): List[(Array[Float], List[Long])] = {
    writeLog("==> Retrieve Range Query records and labels from %s".format(eqCfg.eqRangeLabelPath))

    val tsRcds = sc.objectFile[(Array[Float], List[(Double, Long)])](eqCfg.eqRangeLabelPath).collect()

    val result = tsRcds
      .map { case (qTs, values) => (qTs, values.filter(_._1 < distance).map(_._2)) }
      .filter { case (x, y) => (y.length != 0) }

    val finalResult = if (result.length >= nbr) {
      result.take(nbr)
    } else {
      result
    }
    writeLog(("==> fetchRangeTsAndLabel after filter using distance:%.0f " +
      "\n * actual nbr\t%4d " +
      "\n * require nbr\t%4d " +
      "\n * total nbr\t%4d").format(distance, finalResult.length, nbr, tsRcds.length))
    finalResult.toList
  }

  private def convertPIdToPath(pId: Int): String = {
    require(pId >= 0 && pId < 100000, "pId: %d>= 0 && pId < 100000".format(pId))
    eqCfg.eqRepTsPath + "/part-" + Util.genZero(5 - pId.toString.length) + pId.toString
  }

  private def convertStrToSaxPid(str: String): (String, Int) = {
    val content = str.split("-")
    require(content.length > 1, "%s".format(content.mkString("=")))
    val pid =
      if (content(0) != "LEAF")
        -1
      else
        content.last.split(": ").last.toInt

    val sax = content(1).split(":").last
    (sax, pid)
  }

  private def existIndexLabel(): Boolean = {
    val indexPath = eqCfg.eqIndexPath
    val labelPath = eqConfig.fetchLabelHdfsName()
    val tsPath = eqCfg.eqRepTsPath

    val indexExists = Util.hdfsDirExists(indexPath)
    val labelExists = if (labelPath == "exactLabel") true else Util.hdfsDirExists(labelPath)
    val tsExists = Util.hdfsDirExists(tsPath)

    if (!indexExists) writeLog("==> Can't find Index: %s".format(indexPath))
    if (!labelExists) writeLog("==> Can't find Label: %s".format(labelPath))
    if (!tsExists) writeLog("==> Can't find repartition time series: %s".format(tsPath))

    indexExists && labelExists && tsExists
  }

  private def writeLog(content: String): Unit = {
    Util.writeLog(content, true, eqConfig.logPath)
  }
}

object inCode extends Serializable {
  def contain(nodeHashSet: HashSet[String], sax: String): Boolean = {
    val lenth = nodeHashSet.last.length
    nodeHashSet.contains(sax.take(lenth))
  }
}
