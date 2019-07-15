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

package org.apache.spark.edu.wpi.dsrg.tardis.isax

/**
  * Created by leon on 8/15/17.
  */

import org.apache.spark.edu.wpi.dsrg.tardis.configs.{IdxCfg, idxConfig}
import org.apache.spark.edu.wpi.dsrg.tardis.utils.{Ops, Util}
import org.apache.spark.internal.Logging

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

class iSAXTreeHex(val bitStep: Int) extends Logging with Serializable {

  import NodeType._

  var root = new TreeNodeHex(ROOT)
  private var PId: Int = 0

  def loadCollection(data: List[List[(String, List[(String, Long)], Long)]]): Unit = {
    Util.writeLog("==> Begin generate iSAX Tree", true)
    for (line <- data) {
      for (elem <- line) {
        this.addTreeNodeHex(new TreeNodeHex(elem))
      }
    }
  }

  def loadCollectionSampling(data: List[List[(String, Long)]]): Unit = {
    Util.writeLog("==> Begin generate iSAX Tree using Sampling data", true)
    for (line <- data) {
      for (elem <- line) {
        val newElem = (elem._1, (elem._2 / idxConfig.samplePercent).toLong)
        this.addTreeNodeHexRobust(new TreeNodeHex(newElem))
      }
    }
  }

  private def addTreeNodeHexRobust(newNode: TreeNodeHex): Unit = {
    def recur(start: TreeNodeHex): Unit = {
      if (newNode.hrc == start.hrc + 1) {
        if (start.nodeType != ROOT) start.nodeType = STEM
        newNode.setAncestor(start)
        start.descendants += (newNode.saxHex -> newNode)
      } else {
        if (newNode.hrc > start.hrc) {
          try {
            recur(start.descendants(Ops.cvtSAXtoSpecHrc(newNode.saxHex, start.hrc + 1, bitStep)))
          } catch {
            case e: NoSuchElementException => {
              val tempNodeSaxHex = Ops.cvtSAXtoSpecHrc(newNode.saxHex, start.hrc + 1, bitStep)
              val tempNode = new TreeNodeHex((tempNodeSaxHex, newNode.rcdNbr))
              tempNode.setAncestor(start)
              tempNode.setNodeType(STEM)
              start.descendants += (tempNode.saxHex -> tempNode)
              recur(start.descendants(Ops.cvtSAXtoSpecHrc(newNode.saxHex, start.hrc + 1, bitStep)))
            }
          }
        } else {
          logError("add node error\tnewNode.hrc\t%d,start.hrc\t%d".format(newNode.hrc, start.hrc))
        }
      }
    }
    recur(root)
  }

  private def addTreeNodeHex(newNode: TreeNodeHex): Unit = {
    def recur(start: TreeNodeHex): Unit = {
      if (newNode.hrc == start.hrc + 1) {
        if (start.nodeType != ROOT)
          start.nodeType = STEM
        newNode.setAncestor(start)
        start.descendants += (newNode.saxHex -> newNode)
      } else {
        if (newNode.hrc == start.hrc) {
          root = newNode
        } else if (newNode.hrc > start.hrc) {
          recur(start.descendants(Ops.cvtSAXtoSpecHrc(newNode.saxHex, start.hrc + 1, bitStep)))
        } else {
          logError("newNode.hrc\t%d,start.hrc\t%d".format(newNode.hrc, start.hrc))
        }
      }
    }

    recur(root)
  }

  def assignPId(threshold: Long): Unit = {
    Util.writeLog("==> Begin assign partition ID for iSAX Tree", true)

    def recur(n: TreeNodeHex): Unit = {
      if (n.rcdNbr <= threshold) {
        n.setPId(PIdFactory())
        n.pruneNode()
      } else {
        val tlarge = n.descendants.filter((t) => t._2.rcdNbr > threshold)
        val tless = n.descendants.filter((t) => t._2.rcdNbr <= threshold)

        firstFitD(tless, threshold)

        for (k <- tlarge.keySet) {
          recur(n.descendants(k))
        }
      }
    }

    recur(root)
    Util.writeLog("==> Finish assign ID, partition number: %d".format(getPartitionNumber()), true)
  }

  def assignPIdSampling(threshold: Long): Unit = {
    Util.writeLog("==> Begin assign partition ID for iSAX Tree using Sampling", true)

    def recur(n: TreeNodeHex): Unit = {
      if ((n.rcdNbr <= threshold) && (n.nodeType != ROOT)) {
        n.setPId(PIdFactory())
        n.pruneNode()
      } else {
        val tlarge = n.descendants.filter((t) => t._2.rcdNbr > threshold)
        val tless = n.descendants.filter((t) => t._2.rcdNbr <= threshold)

        firstFitD(tless, threshold)

        for (k <- tlarge.keySet) {
          recur(n.descendants(k))
        }
      }
    }

    recur(root)
  }

  def getPId(isax: String): Int = {
    try{
      this.root.getPId(isax, this.bitStep)
    }catch {
      case e:Exception => this.PId
    }
  }
  def getFatherNodePIdList(isax: String): List[Int] = {
    try {
      this.root.getFatherNodePIdList(isax, this.bitStep)
    } catch {
      case e: Exception => List(this.PId)
    }
  }

  def getPIdList(isax: String, kValue: Int): (Boolean, String, List[Int]) = {
    this.root.getPIdList(isax, kValue, this.bitStep)
  }

  def getRangePids(ts: Array[Float], distT: Double,idxCfg: IdxCfg):List[Int]={
    root.getRangePids(ts, distT, idxCfg)
  }

  def getPartitionNumber(): Int = {
    this.PId //this.PIdFactory() already + 1
  }

  def getPartitionStatisInfo(): Unit = {
    logError("bug")
    val nodeIterator = root.descendants.valuesIterator
    var nodeSizeMap = mutable.HashMap.empty[Int, Long]
    while (nodeIterator.hasNext) {
      val elem = nodeIterator.next()
      if ((elem.nodeType != NodeType.ROOT) && (elem.nodeType != NodeType.STEM)) {
        val elemPID = elem.pId(0)
        val elemRcdNbr = elem.rcdNbr
        if (nodeSizeMap.contains(elemPID)) {
          nodeSizeMap(elemPID) += elemRcdNbr
        } else {
          nodeSizeMap += (elemPID -> elemRcdNbr)
        }
      }
    }

    val nodeValues = nodeSizeMap.values.toList

    val tp = nodeValues.sum.toInt
    if (tp != this.root.rcdNbr)
      logError("nodeSizeMap.values.sum: %d != this.root.rcdNbr: %d".format(tp, this.root.rcdNbr))

    val mean = Util.mean(nodeValues)
    val stddev = Util.stddev(nodeValues, mean)
    Util.writeLog("==> Partition size Mean: %.2f, Stddev: %.2f".format(mean, stddev))
  }

  override def toString: String = {
    root.toString()
  }

  private def PIdFactory(): Int = {
    this.PId += 1
    this.PId - 1
  }

  private def firstFitD(dataHash: HashMap[String, TreeNodeHex], binSize: Long): Unit = {
    var res: Int = 0
    var bin_rem = new Array[Long](dataHash.size)
    var bins = new ListBuffer[ListBuffer[String]]()

    val dataSortedMap = ListMap(dataHash.map((t) => (t._1 -> t._2.rcdNbr)).toSeq.sortWith(_._2 > _._2): _*)

    for ((iSAX, rcdNbr) <- dataSortedMap) {
      var j = 0
      var foundIt = false

      while (j < res && !foundIt) {
        if (bin_rem(j) >= rcdNbr) {
          bin_rem(j) = bin_rem(j) - rcdNbr
          bins(j) += iSAX
          foundIt = true
        }
        if (!foundIt) j += 1
      }

      if (j == res) {
        bin_rem(j) = binSize - rcdNbr
        bins += new ListBuffer[String]
        bins(j) += iSAX
        res += 1
      }
    }

    for (bin <- bins) {
      val tmpId = PIdFactory()
      for (isax <- bin) {
        try {
          dataHash(isax).setPId(tmpId)
          dataHash(isax).pruneNode()
        } catch {
          case e: NoSuchElementException => {
            logError("\nCan't find " + isax.toString + " in " + bin.toList)
            logError("==> " + dataSortedMap.mkString("; "))
            logError(bins.mkString("\n"))
            logError("\n\n\n\n")
          }
        }
      }
    }
  }
}