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
  * Created by leon on 8/13/17.
  */

import org.apache.spark.edu.wpi.dsrg.tardis.configs.{IdxCfg, idxConfig}
import org.apache.spark.edu.wpi.dsrg.tardis.isax.NodeType._
import org.apache.spark.edu.wpi.dsrg.tardis.utils.Ops
import org.apache.spark.internal.Logging

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}

class TreeNodeHex extends Logging with Serializable {
  var saxHex: String = _
  var hrc: Int = _
  var rcdNbr: Long = _
  var nodeType: NodeType = UNDEF
  var pId = ListBuffer.empty[Int]
  private var ancestor: TreeNodeHex = null
  var descendants = HashMap.empty[String, TreeNodeHex]

  def this(noteType: NodeType) {
    this()
    require(noteType == ROOT, "noteType == ROOT")
    saxHex = ""
    hrc = getHrcFromSaxHex(saxHex)
    rcdNbr = 0
    nodeType = ROOT
  }

  def this(saxHex: String, rcdNbr: Long, ancestor: TreeNodeHex) {
    this()
    this.saxHex = saxHex
    this.rcdNbr = rcdNbr
    this.hrc = getHrcFromSaxHex(saxHex)

    this.nodeType = UNDEF
    this.ancestor = ancestor
    if (this.ancestor != null)
      require(this.hrc - 1 == this.ancestor.hrc, "this.hrc-1 == this.ancestor.hrc")
  }

  def this(values: (String, Long)) {
    this(values._1, values._2, null)
  }

  def this(values: (String, List[(String, Long)], Long)) {
    this()
    this.saxHex = values._1
    this.hrc = getHrcFromSaxHex(saxHex)
    this.rcdNbr = values._3

    if (values._2 != null && values._2.length != 0) {
      if (hrc == 0) this.nodeType = ROOT
      else this.nodeType = STEM

      for (elem <- values._2) {
        descendants += (elem._1 -> new TreeNodeHex(elem._1, elem._2, this))
      }
    } else {
      this.nodeType = LEAF
    }
  }

  private def getHrcFromSaxHex(saxHex: String): Int = {
    val length = saxHex.length
    require(length % idxConfig.bitStep == 0, "saxLength%idxConfig.bitStep==0")
    length / idxConfig.bitStep
  }

  /**
    * set PId for the node and all his descendants
    */
  def setPId(id: Int): Unit = {
    val tmpList = this.pId.toList

    if (!tmpList.contains(id)) {
      this.pId += id
      if (this.nodeType != ROOT) this.ancestor.setPId(id)
    }
  }

  def getPId(isax: String,bitStep:Int): Int = {
    def recur(n: TreeNodeHex): Int = {
      if (n.pId.size == 1 && n.nodeType == LEAF) n.pId(0)
      else {
        recur(n.descendants(Ops.cvtSAXtoSpecHrc(isax, n.hrc + 1,bitStep)))
      }
    }

    recur(this)
  }

  def getFatherNodePIdList(isax: String,bitStep:Int): List[Int] = {
    def recur(n: TreeNodeHex): List[Int] = {
      if (n.pId.size == 1 && n.nodeType == LEAF) {
        n.ancestor.pId.toList
      }
      else {
        recur(n.descendants(Ops.cvtSAXtoSpecHrc(isax, n.hrc + 1,bitStep)))
      }
    }

    recur(this)
  }

  def getPIdList(isax: String, kValue: Int,bitStep:Int): (Boolean, String, List[Int]) = {
    def recur(n: TreeNodeHex): (Boolean, String, List[Int]) = {
      if ((n.rcdNbr > kValue) && ((n.nodeType == ROOT) || (n.nodeType == STEM))) {
        try {
          recur(n.descendants(Ops.cvtSAXtoSpecHrc(isax, n.hrc + 1,bitStep)))
        } catch {
          case e: NoSuchElementException => (true, n.saxHex, n.pId.toList)
        }

      } else if ((n.rcdNbr < kValue) && ((n.nodeType == STEM) || (n.nodeType == LEAF))) {
        (false, n.ancestor.saxHex, n.ancestor.pId.toList)
      } else {
        (true, n.saxHex, n.pId.toList)
      }
    }

    recur(this)
  }

  def pruneNode(): Unit = {
    this.descendants = HashMap.empty[String, TreeNodeHex]
    if (this.nodeType != ROOT) this.nodeType = LEAF
  }

  def setNodeType(nodeType: NodeType): Unit = {
    this.nodeType = nodeType
  }

  def setAncestor(ancestor: TreeNodeHex): Unit = {
    this.ancestor = ancestor
  }

  def getRangePids(ts: Array[Float], distT: Double, idxCfg: IdxCfg): List[Int]={
    val holder = ListBuffer.empty[Int]
    if (this.descendants.size == 0){
        if (Ops.calPaaSaxLB(this.saxHex,ts,idxCfg) <= distT){
          for (pidElem <- this.pId)
            holder += pidElem
        }
    }else{
      for ((sax,node) <- this.descendants){
        if (Ops.calPaaSaxLB(sax,ts,idxCfg) <= distT)
          holder ++= node.getRangePids(ts,distT,idxCfg)
      }
    }
    holder.toList
  }

  override def toString(): String = {
    val acstr: String = if (this.ancestor == null) "N" else "Y_" + this.ancestor.saxHex + "_" + this.ancestor.hrc.toString

    "\n" + "\t" * hrc + nodeType.toString + "-iSAX:" + saxHex + "-Hrc:" + hrc.toString + "-RcdNbr:" + rcdNbr.toString + "-acstr:" + acstr + "-pID: " + pId.toList.mkString(".") + descendants.valuesIterator.mkString
  }

}
