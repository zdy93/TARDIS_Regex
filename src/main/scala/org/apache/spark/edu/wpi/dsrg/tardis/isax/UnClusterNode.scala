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

import org.apache.spark.internal.Logging

import scala.collection.mutable.{HashMap, ListBuffer}

/**
  * Created by leon on 10/17/17.
  */
class AbstractUnclusterNode extends Logging with Serializable {
  var saxRep: String = _
  var rcdNbr: Long = 0
  var ancestor: AbstractUnclusterNode = _
  var descendants = HashMap.empty[String, AbstractUnclusterNode]

  def calHrc(bitStep: Int = 1): Int = {
    if (saxRep == "Root")
      0
    else
      saxRep.length / bitStep
  }

  def setAncestor(ancestor: AbstractUnclusterNode): Unit = {
    this.ancestor = ancestor
  }

  def fetchSaxRep(): String = {
    saxRep
  }

  def fetchRcdNbr(): Long = {
    rcdNbr
  }

  def fetchAncestor(): AbstractUnclusterNode = {
    ancestor
  }

  def addRecord(elem: (String, Long), bitStep: Int): Unit = {
    this.rcdNbr += 1

    if (this.calHrc(bitStep) + 1 == (elem._1.length / bitStep)) {
      val teminalSaxRep = elem._1

      if (!this.descendants.contains(teminalSaxRep)){
        val terminalNodeR = new TerminalUnclusterNode(elem._1)
        terminalNodeR.setAncestor(this)
        this.descendants += (teminalSaxRep -> terminalNodeR)
      }

      this.descendants(teminalSaxRep).addRecord(elem,bitStep)

    } else if (this.calHrc(bitStep) + 1 < (elem._1.length / bitStep)) {
      val internalSaxRep = elem._1.take((this.calHrc(bitStep) + 1) * bitStep)

      if (!this.descendants.contains(internalSaxRep)) {
        val internalNodeR = new InternalUnclusterNode(internalSaxRep)
        internalNodeR.setAncestor(this)
        this.descendants += (internalSaxRep -> internalNodeR)
      }

      this.descendants(internalSaxRep).addRecord(elem, bitStep)
    } else {
      logError("Error Add")
    }
  }

  def shrinkNodeLayer(th:Int): Unit ={
    for((sax,node) <- this.descendants){
      if (node.isInstanceOf[InternalUnclusterNode]){
        if (node.rcdNbr <= th){

          val newTerminalNode = new TerminalUnclusterNode(sax)
          newTerminalNode.setAncestor(this)
          newTerminalNode.setContent(node.fetchAllRecords())

          this.descendants(sax) = newTerminalNode
        }else{
          this.descendants(sax).shrinkNodeLayer(th)
        }
      }
    }
  }

  def fetchTerminalRecords(sax:String,bitStep:Int): List[(String, Long)] ={
    require(sax.length >= bitStep, "sax.length >= bitStep")

    try{
      if (saxRep == "Root"){
        val startLayer = this.asInstanceOf[RootUnclusterNode].startLayer
        this.descendants(sax.take(bitStep*startLayer)).fetchTerminalRecords(sax,bitStep)
      }else{
        val len = saxRep.length
        if (saxRep == sax) {
          if (this.isInstanceOf[TerminalUnclusterNode]){
            this.fetchTerminalRecords(sax,bitStep)
          }else{
            this.fetchAllRecords()
          }
        }else{
          this.descendants(sax.take(len + bitStep)).fetchTerminalRecords(sax,bitStep)
        }
      }
    }catch {
      case e:NoSuchElementException => {
        logError(e.toString + " in Node: %s fetchTerminalRecords".format(this.saxRep))
        List()
      }
    }
  }

  def fetchAllRecords(): List[(String, Long)] = {
    if (this.isInstanceOf[RootUnclusterNode] || this.isInstanceOf[InternalUnclusterNode]){
      val result = ListBuffer.empty[(String,Long)]

      val iter = this.descendants.toIterator
      while(iter.hasNext){
        result ++= iter.next()._2.fetchAllRecords()
      }
      result.toList
    }else{
      this.fetchAllRecords()
    }
  }

  def getNodeNbr(): (Int,Int) ={
    var terminalNbr = 0
    var interalNbr = if (this.isInstanceOf[InternalUnclusterNode]){1} else {0}

    for ((k,node)<-this.descendants){
      var (i,t) = node.getNodeNbr()
      interalNbr += i
      terminalNbr += t
    }

    (interalNbr,terminalNbr)
  }

  override def toString(): String = {
    val classType = this.getClass.toString.split('.').last match {
      case "InternalUnclusterNode" => "I";
      case "TerminalUnclusterNode" => "T";
      case "RootUnclusterNode" => "R";
    }
    "\n|" + "-" * calHrc() + classType + " sax:" + saxRep + " nbr:" + this.rcdNbr.toString + descendants.valuesIterator.mkString
  }
}

object RootUnclusterNode extends Logging with Serializable {
  def apply(datas: Iterator[(String, Long)], bitStep: Int,th:Int): Iterator[RootUnclusterNode] = {
    val root = new RootUnclusterNode(1)
    root.build(datas, bitStep, th)
  }
}

class RootUnclusterNode extends AbstractUnclusterNode {
  var startLayer:Int = 1;

  def this(f: Int) {
    this()
    saxRep = "Root"
    ancestor = this
    startLayer = 1
    descendants = HashMap.empty[String, AbstractUnclusterNode]
  }

  def build(datas: Iterator[(String, Long)], bitStep: Int, th:Int): Iterator[RootUnclusterNode] = {
    for (elem <- datas) {
      this.addRecord(elem, bitStep)
    }
    this.prueSingleElementLayer()
    this.shrinkNodeLayer(th)
    Iterator(this)
  }

  def prueSingleElementLayer(): Unit = {
    var tempNodeR: AbstractUnclusterNode = this

    while (tempNodeR.descendants.size == 1) {
      tempNodeR = tempNodeR.descendants.last._2
      startLayer += 1
    }
    this.descendants = tempNodeR.descendants

    this.descendants.foreach(_._2.setAncestor(this))
  }

}

class InternalUnclusterNode extends AbstractUnclusterNode {
  def this(sax: String) {
    this()
    saxRep = sax
    descendants = HashMap.empty[String, AbstractUnclusterNode]
  }
}


class TerminalUnclusterNode extends AbstractUnclusterNode {
  val content = ListBuffer.empty[(String, Long)]

  def this(sax: String){
    this()
    this.saxRep = sax
    rcdNbr = 0
  }

  override def addRecord(elem: (String, Long), bitStep: Int): Unit ={
    content += elem
    rcdNbr += 1
  }

  def setContent(records: List[(String,Long)]): Unit ={
    rcdNbr = 0

    for (elem <- records){
      content += elem
      rcdNbr += 1
    }
  }

  override def fetchAllRecords(): List[(String, Long)] = {
    content.toList
  }

  override def fetchTerminalRecords(sax:String,bitStep:Int): List[(String,Long)] = {
    content.toList
  }

  override def getNodeNbr(): (Int, Int) = {
    (0,1)
  }
}