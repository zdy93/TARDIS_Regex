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

import org.apache.spark.edu.wpi.dsrg.tardis.configs.{IdxCfg, eqConfig}
import org.apache.spark.edu.wpi.dsrg.tardis.utils.Ops
import org.apache.spark.internal.Logging
import org.apache.spark.edu.wpi.dsrg.tardis.utils.Util

import scala.collection.mutable.{HashMap, ListBuffer}

/**
  * Created by leon on 10/18/17.
  */
class AbstractClusterNodeR extends Logging with Serializable {
  var saxRep: String = _
  var rcdNbr: Long = 0
  var ancestor: AbstractClusterNodeR = _
  var descendants = HashMap.empty[String, AbstractClusterNodeR]

  def calHrc(bitStep: Int = 1): Int = {
    if (saxRep == "Root")
      0
    else
      saxRep.length / bitStep
  }

  def setAncestor(ancestor: AbstractClusterNodeR): Unit = {
    this.ancestor = ancestor
  }

  def fetchSaxRep(): String = {
    saxRep
  }

  def fetchRcdNbr(): Long = {
    rcdNbr
  }

  def fetchAncestor(): AbstractClusterNodeR = {
    ancestor
  }

  def addRecord(elem: (String, (Array[Float], Long)), bitStep: Int): Unit = {
    this.rcdNbr += 1

    if (this.calHrc(bitStep) + 1 == (elem._1.length / bitStep)) {
      val teminalSaxRep = elem._1

      if (!this.descendants.contains(teminalSaxRep)){
        val terminalNodeR = new TerminalClusterNodeR(elem._1)
        terminalNodeR.setAncestor(this)
        this.descendants += (teminalSaxRep -> terminalNodeR)
      }

      this.descendants(teminalSaxRep).addRecord(elem,bitStep)

    } else if (this.calHrc(bitStep) + 1 < (elem._1.length / bitStep)) {
      val internalSaxRep = elem._1.take((this.calHrc(bitStep) + 1) * bitStep)

      if (!this.descendants.contains(internalSaxRep)) {
        val internalNodeR = new InternalClusterNodeR(internalSaxRep)
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
      if (node.isInstanceOf[InternalClusterNodeR]){
        if (node.rcdNbr <= th){

          val newTerminalNode = new TerminalClusterNodeR(sax)
          newTerminalNode.setAncestor(this)
          newTerminalNode.setContent(node.fetchAllRecords())

          this.descendants(sax) = newTerminalNode
        }else{
          this.descendants(sax).shrinkNodeLayer(th)
        }
      }
    }
  }

  def fetchTerminalRecords(sax:String,bitStep:Int): List[(String,(Array[Float], Long))] ={
    require(sax.length >= bitStep, "sax.length >= bitStep")

    try{
      if (saxRep == "Root"){
        val startLayer = this.asInstanceOf[RootClusterNodeR].startLayer
        this.descendants(sax.take(bitStep*startLayer)).fetchTerminalRecords(sax,bitStep)
      }else{
        val len = saxRep.length
        if (saxRep == sax) {
          if (this.isInstanceOf[TerminalClusterNodeR]){
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

  def getExactMatching(sax:String,bitStep:Int,ts:Array[Float]): Boolean ={
    val records = fetchTerminalRecords(sax,bitStep)

    for (r <- records){
      if (sax == r._1){
        if (r._2._1.sameElements(ts)){
          return true
        }
      }
    }
    false
  }

  def fetchTopRecords(sax:String,bitStep:Int, kValue: Int): List[(Array[Float], Long)] = {
    if (saxRep == "Root" ){
      try{
        val startLayer = this.asInstanceOf[RootClusterNodeR].startLayer
        val newRep = sax.take(bitStep*startLayer)
        if (this.descendants.contains(newRep)&& this.descendants(newRep).rcdNbr > kValue && this.rcdNbr > kValue){
          this.descendants(sax.take(bitStep*startLayer)).fetchTopRecords(sax,bitStep,kValue)
        }else{
          fetchAllTsId()
        }
      }catch {
        case e:Exception => {
          logError(e.toString)
          fetchAllTsId()
        }
      }
    }else{
      if (this.isInstanceOf[InternalClusterNodeR]){
        val len =  saxRep.length
        if(this.rcdNbr >= kValue){
          try{
            if (this.descendants(sax.take(len + bitStep)).rcdNbr > kValue){
              this.descendants(sax.take(len + bitStep)).fetchTopRecords(sax,bitStep,kValue)
            }else{
              this.fetchAllTsId()
            }
          }catch{
            case e:Exception => {
              logError(e.toString)
              this.fetchAllTsId()
            }
          }
        }else{
          logError("fetchTopKRecords: %s\t this.rcdNbr < kValue".format(this.saxRep))
          this.fetchAllTsId()
        }
      }else{ // TerminalClusterNodeR
        this.fetchTopRecords(sax,bitStep,kValue)
      }
    }
  }

  def fetchAllRecords(): List[(String,(Array[Float], Long))] = {
    if (this.isInstanceOf[RootClusterNodeR] || this.isInstanceOf[InternalClusterNodeR]){
      val result = ListBuffer.empty[(String,(Array[Float], Long))]

      val iter = this.descendants.toIterator
      while(iter.hasNext){
        result ++= iter.next()._2.fetchAllRecords()
      }
      result.toList
    }else{
      this.fetchAllRecords()
    }
  }

  def fetchAllTsId():List[(Array[Float], Long)]={
    val contents = fetchAllRecords()
    val result = ListBuffer.empty[(Array[Float], Long)]

    for(c <- contents){
        result += c._2
    }

    result.toList
  }

  //create use for label
  def fetchRangDistId(ts:Array[Float], distT:Double, idxCfg: IdxCfg): List[(Double,Long)] ={
    val holder = ListBuffer.empty[(Double,Long)]

    if (this.descendants.size == 0){
      if (Ops.calPaaSaxLB(this.saxRep,ts,idxCfg) <= distT){
        for ((sax,(tsb,id)) <- this.asInstanceOf[TerminalClusterNodeR].content){
          val distance = Util.euclideanDistance(tsb,ts)
          if (distance <= distT)
            holder += ((distance,id))
        }
      }
    }else{
      for((sax,node) <- this.descendants){
        if(Ops.calPaaSaxLB(sax,ts,idxCfg) <= distT){
          holder ++=  node.fetchRangDistId(ts,distT,idxCfg)
        }
      }
    }
    holder.toList
  }

  def fetchRangId(ts:Array[Float], distT:Double, idxCfg: IdxCfg): List[Long] ={
    val holder = ListBuffer.empty[Long]

    if (this.descendants.size == 0){
      if (Ops.calPaaSaxLB(this.saxRep,ts,idxCfg) <= distT){
        for ((sax,(tsb,id)) <- this.asInstanceOf[TerminalClusterNodeR].content){
          if (Util.euclideanDistance(tsb,ts) <= distT)
            holder += id
        }
      }
    }else{
      for((sax,node) <- this.descendants){
        if(Ops.calPaaSaxLB(sax,ts,idxCfg) <= distT){
          holder ++=  node.fetchRangId(ts,distT,idxCfg)
        }
      }
    }
    holder.toList
  }

  /**
    * get the internal and terminal numbers
    * return  result:
    *   (idList, (internal Nbr, terminal Nbr))
    */
  def fetchRangIdWithNodeNbr(ts:Array[Float], distT:Double, idxCfg: IdxCfg): (List[Long],(Int,Int)) ={
    val holder = ListBuffer.empty[Long]
    var iNbr = 0
    var tNbr = 0

    if (this.descendants.size == 0){
      if (Ops.calPaaSaxLB(this.saxRep,ts,idxCfg) <= distT){
        for ((sax,(tsb,id)) <- this.asInstanceOf[TerminalClusterNodeR].content){
          if (Util.euclideanDistance(tsb,ts) <= distT)
            holder += id
        }
        tNbr = 1
      }
    }else{
      iNbr = 1
      for((sax,node) <- this.descendants){
        if(Ops.calPaaSaxLB(sax,ts,idxCfg) <= distT){
          val (tempHolder,(tin,ttn)) = node.fetchRangIdWithNodeNbr(ts,distT,idxCfg)
          holder ++=  tempHolder
          iNbr += tin
          tNbr += ttn
        }
      }
    }
    (holder.toList,(iNbr,tNbr))
  }

  def getNodeNbr(): (Int,Int) ={
    var terminalNbr = 0
    var interalNbr = if (this.isInstanceOf[InternalClusterNodeR]){1} else {0}

    for ((k,node)<-this.descendants){
      var (i,t) = node.getNodeNbr()
      interalNbr += i
      terminalNbr += t
    }

    (interalNbr,terminalNbr)
  }

  override def toString(): String = {
    val classType = this.getClass.toString.split('.').last match {
      case "InternalClusterNodeR" => "I";
      case "TerminalClusterNodeR" => "T";
      case "RootClusterNodeR" => "R";
    }
    "\n|" + "-" * calHrc() + classType + " sax:" + saxRep + " nbr:" + this.rcdNbr.toString + descendants.valuesIterator.mkString
  }

}

object RootClusterNodeR extends Logging with Serializable {
  def apply(datas: Iterator[(String, (Array[Float], Long))], bitStep: Int,th:Int): Iterator[RootClusterNodeR] = {
    val root = new RootClusterNodeR(1)
    root.build(datas, bitStep, th)
  }
}

class RootClusterNodeR extends AbstractClusterNodeR {
  var startLayer:Int = 1;

  def this(f: Int) {
    this()
    saxRep = "Root"
    ancestor = this
    startLayer = 1
    descendants = HashMap.empty[String, AbstractClusterNodeR]
  }

  def build(datas: Iterator[(String, (Array[Float], Long))], bitStep: Int, th:Int): Iterator[RootClusterNodeR] = {
    for (elem <- datas) {
      this.addRecord(elem, bitStep)
    }
    this.prueSingleElementLayer()
    this.shrinkNodeLayer(th)
    Iterator(this)
  }

  def prueSingleElementLayer(): Unit = {
    var tempNodeR: AbstractClusterNodeR = this

    while (tempNodeR.descendants.size == 1) {
      tempNodeR = tempNodeR.descendants.last._2
      startLayer += 1
    }
    this.descendants = tempNodeR.descendants

    this.descendants.foreach(_._2.setAncestor(this))
  }

  def calTopKRecords(ts:Array[Float],sax:String,bitStep:Int, kValue: Int): List[(Double,Long)] = {
    val failRecords = List((Double.PositiveInfinity, -1L))
    try{
      val records = fetchTopRecords(sax,bitStep,kValue)

      //    val content ="calTopKRecords  "+ kValue+"/"+records.length
      //    println(content)

      if (records.size > kValue){
        records.map{case(tsb,id) => (Ops.calDistance(tsb, ts), id)}
          .sortWith(_._1 < _._1)
          .take(kValue)
      }else{
        records.map{case(tsb,id) => (Ops.calDistance(tsb, ts), id)}
          .sortWith(_._1 < _._1)
      }
    } catch{
      case e:Exception => {
        logError(e.toString)
        failRecords
      }
      case e:Error => {
        logError(e.toString)
        failRecords
      }
    }
  }

  def calTopKRecords1NdWhole(ts:Array[Float],sax:String,bitStep:Int, kValue: Int,idxCfg: IdxCfg): List[(Double,Long)] = {
    val failRecords = List((Double.PositiveInfinity, -1L))
    try{
      val records = fetchTopRecords(sax,bitStep,kValue)

      //    println("calTopKRecords-1  "+ kValue+"/"+records.length)

      val firstTopValue = if (records.size > kValue){
        records.map{case(tsb,id) => (Ops.calDistance(tsb, ts), id)}
          .sortWith(_._1 < _._1)
          .take(kValue)
      }else{
        records.map{case(tsb,id) => (Ops.calDistance(tsb, ts), id)}
          .sortWith(_._1 < _._1)
      }

      val th = firstTopValue.last._1
//      Util.writeLog("++> success get firstTopValue, the length is %d".format(firstTopValue.length))

      val secondTopValue = fetchRangDistId(ts,th,idxCfg)

      //    println("calTopKRecords-2  "+ kValue+"/"+secondTopValue.length)

      if (secondTopValue.size > kValue){
        secondTopValue
          .sortWith(_._1 < _._1)
          .take(kValue)
      }else{
        secondTopValue
          .sortWith(_._1 < _._1)
      }
    } catch{
      case e:Exception => {
        logError(e.toString)
        failRecords
      }
      case e:Error => {
        logError(e.toString)
        failRecords
      }
    }
  }

  def calTopKRecordsMNdThreshold(ts:Array[Float],sax:String,bitStep:Int, kValue: Int): Double = {
    val failThres = Double.PositiveInfinity
    try{
      val records = fetchTopRecords(sax,bitStep,kValue)

      //    println("calTopKRecords  "+ kValue+"/"+records.length)

      val firstTopValue = if (records.size > kValue){
        records.map{case(tsb,id) => (Ops.calDistance(tsb, ts), id)}
          .sortWith(_._1 < _._1)
          .take(kValue)
      }else{
        records.map{case(tsb,id) => (Ops.calDistance(tsb, ts), id)}
          .sortWith(_._1 < _._1)
      }
      val threshold = firstTopValue.last._1

      threshold
    } catch{
      case e:Exception => {
        logError(e.toString)
        failThres
      }
      case e:Error => {
        logError(e.toString)
        failThres
      }
    }

  }
}


class InternalClusterNodeR extends AbstractClusterNodeR {
  def this(sax: String) {
    this()
    saxRep = sax
    descendants = HashMap.empty[String, AbstractClusterNodeR]
  }
}

class TerminalClusterNodeR extends AbstractClusterNodeR {
  val content = ListBuffer.empty[(String, (Array[Float], Long))]

  def this(sax: String){
    this()
    this.saxRep = sax
    rcdNbr = 0
  }

  override def addRecord(elem: (String, (Array[Float], Long)), bitStep: Int): Unit ={
    content += elem
    rcdNbr += 1
  }

  def setContent(records: List[(String,(Array[Float], Long))]): Unit ={
    rcdNbr = 0

    for (elem <- records){
      content += elem
      rcdNbr += 1
    }
  }

  override def fetchTopRecords(sax: String, bitStep: Int, kValue: Int): List[(Array[Float], Long)] = {
    //require(kValue <= this.rcdNbr,"TerminalClusterNodeR: %s\tkValue <= this.rcdNbr".format(this.saxRep))
    content.map{case(sax,(ts,id)) => (ts,id)}.toList
  }

  override def fetchAllRecords(): List[(String, (Array[Float], Long))] = {
    content.toList
  }

  override def fetchTerminalRecords(sax:String,bitStep:Int): List[(String,(Array[Float], Long))] = {
    content.toList
  }

  override def getNodeNbr(): (Int, Int) = {
    (0,1)
  }
}