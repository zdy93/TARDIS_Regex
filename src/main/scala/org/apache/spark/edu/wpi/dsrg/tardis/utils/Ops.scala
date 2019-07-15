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

/**
  * Created by lzhang6 on 8/13/17.
  * Modified by Dongyu Zhang on 06/14/2019.
  */

import org.apache.spark.edu.wpi.dsrg.tardis.configs.{EqCfg, IdxCfg}
import org.apache.spark.edu.wpi.dsrg.tardis.utils.Util.genZero
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer
import scala.math._
import scala.util.Random

/**
  * old version: SAX byte
  * new version: SAX int, more scalability
  */
object Ops extends Logging with Serializable {
  def cvtTsToIsaxHex(tsRecord: Array[Float], idxCfg: IdxCfg): String = {
    val tsPaa = cvtTsToPaa(tsRecord, idxCfg.step)
    val tsSax = cvtPaaToSAX(tsPaa, idxCfg.breakPoints)
    val tsIsaxHex = cvtSaxToIsaxHex(tsSax, idxCfg.startHrc, idxCfg.bitStep)
    tsIsaxHex
  }

  def cvtTsULToIsaxHexArray(tsRecord: Array[Float], idxCfg: IdxCfg, eqCfg: EqCfg): Array[String] = {
    val tsUb = tsRecord.slice(0, tsRecord.length/2)
    val tsLb = tsRecord.slice(tsRecord.length/2, tsRecord.length)
    val tsUL = tsUb zip tsLb
    val tsULPAAZip = tsUL.sliding(idxCfg.step, idxCfg.step).map(x => cvtTsUlToPaa(x)).toArray
    val (tsUbPAA, tsLbPAA) = tsULPAAZip.unzip
    val tsUbSAX = cvtPaaToSAX(tsUbPAA, idxCfg.breakPoints)
    val tsLbSAX = cvtPaaToSAX(tsLbPAA, idxCfg.breakPoints)

    if (eqCfg.eqSortSAXType == 0){
      val eleList = (tsLbSAX zip tsUbSAX).map{case (lb, ub) => (lb to ub).toList}.toList
      val result = cartesianProduct(eleList)
      val finalResult = result.toArray
        .map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep))
      Random.setSeed(eqCfg.eqSeed)
//      Util.writeLog(content = "==> the tsSAXArray Length is: %d".format(finalResult.length))
      Random.shuffle(finalResult.toSeq).toArray.take(eqCfg.eqMaxSAXNbr)
    } else if(eqCfg.eqSortSAXType == 1){
      val eleList = (tsLbSAX zip tsUbSAX).map{case (lb, ub) => (lb to ub).toList}.toList.map(x => medianFirst(x))
      val result = cartesianProduct(eleList)
      val finalResult = medianListFirst(result).toArray
        .map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep))
//      Util.writeLog(content = "==> the tsSAXArray Length is: %d".format(finalResult.length))
      finalResult.take(eqCfg.eqMaxSAXNbr)
    } else if(eqCfg.eqSortSAXType == 2){
      val eleList = (tsLbSAX zip tsUbSAX).map{case (lb, ub) => (lb to ub).toList}.toList.map(x => medianFirst(x))
      val result = cartesianProduct(eleList)
      val finalResult = medianListFirst(result).toArray
        .map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep)).reverse
//      Util.writeLog(content = "==> the tsSAXArray Length is: %d".format(finalResult.length))
      finalResult.take(eqCfg.eqMaxSAXNbr)
    }else if(eqCfg.eqSortSAXType == 3){
      val tsIgnoreRegexPAA = tsUL.sliding(idxCfg.step, idxCfg.step).map(x => cvtTsUlToOnePaa(x)).toArray
      val tsIgnoreRegexSAX = cvtPaaToSAX(tsIgnoreRegexPAA, idxCfg.breakPoints)
      val eleList = (tsLbSAX zip tsUbSAX).map{case (lb, ub) => (lb to ub).toList}.toList
      val result = cartesianProduct(eleList)
      val finalResult = closeToIgnoreSAXFirst(result, tsIgnoreRegexSAX.toList).toArray
        .map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep))
//      Util.writeLog(content = "==> the tsSAXArray Length is: %d".format(finalResult.length))
      finalResult.take(eqCfg.eqMaxSAXNbr)
    }else if(eqCfg.eqSortSAXType ==4){
      val tsIgnoreRegexPAA = tsUL.sliding(idxCfg.step, idxCfg.step).map(x => cvtTsUlToOnePaa(x)).toArray
      val tsIgnoreRegexSAX = cvtPaaToSAX(tsIgnoreRegexPAA, idxCfg.breakPoints)
      val eleList = (tsLbSAX zip tsUbSAX).map{case (lb, ub) => (lb to ub).toList}.toList
      val result = cartesianProduct(eleList)
      val finalResult = closeToIgnoreSAXFirst(result, tsIgnoreRegexSAX.toList).toArray
        .map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep)).reverse
//      Util.writeLog(content = "==> the tsSAXArray Length is: %d".format(finalResult.length))
      finalResult.take(eqCfg.eqMaxSAXNbr)
    }else if(eqCfg.eqSortSAXType == 5){
      val eleList = (tsLbSAX zip tsUbSAX).map{case (lb, ub) => (lb to ub).toList}.toList
      val result = cartesianProduct(eleList)
      if (result.length > eqCfg.eqMaxSAXNbr){
        val finalResult = equWidthTake(result, eqCfg.eqMaxSAXNbr, eqCfg.eqSeed).toArray
          .map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep))
//        Util.writeLog(content = "==> the tsSAXArray Length is: %d".format(finalResult.length))
        finalResult
      }else{
        val finalResult = result.toArray
          .map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep))
//        Util.writeLog(content = "==> the tsSAXArray Length is: %d".format(finalResult.length))
        finalResult
      }
    }else {
      val result = takeEquWidth(tsUbSAX, tsLbSAX, eqCfg.eqMaxSAXNbr)
      val finalResult = result.toArray.map(x => cvtSaxToIsaxHex(x.toArray, idxCfg.startHrc, idxCfg.bitStep))
      finalResult
    }
  }

  private[this] def medianFirst(ele: List[Int]): List[Int] ={
    val mean = ele.sum.toFloat / ele.length.toFloat
    def sortByMeanDiff(x1: Int, x2: Int): Boolean ={
      abs(x1 - mean) < abs(x2 - mean)
    }
    val oEle = ele.sortWith((x1, x2)=> sortByMeanDiff(x1, x2))
    oEle
  }


  private[this] def cartesianProduct[T](lst: List[List[T]]): List[List[T]] = {

    /**
      * https://rosettacode.org/wiki/Cartesian_product_of_two_or_more_lists#Scala
      * Prepend single element to all lists of list
      * @param e single elemetn
      * @param ll list of list
      * @param a accumulator for tail recursive implementation
      * @return list of lists with prepended element e
      */
    def pel(e: T,
            ll: List[List[T]],
            a: List[List[T]] = Nil): List[List[T]] =
      ll match {
        case Nil => a.reverse
        case x :: xs => pel(e, xs, (e :: x) :: a )
      }

    lst match {
      case Nil => Nil
      case x :: Nil => List(x)
      case x :: _ =>
        x match {
          case Nil => Nil
          case _ =>
            lst.par.foldRight(List(x))( (l, a) =>
              l.flatMap(pel(_, a))
            ).map(_.dropRight(x.size))
        }
    }
  }


  private[this] def medianListFirst(cartArray: List[List[Int]]): List[List[Int]] ={
    val medianList = cartArray.head
    def sortByMedianDiff(el: List[Int]): Int ={
      (medianList zip el).map{case(m, e) => abs(m - e)}.sum
    }
    cartArray.sortWith((e1, e2) => sortByMedianDiff(e1) < sortByMedianDiff(e2))
  }

  private[this] def closeToIgnoreSAXFirst(cartArray: List[List[Int]], tsIgnoreRegexSAX: List[Int]): List[List[Int]] ={
    def sortByDiff(el: List[Int]): Int ={
      (tsIgnoreRegexSAX zip el).map{case(m ,e) => abs(m - e)}.sum
    }
    cartArray.sortWith((e1, e2) => sortByDiff(e1) < sortByDiff(e2))
  }

  private[this] def equWidthTake(cartArray: List[List[Int]], num: Int, seed: Long): List[List[Int]]={
    if (num == 1){
      List(cartArray.head)
    }else{
      val firstList = cartArray.head
      def sortByDiff(el: List[Int]): Int ={
        (firstList zip el).map{case(m, e) => abs(m - e)}.sum
      }
      val distMap = cartArray.groupBy(sortByDiff(_))
      val distList = distMap.keySet.toList.sorted
      var finalList = ListBuffer.empty[List[Int]]
      if (num <= distList.length){
        val width = distList.length / (num - 1)
        for(i <- distList.indices by width){
          Random.setSeed(seed)
          val subList = Random.shuffle(distMap.get(distList(i)).toList.head).take(1)
          finalList ++= subList
        }

      } else{
        val numToTake = math.ceil(num / distList.length).toInt
        for(i <- distList.indices){
          Random.setSeed(seed)
          val subList = Random.shuffle(distMap.get(distList(i)).toList.head).take(numToTake)
          finalList ++= subList
        }
      }
      if (finalList.size < num){
        finalList ++= Random.shuffle(distMap.get(distList(distList.last)).toList.head).take(num - finalList.size)
      }
      if (finalList.size > num){
        Random.shuffle(finalList).take(num).sortBy(sortByDiff(_)).toList
      }else{
        finalList.toList
      }
    }
  }

  private[this] def getBPByNum(u: Int, l: Int, num: Int):  List[Int]= {
    val step = (u -l).toFloat/(num -1f)
    if(step == 0f){
      List.tabulate(num)(_ => (u+l)/2)
    }else{
      var finalList = ((l.toFloat to u - 1.0f by step).toArray.map(x => Math.round(x)).take(num -1) :+ u).toList
      if(finalList.length < num){
        finalList ::: List.tabulate(num - finalList.length)(_ => u)
      }else{
        finalList
      }
    }
  }

  private[this] def takeEquWidth(ubSAX: Array[Int], lbSAX: Array[Int], num: Int): List[List[Int]]={
    val ulSAX = lbSAX zip ubSAX
    if (num == 1){
      val cartArray = ulSAX.map{case(l, u) => (l + u)/2}.toList
      List(cartArray)
    }else{
      val totalSAX = ulSAX.map{case(l, u) => (u-l)+1}.product
      if (totalSAX > num){
        val eleArray = ulSAX.map{case (l, u) => getBPByNum(u, l, num)}.toList
        val cartArray = eleArray.transpose
        cartArray
      }else {
        val eleArray = ulSAX.map{case (l, u) => (l to u).toList}.toList
        val cartArray = cartesianProduct(eleArray)
        cartArray
      }
    }

  }

  private[this] def cvtTsToPaa(tsRecord: Array[Float], step: Int): Array[Float] = {
    tsRecord.sliding(step, step).map(x => x.sum / x.length).toArray
  }

  private[this] def cvtTsUlToPaa(ts_ul: Array[(Float, Float)]): (Float, Float) = {
    val (ts_up, ts_low) = ts_ul.unzip
    var ts_up_p, ts_low_p = 0f
    ts_up_p = ts_up.sum / ts_up.length
    ts_low_p = ts_low.sum / ts_low.length
//    if (java.util.Arrays.equals(ts_up, ts_low)){
//      ts_up_p = ts_up.sum / ts_up.length
//      ts_low_p = ts_low.sum / ts_low.length
//    }else{
//      val ul = ts_up zip ts_low
//      ts_up_p = ts_up.max
//      ts_low_p = ts_low.min
//      val ul_eq = ul.filter(x => x._1 == x._2).map(x => x._1)
//      val ul_neq = ul.filterNot(x => x._1 == x._2)
//      val (ul_neq_u, ul_neq_l)= ul_neq.unzip
//      ts_up_p = List(ul_neq_u.max, ul_eq.sum / ul_eq.length).max
//      ts_low_p = List(ul_neq_l.min, ul_eq.sum / ul_eq.length).min
//    }
    (ts_up_p, ts_low_p)
  }
  private[this] def cvtTsUlToOnePaa(ts_ul: Array[(Float, Float)]): Float = {
    val (ts_up, ts_low) = ts_ul.unzip
    (ts_up.sum + ts_low.sum) / (ts_up.length + ts_low.length)
  }

  private[this] def cvtPaaToSAX(tsPaa: Array[Float], BPs: Array[Float]): Array[Int] = {
    def convert(v: Float): Int = BPs.indexWhere(v < _)

    tsPaa.map(x => convert(x))
  }

  private[this] def cvtSaxToIsaxHex(tsSax: Array[Int], hrc: Int, bitStep: Int): String = {
    def cvtIntToBinStr(x: Int, hrc: Int): String = {
      val xs: String = x.toBinaryString
      if (xs.length < hrc)
        genZero(hrc - xs.length) + xs
      else xs
    }

    def cvtBinCharArrayToHexStr(v: Array[Char]): String = {
      val tmp = Integer.parseInt(v.mkString, 2).toHexString
      if (tmp.length < bitStep)
        genZero(bitStep - tmp.length) + tmp
      else tmp
    }

    tsSax
      .map(x => cvtIntToBinStr(x, hrc).toArray)
      .transpose
      .map(x => cvtBinCharArrayToHexStr(x))
      .mkString
  }

  def calDistance(aTs: Array[Float], bTs: Array[Float]): Double = {
//    require(aTs.length == bTs.length, "aTs.length: %d == bTs.length: %d".format(aTs.length, bTs.length))
    Util.euclideanDistance(aTs, bTs)
  }

  def calPaaSaxLB(ndSax: String, ts: Array[Float], idxCfg: IdxCfg): Double = {
    val saxA = cvtSaxHexToDec(ndSax, idxCfg.bitStep)
    var tsPAA = Array(0f)

    if (ts.length / idxCfg.tsLength == 2){
      val tsUb = ts.slice(0, ts.length/2)
      val tsLb = ts.slice(ts.length/2, ts.length)
      val tsUL = tsUb zip tsLb
      val tsULPAAZip = tsUL.sliding(idxCfg.step, idxCfg.step).map(x => cvtTsUlToPaa(x)).toArray
      val (tsUbPAA, tsLbPAA) = tsULPAAZip.unzip
      tsPAA = tsUbPAA ++ tsLbPAA
    }else{
      tsPAA = cvtTsToPaa(ts, idxCfg.step)
    }
    val hrc = ndSax.length / idxCfg.bitStep
    sqrt(idxCfg.tsLength / idxCfg.wordLength) * calPaaSaxDist(saxA, tsPAA, hrc)
  }

  def containSax(sax: String, saxNode: String): Boolean = {
    require(sax.length >= saxNode.length, "sax.length: %d >= saxNode.length: %d".format(sax.length, saxNode.length))
    sax.take(saxNode.length) == saxNode
  }

  def cvtSAXtoSpecHrc(sax: String, hrc: Int, bitStep: Int): String = {
    require(sax.length >= hrc * bitStep, "sax.length %d >= hrc*idxCfg.bitStep %d".format(sax.length, hrc * bitStep))
    sax.take(hrc * bitStep)
  }

  def cvtToAncestor(sax: String, bitStep: Int): String = {
    require(sax.length > bitStep, "sax.length: %d > idxCfg.bitStep: %d".format(sax.length, bitStep))
    sax.dropRight(bitStep)
  }

  def cvtSaxHexToDec(ndSax: String, bitStep: Int): List[Int] = {
    def cvtHexToBin(hexSax: String, len: Int): String = {
      val data = Integer.parseInt(hexSax, 16).toBinaryString
      val gap = len - data.length
      if (gap > 0) {
        Util.genZero(gap) + data
      } else data
    }

    val len = bitStep * 4

    val ndSaxHex = ndSax
      .sliding(bitStep, bitStep)
      .map(x => cvtHexToBin(x, len).sliding(1, 1).toArray)
      .toArray

    val result = ndSaxHex
      .transpose
      .map(elem => Integer.parseInt(elem.toList.mkString(""), 2))
      .toList
    result
  }



  def calPaaSaxDist(xsPaa: List[Int], ysSax: Array[Float], hrc: Int): Double = {
    val cardinality = pow(2, hrc).toInt
    val BPs = BreakPoint(cardinality).clone()

    def paaSaxULPairDis(x: Int, yU: Float, yL: Float): Float = {
      val uL = BPs(x)
      val dL = if (x == 0) Float.NegativeInfinity else BPs(x - 1)

      val result = if(yL > uL && uL != Float.NegativeInfinity) abs(uL - yL)
      else if (dL > yU && dL != Float.PositiveInfinity) abs(dL - yU)
      else 0.0F

      result
    }

    def paaSaxPairDis(x: Int, y: Float): Float = {
      val uL = BPs(x)
      val dL = if (x == 0) Float.NegativeInfinity else BPs(x - 1)

      val result = if (y > uL && uL != Float.NegativeInfinity) abs(uL - y)
      else if (dL > y && dL != Float.PositiveInfinity) abs(dL - y)
      else 0.0F

      //      println("uL: %.2f, dL: %.2f, \t\tx: %d,y: %.1f, \t\t result: %.2f".format(uL,dL,x,y,result))
      result
    }

    if (2 * xsPaa.length == ysSax.length){
      val ysUbSax = ysSax.slice(0, ysSax.length/2)
      val ysLbSax = ysSax.slice(ysSax.length/2, ysSax.length)
      sqrt((xsPaa, ysUbSax, ysLbSax).zipped.map{ case(x, yU, yL) => pow(paaSaxULPairDis(x, yU, yL), 2)}.sum)
    }else{
      require(xsPaa.length == ysSax.length, "calPaaSaxDist: xs.length:%d != ys.length: %d".format(xsPaa.length, ysSax.length))
      sqrt((xsPaa zip ysSax).map { case (x, y) => pow(paaSaxPairDis(x, y), 2) }.sum)
    }
  }
}