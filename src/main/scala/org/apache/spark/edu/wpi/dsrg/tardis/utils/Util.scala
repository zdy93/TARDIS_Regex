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

import java.io._
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.edu.wpi.dsrg.tardis.configs.idxConfig
import org.apache.spark.internal.Logging

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.math._

/**
  * Created by leon on 6/26/17.
  * Modified by Dongyu Zhang on 06/15/2019
  */
object Util extends Logging {

  def genZero(nbr: Int): String = {
    require(nbr >= 0, s"nbr>=0, but input is $nbr")
    if (nbr == 0) "" else "0" + genZero(nbr - 1)
  }

  def writeLog(content: String, isPrint: Boolean = true, logFileName: String=idxConfig.logPath): Unit = {
    try {
      val file = new File(logFileName)
      val writer = new PrintWriter(new FileOutputStream(file, true))

      val time = getCurrentTime()

      if (content.contains("Configuration")) {
        writer.append("\n" + "-*" * 15 + time + "-*" * 15 + "\n")
      }

      writer.append(time + content + "\n")
      if (isPrint) {
        println(time + content)
      }
      writer.close()
    } catch {
      case e: java.lang.NullPointerException => logError(e.toString)
    }
  }

  private[this] def getCurrentTime(): String = {
    val timeFormat = new SimpleDateFormat("HH:mm:ss - MM.dd.yyyy - z")
    val now = Calendar.getInstance().getTime()
    timeFormat.format(now)
  }

  def mean(xs: List[Long]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }

  def stddev(xs: List[Long], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a, e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  def meanDouble(xs: List[Double]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }

  def stddevDouble(xs: List[Double], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a, e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

//  def euclideanDistance(xs: Array[Double], ys: Array[Double]): Double = {
//    sqrt((xs zip ys).map { case (x, y) => pow(y - x, 2) }.sum)
//  }

  def euclideanDistance(xs: Array[Float], ys: Array[Float]): Double = {
    if (xs.length == ys.length){
      sqrt((xs zip ys).map { case (x, y) => pow(y - x, 2) }.sum)
    } else{
      var LongS, ShortS = Array(0f)
      if (xs.length * 2 == ys.length){
        LongS = ys
        ShortS = xs
      } else{
        require(xs.length == ys.length * 2)
        LongS = xs
        ShortS = ys
      }
      val lUb = LongS.slice(0, ys.length/2)
      val lLb = LongS.slice(ys.length/2, ys.length)

      def ULPairDistance(x: Float, yU:Float, yL: Float): Double = {
        val result = if (x < yL) abs(yL - x)
        else if (x > yU) abs(x - yU)
        else 0.0
        result
      }

      sqrt((ShortS, lUb, lLb).zipped.map{ case (x, yU, yL) => pow(ULPairDistance(x, yU, yL), 2)}.sum)
    }
  }


  def lowBoundDist(xs:List[Int],ys:List[Int],hrc:Int): Double ={
    val cardinality =pow(2,hrc).toInt
    val BPs = BreakPoint(cardinality).clone()

    def convert(x:Int,y:Int): Double = {
      if ((x==y) || abs(x-y)==1){
        0
      }else{
        BPs(max(x,y)-1) - BPs(min(x,y))
      }
    }

    sqrt((xs zip ys).map{case (x,y) => pow(convert(x,y),2)}.sum)
  }

  def readConfigFile(configPath: String): Map[String, String] = {
    var staff: Array[String] = null
    try {
      val configFile = Source.fromFile(configPath)
      staff = configFile.getLines().toArray
      configFile.close()
    } catch {
      case e1: FileNotFoundException => logError(configPath + e1.toString)
      case e2: IOException => logError(configPath + e2.toString)
      System.exit(0)
    }

    var content = HashMap.empty[String, String]
    val sep = "="
    for (line <- staff if (line.contains(sep) && !line.contains("#"))) {
        try {
          val t = line.split(sep)
          content += (t(0).trim -> t(1).trim)
        } catch {
          case e: Exception => println("==> %s can't parse %s".format(line.toString,e.toString))
        }
    }

    content.toMap
  }

  def checkDir(file: File): Unit = {
    val dir = new File(file.getParent)
    if (!dir.exists()) {
      dir.mkdir()
    }
  }

  def hdfsDirExists(hdfsDirectory: String): Boolean = {
    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    val exists = try {
      fs.exists(new Path(hdfsDirectory))
    } catch {
      case e: java.net.ConnectException => {
        logError(e.toString)
        false
      }
    }

    return exists
  }

  def removeHdfsFiles(hdfsDirectory: String): String = {
    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    try {
      fs.delete(new Path(hdfsDirectory), true)
    } catch {
      case e: Exception => logError(e.toString)
    }

    "==> remove %s".format(hdfsDirectory)
  }

  def getHdfsFileNameList(hdfsFileName: String): List[String] = {
    val fs = FileSystem.get(new URI(hdfsFileName), new Configuration())
    fs.listStatus(new Path(hdfsFileName)).map {
      _.getPath.toString
    }.filter(x => !x.contains("_SUCCESS")).toList
  }

  def writeArrayBytesToHdfs(hdfsFileName: String, data: Array[Byte]): Unit = {
    val fs = FileSystem.get(new URI(hdfsFileName), new Configuration())

    if (Util.hdfsDirExists(hdfsFileName))
      Util.removeHdfsFiles(hdfsFileName)

    val os = fs.create(new Path(hdfsFileName))
    try {
      os.write(data)
    } finally {
      os.close()
    }
  }

  def getHdfsFileSizeMList(hdfsDirectory: String): List[Double] = {
    val hadoopConf = new Configuration()
    val uri = new URI(hdfsDirectory)
    val fListStatus = FileSystem.get(uri, hadoopConf).listStatus(new Path(hdfsDirectory))
    fListStatus.map { x => convertToM(x.getLen) }.filter(x => x != 0.0).toList
  }

  def getArrayBytesFromHdfs(hdfsDirectory: String): Array[Byte] = {
    require(hdfsDirExists(hdfsDirectory), "%s doesn't exist".format(hdfsDirectory))

    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    val path = new Path(hdfsDirectory)
    val is = fs.open(path)
    val fileSize = fs.listStatus(path).map {
      _.getLen
    }.max.toInt

    var contentBuffer = new Array[Byte](fileSize)
    is.readFully(0, contentBuffer)
    require((fileSize == contentBuffer.size), "(fileSize: %d == contentBuffer.size: %d)".format(fileSize, contentBuffer.size))
    is.close()
    contentBuffer
  }

  def fetchBoolFromString(input: String): Boolean = {
    val content = input.trim.toLowerCase
    if (content == "true" || content == "yes") true
    else false
  }

  private def convertToM(size: Long): Double = {
    (size * 1.0) / (1024 * 1024)
  }
}