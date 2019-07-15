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

package org.apache.spark.edu.wpi.dsrg.tardis

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.edu.wpi.dsrg.tardis.isax.{TreeNodeHex, iSAXPartitioner, iSAXPartitionerSampling, iSAXTreeHex}
import org.apache.spark.edu.wpi.dsrg.tardis.utils.{Util, tsEvaluate}

/**
  * Created by leon on 6/22/17.
  */
object TARDIS extends Logging {
  def main(args: Array[String]): Unit = {
    val sc = initializeSparkContext()

    args(0) match {
      case "-g" => RandWalkTsCreater.generateTsAndSave(sc)
      case "-b" => IndexHex(sc)
      case "-c" => CreateLabel(sc,args(1))
      case "-q" => ExecuteQuery(sc)
      case "-e" => tsEvaluate(sc)
      case _ => printHelpMsg()
    }
    sc.stop()
  }

  def printHelpMsg(): Unit = {
    println("Usage: configuration file is ./etc/config.conf")
    println("Generate data:  -g")
    println("Build index  :  -b")
    println("Create labels:  -c exact-knn-range")
    println("Query records:  -q")
    println("evaluate time series:  -e")
    println("help -h")
  }

  private def initializeSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(
      classOf[TreeNodeHex],
      classOf[iSAXPartitioner],
      classOf[iSAXPartitionerSampling],
      classOf[iSAXTreeHex]))
    new SparkContext(conf)
  }
}
