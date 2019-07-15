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
  * Created by leon on 9/15/17.
  */

import bloomfilter.mutable.BloomFilter
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object BloomFilterRdd {
  def apply(): BloomFilterRdd = new BloomFilterRdd()
}

class BloomFilterRdd extends Logging {
  var bfs: RDD[(Int, BloomFilter[String])] = null

  def generateAndSave(saxRdd: RDD[(Int, Iterable[String])], path: String): Unit = {
    saxRdd
      .map { case (id, saxs) => (id, inOps.cvtSaxToBf(saxs)) }
      .saveAsObjectFile(path)

    //      .partitionBy(new RangePartitioner(10,saxRdd))
  }

  def querySax(qSax: String, pId: Int): Boolean = {
    require(bfs != null, "initial bfs at first")
//    val tp = bfs.lookup(pId)
//    println("tp.size\t%d".format(tp.size))
//    tp(0).mightContain(qSax)

    bfs.filter { case (id, bf) => id == pId }
      .map { case (id, bf) => bf.mightContain(qSax) }
      .collect()(0)
  }

  def readAndPersistRdd(sc: SparkContext, path: String): Unit = {
    require(bfs == null, "bfs should be null")
    try {
      bfs = sc.objectFile[(Int, BloomFilter[String])](path).persist(StorageLevel.MEMORY_AND_DISK)
    } catch {
      case e: Exception => logError(e.toString)
    }
  }
}

object inOps extends Serializable  with Logging{
  def cvtSaxToBf(iterData: Iterable[String]): BloomFilter[String] = {
    val data = iterData.toArray
    val bf = BloomFilter[String](data.length, 0.1)
    for (d <- data) {
      logError("-- add record !")
      bf.add(d)
    }
    bf
  }
}