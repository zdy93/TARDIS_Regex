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

import org.apache.spark.SparkContext
import org.apache.spark.edu.wpi.dsrg.tardis.configs.{IdxCfg, idxConfig}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
/**
  * Created by leon on 10/30/17.
  */
object tsEvaluate extends Logging {
  def apply(sc: SparkContext): Unit = {
    idxConfig.printCfg()
    val idxCfg = idxConfig.generateIdxCfg()

    val eq = new tsEvaluate(sc)
    eq.getTsInfo(idxCfg)
  }
}

class tsEvaluate (sc: SparkContext) extends Logging with Serializable{
  def getTsInfo(idxCfg: IdxCfg): Unit ={
    val tsRdd = sc.objectFile[(Long, Array[Float])](idxCfg.tsFileName)
    var newrdd = tsRdd.map { case (id, ts) =>Ops.cvtTsToIsaxHex(ts, idxCfg)}
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)

    writeLog(" sax_7 \t%d".format(newrdd.count()))

    val bitStep = 2

    for (i<- 0 to 4){
      newrdd = newrdd.map(sax => sax.dropRight(bitStep))//Ops.cvtToAncestor(sax,bitStep))
        .distinct().persist(StorageLevel.MEMORY_AND_DISK)
      val nbr = newrdd.count()
      writeLog(" sax_%d \t%d".format(6-i,nbr))
    }
  }

  private def writeLog(content: String): Unit = {
    Util.writeLog(content, true, idxConfig.logPath)
  }
}

