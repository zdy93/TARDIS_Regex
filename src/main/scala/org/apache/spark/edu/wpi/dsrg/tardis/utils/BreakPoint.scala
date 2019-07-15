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

import org.apache.spark.internal.Logging
/**
  * Created by leon on 9/6/17.
  */
object BreakPoint extends Logging{
  val case2:Array[Float] = Array(0.0F, Float.PositiveInfinity)
  val case3:Array[Float] = Array(-0.43073F, 0.43073F, Float.PositiveInfinity)
  val case4:Array[Float] = Array(-0.67449F, 0F, 0.67449F, Float.PositiveInfinity)
  val case5:Array[Float] = Array(-0.84F, -0.25F, 0.25F, 0.84F, Float.PositiveInfinity)
  val case6:Array[Float] = Array(-0.97F, -0.43F, 0.00F, 0.43F, 0.97F, Float.PositiveInfinity)
  val case7:Array[Float] = Array(-1.07F, -0.57F, -0.18F, 0.18F, 0.57F, 1.07F, Float.PositiveInfinity)
  val case8:Array[Float] = Array(-1.1503F, -0.67449F, -0.31864F, 0F, 0.31864F, 0.67449F, 1.1503F, Float.PositiveInfinity)
  val case9:Array[Float] = Array(-1.22F, -0.76F, -0.43F, -0.14F, 0.14F, 0.43F, 0.76F, 1.22F, Float.PositiveInfinity)
  val case10:Array[Float] = Array(-1.28F, -0.84F, -0.52F, -0.25F, 0.00F, 0.25F, 0.52F, 0.84F, 1.28F, Float.PositiveInfinity)
  val case11:Array[Float] = Array(-1.34F, -0.91F, -0.6F, -0.35F, -0.11F, 0.11F, 0.35F, 0.6F, 0.91F, 1.34F, Float.PositiveInfinity)
  val case12:Array[Float] = Array(-1.38F, -0.97F, -0.67F, -0.43F, -0.21F, 0.00F, 0.21F, 0.43F, 0.67F, 0.97F, 1.38F, Float.PositiveInfinity)
  val case13:Array[Float] = Array(-1.43F, -1.02F, -0.74F, -0.5F, -0.29F, -0.1F, 0.1F, 0.29F, 0.5F, 0.74F, 1.02F, 1.43F, Float.PositiveInfinity)
  val case14:Array[Float] = Array(-1.47F, -1.07F, -0.79F, -0.57F, -0.37F, -0.18F, 0.00F, 0.18F, 0.37F, 0.57F, 0.79F, 1.07F, 1.47F, Float.PositiveInfinity)
  val case15:Array[Float] = Array(-1.5F, -1.11F, -0.84F, -0.62F, -0.43F, -0.25F, -0.08F, 0.08F, 0.25F, 0.43F, 0.62F, 0.84F, 1.11F, 1.5F, Float.PositiveInfinity)
  val case16:Array[Float] = Array(-1.5341F, -1.1503F, -0.88715F, -0.67449F, -0.48878F, -0.31864F, -0.15731F, 0F, 0.15731F, 0.31864F, 0.48878F, 0.67449F, 0.88715F, 1.1503F, 1.5341F, Float.PositiveInfinity)
  val case17:Array[Float] = Array(-1.56F, -1.19F, -0.93F, -0.72F, -0.54F, -0.38F, -0.22F, -0.07F, 0.07F, 0.22F, 0.38F, 0.54F, 0.72F, 0.93F, 1.19F, 1.56F, Float.PositiveInfinity)
  val case18:Array[Float] = Array(-1.59F, -1.22F, -0.97F, -0.76F, -0.59F, -0.43F, -0.28F, -0.14F, 0.00F, 0.14F, 0.28F, 0.43F, 0.59F, 0.76F, 0.97F, 1.22F, 1.59F, Float.PositiveInfinity)
  val case19:Array[Float] = Array(-1.62F, -1.25F, -1F, -0.8F, -0.63F, -0.48F, -0.34F, -0.2F, -0.07F, 0.07F, 0.2F, 0.34F, 0.48F, 0.63F, 0.8F, 1.00F, 1.25F, 1.62F, Float.PositiveInfinity)
  val case20:Array[Float] = Array(-1.64F, -1.28F, -1.04F, -0.84F, -0.67F, -0.52F, -0.39F, -0.25F, -0.13F, 0.00F, 0.13F, 0.25F, 0.39F, 0.52F, 0.67F, 0.84F, 1.04F, 1.28F, 1.64F, Float.PositiveInfinity)
  val case32:Array[Float] = Array(-1.8627F, -1.5341F, -1.318F, -1.1503F, -1.01F, -0.88715F, -0.77642F, -0.67449F, -0.57913F, -0.48878F, -0.40225F, -0.31864F, -0.2372F, -0.15731F, -0.078412F, 0F, 0.078412F, 0.15731F, 0.2372F, 0.31864F, 0.40225F, 0.48878F, 0.57913F, 0.67449F, 0.77642F, 0.88715F, 1.01F, 1.1503F, 1.318F, 1.5341F, 1.8627F, Float.PositiveInfinity)
  val case64:Array[Float] = Array(-2.1539F, -1.8627F, -1.6759F, -1.5341F, -1.4178F, -1.318F, -1.2299F, -1.1503F, -1.0775F, -1.01F, -0.94678F, -0.88715F, -0.83051F, -0.77642F, -0.72451F, -0.67449F, -0.6261F, -0.57913F, -0.53341F, -0.48878F, -0.4451F, -0.40225F, -0.36013F, -0.31864F, -0.27769F, -0.2372F, -0.1971F, -0.15731F, -0.11777F, -0.078412F, -0.039176F, 0F, 0.039176F, 0.078412F, 0.11777F, 0.15731F, 0.1971F, 0.2372F, 0.27769F, 0.31864F, 0.36013F, 0.40225F, 0.4451F, 0.48878F, 0.53341F, 0.57913F, 0.6261F, 0.67449F, 0.72451F, 0.77642F, 0.83051F, 0.88715F, 0.94678F, 1.01F, 1.0775F, 1.1503F, 1.2299F, 1.318F, 1.4178F, 1.5341F, 1.6759F, 1.8627F, 2.1539F, Float.PositiveInfinity)

  val case128:Array[Float] = Array(-2.4176F, -2.1539F, -1.9874F, -1.8627F, -1.7617F, -1.6759F, -1.601F, -1.5341F, -1.4735F, -1.4178F, -1.3662F, -1.318F, -1.2727F, -1.2299F, -1.1892F, -1.1503F, -1.1132F, -1.0775F, -1.0432F, -1.01F, -0.9779F, -0.94678F, -0.91656F, -0.88715F, -0.85848F, -0.83051F, -0.80317F, -0.77642F, -0.75022F, -0.72451F, -0.69928F, -0.67449F, -0.6501F, -0.6261F, -0.60245F, -0.57913F, -0.55613F, -0.53341F, -0.51097F, -0.48878F, -0.46683F, -0.4451F, -0.42358F, -0.40225F, -0.38111F, -0.36013F, -0.33931F, -0.31864F, -0.2981F, -0.27769F, -0.25739F, -0.2372F, -0.21711F, -0.1971F, -0.17717F, -0.15731F, -0.13751F, -0.11777F, -0.098072F, -0.078412F, -0.058783F, -0.039176F, -0.019584F, 0F, 0.019584F, 0.039176F, 0.058783F, 0.078412F, 0.098072F, 0.11777F, 0.13751F, 0.15731F, 0.17717F, 0.1971F, 0.21711F, 0.2372F, 0.25739F, 0.27769F, 0.2981F, 0.31864F, 0.33931F, 0.36013F, 0.38111F, 0.40225F, 0.42358F, 0.4451F, 0.46683F, 0.48878F, 0.51097F, 0.53341F, 0.55613F, 0.57913F, 0.60245F, 0.6261F, 0.6501F, 0.67449F, 0.69928F, 0.72451F, 0.75022F, 0.77642F, 0.80317F, 0.83051F, 0.85848F, 0.88715F, 0.91656F, 0.94678F, 0.9779F, 1.01F, 1.0432F, 1.0775F, 1.1132F, 1.1503F, 1.1892F, 1.2299F, 1.2727F, 1.318F, 1.3662F, 1.4178F, 1.4735F, 1.5341F, 1.601F, 1.6759F, 1.7617F, 1.8627F, 1.9874F, 2.1539F, 2.4176F, Float.PositiveInfinity)

  //if support 256 cardinalityF, the unit of sax need be short rather than byte[-127~127]
  val case256:Array[Float] = Array(-2.6601F, -2.4176F, -2.2662F, -2.1539F, -2.0635F, -1.9874F, -1.9214F, -1.8627F, -1.8099F, -1.7617F, -1.7172F, -1.6759F, -1.6373F, -1.601F, -1.5667F, -1.5341F, -1.5031F, -1.4735F, -1.4451F, -1.4178F, -1.3915F, -1.3662F, -1.3417F, -1.318F, -1.295F, -1.2727F, -1.251F, -1.2299F, -1.2093F, -1.1892F, -1.1695F, -1.1503F, -1.1316F, -1.1132F, -1.0952F, -1.0775F, -1.0602F, -1.0432F, -1.0264F, -1.01F, -0.99382F, -0.9779F, -0.96222F, -0.94678F, -0.93156F, -0.91656F, -0.90175F, -0.88715F, -0.87273F, -0.85848F, -0.84442F, -0.83051F, -0.81677F, -0.80317F, -0.78973F, -0.77642F, -0.76325F, -0.75022F, -0.7373F, -0.72451F, -0.71184F, -0.69928F, -0.68683F, -0.67449F, -0.66225F, -0.6501F, -0.63806F, -0.6261F, -0.61423F, -0.60245F, -0.59075F, -0.57913F, -0.56759F, -0.55613F, -0.54473F, -0.53341F, -0.52215F, -0.51097F, -0.49984F, -0.48878F, -0.47777F, -0.46683F, -0.45593F, -0.4451F, -0.43431F, -0.42358F, -0.41289F, -0.40225F, -0.39166F, -0.38111F, -0.3706F, -0.36013F, -0.3497F, -0.33931F, -0.32896F, -0.31864F, -0.30835F, -0.2981F, -0.28788F, -0.27769F, -0.26753F, -0.25739F, -0.24729F, -0.2372F, -0.22714F, -0.21711F, -0.20709F, -0.1971F, -0.18713F, -0.17717F, -0.16723F, -0.15731F, -0.1474F, -0.13751F, -0.12764F, -0.11777F, -0.10792F, -0.098072F, -0.088238F, -0.078412F, -0.068594F, -0.058783F, -0.048977F, -0.039176F, -0.029379F, -0.019584F, -0.0097917F, 0F, 0.0097917F, 0.019584F, 0.029379F, 0.039176F, 0.048977F, 0.058783F, 0.068594F, 0.078412F, 0.088238F, 0.098072F, 0.10792F, 0.11777F, 0.12764F, 0.13751F, 0.1474F, 0.15731F, 0.16723F, 0.17717F, 0.18713F, 0.1971F, 0.20709F, 0.21711F, 0.22714F, 0.2372F, 0.24729F, 0.25739F, 0.26753F, 0.27769F, 0.28788F, 0.2981F, 0.30835F, 0.31864F, 0.32896F, 0.33931F, 0.3497F, 0.36013F, 0.3706F, 0.38111F, 0.39166F, 0.40225F, 0.41289F, 0.42358F, 0.43431F, 0.4451F, 0.45593F, 0.46683F, 0.47777F, 0.48878F, 0.49984F, 0.51097F, 0.52215F, 0.53341F, 0.54473F, 0.55613F, 0.56759F, 0.57913F, 0.59075F, 0.60245F, 0.61423F, 0.6261F, 0.63806F, 0.6501F, 0.66225F, 0.67449F, 0.68683F, 0.69928F, 0.71184F, 0.72451F, 0.7373F, 0.75022F, 0.76325F, 0.77642F, 0.78973F, 0.80317F, 0.81677F, 0.83051F, 0.84442F, 0.85848F, 0.87273F, 0.88715F, 0.90175F, 0.91656F, 0.93156F, 0.94678F, 0.96222F, 0.9779F, 0.99382F, 1.01F, 1.0264F, 1.0432F, 1.0602F, 1.0775F, 1.0952F, 1.1132F, 1.1316F, 1.1503F, 1.1695F, 1.1892F, 1.2093F, 1.2299F, 1.251F, 1.2727F, 1.295F, 1.318F, 1.3417F, 1.3662F, 1.3915F, 1.4178F, 1.4451F, 1.4735F, 1.5031F, 1.5341F, 1.5667F, 1.601F, 1.6373F, 1.6759F, 1.7172F, 1.7617F, 1.8099F, 1.8627F, 1.9214F, 1.9874F, 2.0635F, 2.1539F, 2.2662F, 2.4176F, 2.6601F, Float.PositiveInfinity)

  def apply(cardinality:Int): Array[Float] = cardinality match{
    case 2 => case2
    case 3 => case3
    case 4 => case4
    case 5 => case5
    case 6 => case6
    case 7 => case7
    case 8 => case8
    case 9 => case9
    case 10 => case10
    case 11 => case11
    case 12 => case12
    case 13 => case13
    case 14 => case14
    case 15 => case15
    case 16 => case16
    case 17 => case17
    case 18 => case18
    case 19 => case19
    case 20 => case20
    case 32 => case32
    case 64 => case64
    case 128 => case128
    case 256 => case256
  }
}
