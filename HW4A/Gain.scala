

object Gain {
  val lnOf2 = scala.math.log(2) // natural log of 2
  def log2(x: Double): Double = {
    if(x == 0) 0
    else scala.math.log(x) / lnOf2
  }

  def main(args: Array[String]): Unit = {
    
    
    var e = 0.8631 //

    var c = 7.0 //
    var cA = 3 //
    var cB = c - cA

    var pA = 1.0
    var nA = cA - pA

    var pB = 1.0
    var nB = cB - pB


    var gain = e  - ((cA/c) * (-1 * ((pA/cA)*log2(pA/cA) + (nA/cA)*log2(nA/cA)))) - ((cB/c) * (-1 * ((pB/cB)*log2(pB/cB) + (nB/cB)*log2(nB/cB))))  

    println(gain)
    
    
//    println((-2.0/7) * log2(2.0/7) + (-5.0/7) * log2(5.0/7))
    
  }

}